/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "swift/broker/storage_dfs/FileCache.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <functional>
#include <limits>
#include <memory>
#include <pthread.h>
#include <stdint.h>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "swift/monitor/BrokerMetricsReporter.h"
#include "swift/monitor/MetricsCommon.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/BlockPool.h"
#include "swift/util/FileManagerUtil.h"
#include "swift/util/Permission.h"
#include "swift/util/PermissionCenter.h"

namespace swift {
namespace util {
class TimeoutChecker;
} // namespace util
} // namespace swift

namespace swift {
namespace storage {
AUTIL_LOG_SETUP(swift, FileCache);
using namespace swift::util;
using namespace swift::protocol;
using namespace swift::monitor;
using namespace fslib;
using namespace autil;
using namespace std;

const int64_t INT64_MAX_VALUE = numeric_limits<int64_t>::max();

FileCache::FileCache(BlockPool *cachePool,
                     int32_t oneFileFdNum,
                     int64_t fileReserveTimeSec,
                     PermissionCenter *permissionCenter,
                     BrokerMetricsReporter *metricsReporter,
                     const PartitionId &partitionId) {
    _blockPool = cachePool;
    _permissionCenter = permissionCenter;
    _metricsReporter = metricsReporter;
    _oneFileFdNum = oneFileFdNum > 0 ? oneFileFdNum : 1;
    _fileReserveThreshold = fileReserveTimeSec * 1000 * 1000;
    _partitionId = partitionId;
}

FileCache::~FileCache() {
    {
        ScopedReadWriteLock lock(_fileRW, 'w');
        _fileCache.clear();
    }
    {
        ScopedReadWriteLock lock(_metaBlockRW, 'w');
        _metaBlockCache.clear();
    }
    {
        ScopedReadWriteLock lock(_dataBlockRW, 'w');
        _dataBlockCache.clear();
    }
}

ErrorCode FileCache::readBlock(const string &fileName,
                               int64_t offset,
                               bool isAppending,
                               TimeoutChecker *timeoutChecker,
                               BlockPtr &block,
                               bool &isFromCache,
                               ReaderInfo *readerInfo,
                               const ReaderInfoMap *readerInfoMap,
                               ReadMetricsCollector *collector) {
    ErrorCode ec =
        innerReadBlock(fileName, offset, isAppending, timeoutChecker, block, isFromCache, readerInfo, readerInfoMap);
    if (collector) {
        collectBlockMetrics(ec, fileName, isFromCache, block, collector);
    }
    return ec;
}

void FileCache::collectBlockMetrics(
    ErrorCode ec, const string &fileName, bool isFromCache, BlockPtr block, ReadMetricsCollector *collector) {
    ++collector->partitionReadBlockQps;
    if (ec != ERROR_NONE) {
        ++collector->partitionReadBlockErrorQps;
    } else {
        if (isFromCache) {
            ++collector->partitionBlockCacheHitTimes;
        } else {
            ++collector->partitionBlockCacheNotHitTimes;
            collector->partitionDfsActualReadRate += block->getActualSize();
            ++collector->actualReadBlockCount;
        }
    }
    if (fileName.rfind(FileManagerUtil::DATA_SUFFIX) != string::npos) {
        ++collector->partitionReadDataBlockQps;
        if (ec != ERROR_NONE) {
            ++collector->partitionReadDataBlockErrorQps;
        } else {
            if (isFromCache) {
                ++collector->partitionDataBlockCacheHitTimes;
            } else {
                ++collector->partitionDataBlockCacheNotHitTimes;
                collector->partitionDfsActualReadDataRate += block->getActualSize();
            }
        }
    } else {
        ++collector->partitionReadMetaBlockQps;
        if (ec != ERROR_NONE) {
            ++collector->partitionReadMetaBlockErrorQps;
        } else {
            if (isFromCache) {
                ++collector->partitionMetaBlockCacheHitTimes;
            } else {
                ++collector->partitionMetaBlockCacheNotHitTimes;
                collector->partitionDfsActualReadMetaRate += block->getActualSize();
            }
        }
    }
}

ErrorCode FileCache::innerReadBlock(const string &fileName,
                                    int64_t offset,
                                    bool isAppending,
                                    TimeoutChecker *timeoutChecker,
                                    BlockPtr &block,
                                    bool &isFromCache,
                                    ReaderInfo *readerInfo,
                                    const ReaderInfoMap *readerInfoMap) {
    block.reset();
    block = getBlockFromCache(fileName, offset, readerInfo);
    if (block) {
        isFromCache = true;
        return ERROR_NONE;
    }
    int64_t blockId = calBlockId(offset);
    PermissionPtr permission;
    if (_permissionCenter) {
        PermissionKey key(fileName, blockId);
        permission = _permissionCenter->apply(key, timeoutChecker);
        if (!permission) {
            AUTIL_LOG(WARN, "apply from file [%s] offset [%ld] failed.", fileName.c_str(), offset);
            return ERROR_BROKER_BUSY;
        }
    }
    ScopedPermission scopedPermission(_permissionCenter, permission);
    block = getBlockFromCache(fileName, offset, readerInfo);
    if (block) {
        isFromCache = true;
        return ERROR_NONE;
    }
    if (_permissionCenter && !_permissionCenter->incReadFileCount(fileName)) {
        AUTIL_LOG(WARN,
                  "read message from file [%s] failed, exceed read file limit [%d],",
                  fileName.c_str(),
                  _permissionCenter->getReadFileLimit());
        return ERROR_BROKER_BUSY;
    }
    ScopedReadFilePermission readFilePermission(_permissionCenter, fileName);

    isFromCache = false;
    return getBlockFromFile(fileName, offset, isAppending, block, readerInfo, readerInfoMap);
}

int64_t FileCache::getCacheBlockSize() { return _blockPool->getBlockSize(); }

void FileCache::recycle(const ReaderInfoMap *readerInfoMap, int64_t metaThreshold, int64_t dataThreshold) {
    recycleBlock(0, readerInfoMap, metaThreshold, dataThreshold);
    recycleFile();
}

void FileCache::removeBlockItem(const std::string &fileName, int64_t offset) {
    int64_t blockId = calBlockId(offset);
    FileBlockKey key = make_pair(fileName, blockId);
    if (StringUtil::endsWith(fileName, FileManagerUtil::DATA_SUFFIX)) {
        ScopedReadWriteLock lock(_dataBlockRW, 'w');
        _dataBlockCache.erase(key);
    } else {
        ScopedReadWriteLock lock(_metaBlockRW, 'w');
        _metaBlockCache.erase(key);
    }
}

void FileCache::setBadFile(const std::string &fileName) {
    bool isBad = false;
    {
        ScopedReadWriteLock lock(_fileRW, 'w');
        FileWrapperMap::iterator iter = _fileCache.find(fileName);
        if (iter != _fileCache.end()) {
            for (auto &fileWrapper : iter->second) {
                fileWrapper->setBad(true);
                isBad = true;
            }
        }
    }
    if (isBad) {
        AUTIL_LOG(INFO, "set file [%s] bad, recycle it.", fileName.c_str());
    }
}

int64_t FileCache::calBlockId(int64_t offset) {
    int64_t blockSize = _blockPool->getBlockSize();
    return offset / blockSize;
}

BlockPtr FileCache::getBlockFromCache(const string &fileName, int64_t offset, ReaderInfo *readerInfo) {
    int64_t blockId = calBlockId(offset);
    FileBlockKey key = make_pair(fileName, blockId);
    BlockPtr fileBlock;
    if (StringUtil::endsWith(fileName, FileManagerUtil::DATA_SUFFIX)) {
        if (readerInfo) {
            readerInfo->dataInfo->fileName = fileName;
            readerInfo->dataInfo->blockId = blockId;
        }
        ScopedReadWriteLock lock(_dataBlockRW, 'r');
        FileBlockMap::iterator iter = _dataBlockCache.find(key);
        if (iter != _dataBlockCache.end()) {
            fileBlock = iter->second.first;
        }
    } else {
        if (readerInfo) {
            readerInfo->metaInfo->fileName = fileName;
            readerInfo->metaInfo->blockId = blockId;
        }
        ScopedReadWriteLock lock(_metaBlockRW, 'r');
        FileBlockMap::iterator iter = _metaBlockCache.find(key);
        if (iter != _metaBlockCache.end()) {
            fileBlock = iter->second.first;
        }
    }
    return fileBlock;
}

void FileCache::putBlockIntoCache(const string &fileName, int64_t offset, const BlockPtr &block) {
    int64_t blockId = calBlockId(offset);
    FileBlockKey key = make_pair(fileName, blockId);
    FileBlockItem item(block, 0); //刚放进去距离为0
    if (StringUtil::endsWith(fileName, FileManagerUtil::DATA_SUFFIX)) {
        ScopedReadWriteLock lock(_dataBlockRW, 'w');
        _dataBlockCache[key] = item;
    } else {
        ScopedReadWriteLock lock(_metaBlockRW, 'w');
        _metaBlockCache[key] = item;
    }
}

ErrorCode FileCache::getBlockFromFile(const string &fileName,
                                      int64_t offset,
                                      bool isAppending,
                                      BlockPtr &block,
                                      ReaderInfo *readerInfo,
                                      const ReaderInfoMap *readerInfoMap) {
    recycleBlock(1, readerInfoMap);
    BlockPtr applyBlock = _blockPool->allocate();
    if (!applyBlock) {
        AUTIL_LOG(WARN,
                  "apply file block error, max block [%ld], used block [%ld]",
                  _blockPool->getMaxBlockCount(),
                  _blockPool->getUsedBlockCount());
        return ERROR_BROKER_BUSY;
    }
    int64_t blockSize = _blockPool->getBlockSize();
    int64_t alignOffset = calBlockId(offset) * blockSize;
    int64_t maxOffset = alignOffset + blockSize;
    FileWrapperPtr fileWrapper = getFileWrapper(fileName, isAppending, maxOffset);
    if (!fileWrapper) {
        return ERROR_BROKER_SOME_MESSAGE_LOST;
    }
    int64_t readLen = -1;
    bool succ = false;
    kmonitor::MetricsTags tags;
    tags.AddTag("topic", _partitionId.topicname());
    tags.AddTag("partition", intToString(_partitionId.id()));
    FUNC_REPORT_LATENCY(readLen,
                        fileWrapper->pread(applyBlock->getBuffer(), blockSize, alignOffset, succ),
                        reportDfsReadOneBlockLatency,
                        tags);
    if (!succ) {
        AUTIL_LOG(WARN, "open file [%s] failed", fileName.c_str());
        return ERROR_BROKER_SOME_MESSAGE_LOST;
    }
    if (readLen < 0) {
        AUTIL_LOG(WARN,
                  "read file [%s] error, offset [%ld], align offset [%ld],"
                  " block size [%ld]",
                  fileName.c_str(),
                  offset,
                  alignOffset,
                  blockSize);
        return ERROR_BROKER_SOME_MESSAGE_LOST;
    }
    applyBlock->setActualSize(readLen);
    if (readLen > 0) {
        if (readLen == blockSize || !isAppending) {
            putBlockIntoCache(fileName, offset, applyBlock);
        }
    }
    block = applyBlock;
    return ERROR_NONE;
}

FileWrapperPtr FileCache::getFileWrapper(const string &fileName, bool isAppending, int64_t maxReadFileOffset) {
    FileWrapperMap::iterator iter;
    pthread_t tid = pthread_self();
    size_t offset = tid % _oneFileFdNum;
    FileWrapperPtr fileWrapper;
    {
        ScopedReadWriteLock lock(_fileRW, 'r');
        iter = _fileCache.find(fileName);
        if (iter != _fileCache.end()) {
            if (offset < iter->second.size()) {
                fileWrapper = iter->second[offset];
                if (!fileWrapper->isAppending()) {
                    return fileWrapper;
                } else {
                    if (fileWrapper->getFileLength() >= maxReadFileOffset) {
                        if (!isAppending) {
                            fileWrapper->setBad(true);
                        }
                        return fileWrapper;
                    } else {
                        fileWrapper->setBad(true);
                    }
                }
            }
        }
    }
    {
        ScopedReadWriteLock lock(_fileRW, 'w');
        iter = _fileCache.find(fileName);
        if (iter != _fileCache.end()) {
            if (offset < iter->second.size()) {
                fileWrapper = iter->second[offset];
                if (!fileWrapper->isAppending()) {
                    return fileWrapper;
                } else {
                    if (fileWrapper->getFileLength() >= maxReadFileOffset) {
                        if (!isAppending) {
                            fileWrapper->setBad(true);
                        }
                        return fileWrapper;
                    } else {
                        fileWrapper->setBad(true);
                    }
                }
            }
        }
        vector<FileWrapperPtr> fileWrappers;
        for (uint32_t i = 0; i < _oneFileFdNum; i++) {
            fileWrapper.reset(new FileWrapper(fileName, isAppending));
            fileWrappers.push_back(fileWrapper);
        }
        _fileCache[fileName] = fileWrappers;
        fileWrapper = fileWrappers[offset];
    }
    return fileWrapper;
}

void FileCache::recycleBlock(int64_t blockCount,
                             const ReaderInfoMap *readerInfoMap,
                             int64_t metaThreshold,
                             int64_t dataThreshold) {
    if (NULL == readerInfoMap) {
        return;
    }
    if (blockCount != 0) {
        int64_t maxUnusedBlock = _blockPool->getMaxUnusedBlockCount();
        if (maxUnusedBlock >= blockCount) {
            return;
        }
    }

    RecycleBlockMetricsCollector collector(_partitionId.topicname(), _partitionId.id());
    int64_t begTime = TimeUtility::currentTime();
    int64_t usedBlockCount = _metaBlockCache.size() + _dataBlockCache.size();
    int64_t recycleBlockCount = recycleBlockObsolete(readerInfoMap, collector);
    int64_t needBlockCount = blockCount > 0 ? blockCount : usedBlockCount * 0.1;
    needBlockCount = max(int64_t(1), needBlockCount);
    if (recycleBlockCount < needBlockCount) {
        if (-2 == metaThreshold && -2 == dataThreshold) {
            vector<int64_t> metaDisVec, dataDisVec;
            getBlockDis(readerInfoMap, metaDisVec, dataDisVec);
            sort(metaDisVec.begin(), metaDisVec.end(), std::greater<int64_t>());
            sort(dataDisVec.begin(), dataDisVec.end(), std::greater<int64_t>());
            metaThreshold = metaDisVec.size() > 0 ? metaDisVec[metaDisVec.size() / 10] : 0;
            dataThreshold = dataDisVec.size() > 0 ? dataDisVec[dataDisVec.size() / 10] : 0;
            collector.recycleByDisMetaThreshold = metaThreshold;
            collector.recycleByDisDataThreshold = dataThreshold;
        }
        recycleBlockCount += recycleBlockByDis(readerInfoMap, metaThreshold, dataThreshold, collector);
    }
    if (recycleBlockCount > 0) {
        _blockPool->freeUnusedBlock();
    }

    int64_t endTime = TimeUtility::currentTime();
    collector.recycleBlockCount = recycleBlockCount;
    ++collector.recycleBlockCacheQps;
    collector.recycleBlockCacheLatency = (endTime - begTime) / 1000;
    if (_metricsReporter) {
        _metricsReporter->reportRecycleBlockMetrics(collector);
    }
}

//回收reader读过的block
int64_t FileCache::recycleBlockObsolete(const ReaderInfoMap *readerInfoMap, RecycleBlockMetricsCollector &collector) {
    int64_t begTime = TimeUtility::currentTime();
    int64_t metaCount = 0, dataCount = 0;
    int64_t metaBlockId = 0, dataBlockId = 0;
    string metaFileName, dataFileName;
    getSlowestFileReaderInfo(readerInfoMap, metaFileName, metaBlockId, dataFileName, dataBlockId);
    FileBlockKey metaRemoveKey = make_pair(metaFileName, metaBlockId);
    FileBlockKey dataRemoveKey = make_pair(dataFileName, dataBlockId);
    {
        ScopedReadWriteLock lock(_metaBlockRW, 'w');
        FileBlockMap::iterator iter = _metaBlockCache.begin();
        for (; iter != _metaBlockCache.end();) {
            if (iter->first < metaRemoveKey) {
                _metaBlockCache.erase(iter++);
                ++metaCount;
            } else {
                iter++;
            }
        }
    }
    {
        ScopedReadWriteLock lock(_dataBlockRW, 'w');
        FileBlockMap::iterator iter = _dataBlockCache.begin();
        for (; iter != _dataBlockCache.end();) {
            if (iter->first < dataRemoveKey) {
                _dataBlockCache.erase(iter++);
                ++dataCount;
            } else {
                iter++;
            }
        }
    }

    int64_t endTime = TimeUtility::currentTime();
    collector.recycleObsoleteMetaBlockCount = metaCount;
    collector.recycleObsoleteDataBlockCount = dataCount;
    collector.recycleObsoleteBlockLatency = (endTime - begTime) / 1000;
    collector.recycleObsoleteBlockCount = metaCount + dataCount;

    return metaCount + dataCount;
}

int64_t FileCache::recycleBlockByDis(const ReaderInfoMap *readerInfoMap,
                                     int64_t metaThreshold,
                                     int64_t dataThreshold,
                                     RecycleBlockMetricsCollector &collector) {
    /*
      距离计算逻辑：找当前block前面的reader，reader需要在block同一个文件上，reader与block的时间距离公式：
      时间距离 = 相差的block数 * block大小 / reader每秒速度
      距离小于阈值的的保留，或者说距离越大越需要淘汰
    */
    int64_t metaCount = 0, dataCount = 0;
    int64_t begTime = TimeUtility::currentTime();
    if (metaThreshold >= 0) {
        ScopedReadWriteLock lock(_metaBlockRW, 'w');
        FileBlockMap::iterator iter = _metaBlockCache.begin();
        while (iter != _metaBlockCache.end()) {
            if (iter->second.second >= metaThreshold) {
                _metaBlockCache.erase(iter++);
                ++metaCount;
            } else {
                iter++;
            }
        }
    }
    if (dataThreshold >= 0) {
        ScopedReadWriteLock lock(_dataBlockRW, 'w');
        FileBlockMap::iterator iter = _dataBlockCache.begin();
        while (iter != _dataBlockCache.end()) {
            if (iter->second.second >= dataThreshold) {
                _dataBlockCache.erase(iter++);
                ++dataCount;
            } else {
                iter++;
            }
        }
    }
    if (_metricsReporter) {
        int64_t endTime = TimeUtility::currentTime();
        if (metaThreshold > 0) {
            collector.recycleByDisMetaBlockCount = metaCount;
        }
        if (dataThreshold > 0) {
            collector.recycleByDisDataBlockCount = dataCount;
        }
        collector.recycleByDisLatency = (endTime - begTime) / 1000;
        collector.recycleByDisBlockCount = metaCount + dataCount;
        AUTIL_LOG(DEBUG,
                  "recycleByDisBlockCount %ld, thresold: meta[%ld] data[%ld]",
                  metaCount + dataCount,
                  metaThreshold,
                  dataThreshold);
    }
    return metaCount + dataCount;
}

bool FileCache::getBlockDis(const ReaderInfoMap *readerInfoMap,
                            vector<int64_t> &metaDisVec,
                            vector<int64_t> &dataDisVec) {
    int64_t metaCount = 0, dataCount = 0;
    {
        ScopedReadWriteLock lock(_metaBlockRW, 'r');
        FileBlockMap::iterator iter = _metaBlockCache.begin();
        for (; iter != _metaBlockCache.end(); ++iter) {
            int64_t dis = getNearestReaderDis(iter->first, readerInfoMap, false);
            iter->second.second = dis;
            metaDisVec.push_back(dis);
            ++metaCount;
        }
    }
    {
        ScopedReadWriteLock lock(_dataBlockRW, 'r');
        FileBlockMap::iterator iter = _dataBlockCache.begin();
        for (; iter != _dataBlockCache.end(); ++iter) {
            int64_t dis = getNearestReaderDis(iter->first, readerInfoMap, true);
            iter->second.second = dis;
            dataDisVec.push_back(dis);
            ++dataCount;
        }
    }
    if (0 == metaCount + dataCount) {
        return false;
    }
    return true;
}

bool FileCache::getFileMsgId(const string &fileName, int64_t &msgId) {
    string::size_type pos = fileName.rfind('/');
    if (string::npos != pos) {
        const string &realName = fileName.substr(pos);
        const vector<string> &fileVec = StringUtil::split(realName, ".");
        assert(fileVec.size() >= 4);
        return StringUtil::strToInt64(fileVec[2].c_str(), msgId);
    } else {
        const vector<string> &fileVec = StringUtil::split(fileName, ".");
        assert(fileVec.size() >= 4);
        return StringUtil::strToInt64(fileVec[2].c_str(), msgId);
    }
}

/*
cache block跟reader的距离共2种情况:
一、同一个文件中
  1. ---block-----reader---
     block在reader之前，距离为max
  2. ---reader----block----
     block在reader之后，距离可计算，reader马上就要读它
  3. reader跟block重合，这block可以留着
一、不同文件中
  1. ---block----
     ---reader---
    block在reader的上一个文件，距离max
  2. ---reader---
     ---block----
    block在reader的后一个文件，计算文件间距离
*/
int64_t
FileCache::getNearestReaderDis(const FileBlockKey &blockKey, const ReaderInfoMap *readerInfoMap, bool isDataBlock) {
    int64_t nearestDis = INT64_MAX_VALUE;
    int64_t blockBeginMsgId = -1;
    if (!getFileMsgId(blockKey.first, blockBeginMsgId)) {
        AUTIL_LOG(WARN, "get %s msgId fail", blockKey.first.c_str());
        return nearestDis;
    }
    for (ReaderInfoMap::const_iterator iter = readerInfoMap->begin(); iter != readerInfoMap->end(); ++iter) {
        if (!iter->second->getReadFile()) {
            continue;
        }
        ReadFileInfoPtr &fileInfo = isDataBlock ? iter->second->dataInfo : iter->second->metaInfo;
        int64_t rate = fileInfo->getReadRate();
        if (rate <= 0) {
            continue;
        }
        if (blockKey.first == fileInfo->fileName) {
            if (fileInfo->blockId == blockKey.second) {
                return 0;
            }
            if (fileInfo->blockId < blockKey.second) {
                int64_t dis = (blockKey.second - fileInfo->blockId) * getCacheBlockSize() / rate;
                if (dis < nearestDis) {
                    nearestDis = dis;
                }
            }
        } else if (fileInfo->fileName < blockKey.first) {
            if (0 == fileInfo->msgSize) {
                continue;
            }
            int64_t fileMsgId = -1;
            if (!getFileMsgId(fileInfo->fileName, fileMsgId)) {
                AUTIL_LOG(DEBUG, "get %s msgId fail", fileInfo->fileName.c_str());
                continue;
            }
            int64_t disMsgCount = blockBeginMsgId - fileMsgId;
            int64_t dis =
                (disMsgCount * fileInfo->msgSize) + (blockKey.second - fileInfo->blockId) * getCacheBlockSize();
            dis /= rate;
            AUTIL_LOG(DEBUG,
                      "%ld %ld %ld %ld %ld %ld",
                      disMsgCount,
                      fileInfo->msgSize,
                      getCacheBlockSize(),
                      fileInfo->blockId,
                      blockKey.second,
                      dis);
            if (dis <= 0) {
                continue;
            }
            if (dis < nearestDis) {
                nearestDis = dis;
            }
        } else {
        }
    }
    return nearestDis;
}

void FileCache::recycleFile() {
    int64_t threshold = TimeUtility::currentTime() - _fileReserveThreshold;
    vector<FileWrapperPtr> fileVec;
    {
        ScopedReadWriteLock lock(_fileRW, 'w');
        FileWrapperMap::iterator iter = _fileCache.begin();
        for (; iter != _fileCache.end();) {
            if (hasBadFile(iter->second) || getLastAccessTime(iter->second) < threshold) {
                fileVec.insert(fileVec.end(), iter->second.begin(), iter->second.end());
                _fileCache.erase(iter++);
            } else {
                iter++;
            }
        }
    }
    fileVec.clear();
    if (_metricsReporter) {
        kmonitor::MetricsTags tags;
        tags.AddTag("topic", _partitionId.topicname());
        tags.AddTag("partition", intToString(_partitionId.id()));
        _metricsReporter->incRecycleFileCacheQps(&tags);
    }
}

bool FileCache::hasBadFile(const vector<FileWrapperPtr> &fileWrappers) {
    bool hasBad = false;
    for (const auto &fileWrapper : fileWrappers) {
        if (fileWrapper->isBad()) {
            hasBad = true;
            break;
        }
    }
    return hasBad;
}

int64_t FileCache::getLastAccessTime(const vector<FileWrapperPtr> &fileWrappers) {
    int64_t lastAccessTime = 0;
    for (const auto &fileWrapper : fileWrappers) {
        if (lastAccessTime < fileWrapper->getLastAccessTime()) {
            lastAccessTime = fileWrapper->getLastAccessTime();
        }
    }
    return lastAccessTime;
}

int64_t FileCache::getBlockCount() { return _metaBlockCache.size() + _dataBlockCache.size(); }

void FileCache::setFileReserveTime(int64_t timeSecond) { _fileReserveThreshold = timeSecond * 1000 * 1000; }

} // namespace storage
} // namespace swift
