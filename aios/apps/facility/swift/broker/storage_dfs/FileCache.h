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
#pragma once

#include <map>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/broker/storage_dfs/FileWrapper.h"
#include "swift/common/Common.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/Block.h"
#include "swift/util/ReaderRec.h"

namespace swift {
namespace monitor {
class BrokerMetricsReporter;
struct ReadMetricsCollector;
struct RecycleBlockMetricsCollector;
} // namespace monitor
namespace util {
class BlockPool;
class PermissionCenter;
class TimeoutChecker;
} // namespace util
} // namespace swift

namespace swift {
namespace storage {

class FileCache {
public:
    typedef std::pair<std::string, int64_t> FileBlockKey;                      // file name, block id
    typedef std::pair<util::BlockPtr, int64_t> FileBlockItem;                  // blockPtr, dis
    typedef std::map<std::string, std::vector<FileWrapperPtr>> FileWrapperMap; // file name, file wrapper vec
    typedef std::map<FileBlockKey, FileBlockItem> FileBlockMap;

public:
    FileCache(util::BlockPool *centerPool,
              int32_t oneFileFdNum = 1,
              int64_t fileReserveTimeSec = 600,
              util::PermissionCenter *permissionCenter = NULL,
              monitor::BrokerMetricsReporter *patitionMetricsReporter = NULL,
              const protocol::PartitionId &partitionId = protocol::PartitionId());
    ~FileCache();

private:
    FileCache(const FileCache &);
    FileCache &operator=(const FileCache &);

public:
    protocol::ErrorCode readBlock(const std::string &fileName,
                                  int64_t offset,
                                  bool isAppending,
                                  util::TimeoutChecker *timeoutChecker,
                                  util::BlockPtr &block,
                                  bool &isFromCache,
                                  util::ReaderInfo *readerInfo = NULL,
                                  const util::ReaderInfoMap *readerInfoMap = NULL,
                                  monitor::ReadMetricsCollector *collector = NULL);
    void recycle(const util::ReaderInfoMap *readerInfoMap, int64_t metaThreshold, int64_t dataThreshold);
    void recycleFile();
    void removeBlockItem(const std::string &fileName, int64_t offset);
    void setBadFile(const std::string &fileName);
    int64_t getBlockCount();
    bool getBlockDis(const util::ReaderInfoMap *readerInfoMap,
                     std::vector<int64_t> &metaDisVec,
                     std::vector<int64_t> &dataDisVec);
    int64_t getCacheBlockSize();
    int64_t getCachFileCount() {
        autil::ScopedReadWriteLock lock(_fileRW, 'r');
        return _fileCache.size();
    }
    int64_t getCacheMetaBlockCount() {
        autil::ScopedReadWriteLock lock(_metaBlockRW, 'r');
        return _metaBlockCache.size();
    }
    int64_t getCacheDataBlockCount() {
        autil::ScopedReadWriteLock lock(_dataBlockRW, 'r');
        return _dataBlockCache.size();
    }
    void setFileReserveTime(int64_t timeSecond); // for test

private:
    void recycleBlock(int64_t blockCount,
                      const util::ReaderInfoMap *readerInfoMap = NULL,
                      int64_t metaThreshold = -2,
                      int64_t dataThreshold = -2);
    util::BlockPtr getBlockFromCache(const std::string &fileName, int64_t offset, util::ReaderInfo *readerInfo);
    protocol::ErrorCode getBlockFromFile(const std::string &fileName,
                                         int64_t offset,
                                         bool isAppending,
                                         util::BlockPtr &block,
                                         util::ReaderInfo *readerInfo,
                                         const util::ReaderInfoMap *readerInfoMap);
    FileWrapperPtr getFileWrapper(const std::string &fileName, bool isAppending, int64_t maxOffset);
    void putBlockIntoCache(const std::string &fileName, int64_t offset, const util::BlockPtr &block);
    int64_t calBlockId(int64_t offset);
    protocol::ErrorCode innerReadBlock(const std::string &fileName,
                                       int64_t offset,
                                       bool isAppending,
                                       util::TimeoutChecker *timeoutChecker,
                                       util::BlockPtr &block,
                                       bool &isFromCache,
                                       util::ReaderInfo *readerInfo,
                                       const util::ReaderInfoMap *readerInfoMap = NULL);
    bool hasBadFile(const std::vector<FileWrapperPtr> &fileWrappers);
    int64_t getLastAccessTime(const std::vector<FileWrapperPtr> &fileWrappers);
    int64_t recycleBlockObsolete(const util::ReaderInfoMap *readerInfoMap,
                                 monitor::RecycleBlockMetricsCollector &collector);
    int64_t recycleBlockByDis(const util::ReaderInfoMap *readerInfoMap,
                              int64_t metaThreshold,
                              int64_t dataThreshold,
                              monitor::RecycleBlockMetricsCollector &collector);
    int64_t
    getNearestReaderDis(const FileBlockKey &blockKey, const util::ReaderInfoMap *readerInfoMap, bool isDataBlock);
    bool getFileMsgId(const std::string &fileName, int64_t &msgId);
    void collectBlockMetrics(protocol::ErrorCode ec,
                             const std::string &fileName,
                             bool isFromCache,
                             util::BlockPtr block,
                             monitor::ReadMetricsCollector *collector);

private:
    FileBlockMap _metaBlockCache;
    FileBlockMap _dataBlockCache;
    FileWrapperMap _fileCache;
    autil::ReadWriteLock _metaBlockRW;
    autil::ReadWriteLock _dataBlockRW;
    autil::ReadWriteLock _fileRW;
    util::BlockPool *_blockPool;
    util::PermissionCenter *_permissionCenter;
    monitor::BrokerMetricsReporter *_metricsReporter;
    uint32_t _oneFileFdNum;
    int64_t _fileReserveThreshold;
    protocol::PartitionId _partitionId;

public:
    static int64_t FILE_ELIMINATE_THRESHOLD;
    static int64_t FILE_BLOCK_THRESHOLD;
    static int64_t RECYCLE_FILE_INTERVAL;
    static float RESERVED_PERCENT;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FileCache);

} // namespace storage
} // namespace swift
