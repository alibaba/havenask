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
#include "swift/broker/storage_dfs/FsMessageReader.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <string>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "fslib/fs/FileSystem.h"
#include "swift/broker/storage_dfs/BlockMessageReader.h"
#include "swift/broker/storage_dfs/FileManager.h"
#include "swift/broker/storage_dfs/FileMessageMetaVec.h"
#include "swift/broker/storage_dfs/FileWrapper.h"
#include "swift/common/FileMessageMeta.h"
#include "swift/config/ConfigDefine.h"
#include "swift/filter/MessageFilter.h"
#include "swift/monitor/BrokerMetricsReporter.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/Block.h"

namespace swift {
namespace util {
class BlockPool;
class PermissionCenter;
} // namespace util
} // namespace swift

using namespace std;
using namespace autil;
using namespace fslib::fs;
using namespace fslib::cache;
using namespace swift::config;
using namespace swift::protocol;
using namespace swift::util;
using namespace swift::common;
using namespace swift::filter;
using namespace swift::monitor;
namespace swift {
namespace storage {
AUTIL_LOG_SETUP(swift, FsMessageReader);

FsMessageReader::FsMessageReader(const PartitionId &partitionId,
                                 FileManager *fileManager,
                                 BlockPool *cachePool,
                                 int32_t oneFileFdNum,
                                 int64_t fileReserveTime,
                                 PermissionCenter *permissionCenter,
                                 monitor::BrokerMetricsReporter *metricsReporter) {
    _partitionId = partitionId;
    _fileManager = fileManager;
    _metricsReporter = metricsReporter;
    _fileCache =
        new FileCache(cachePool, oneFileFdNum, fileReserveTime, permissionCenter, metricsReporter, _partitionId);
}

FsMessageReader::~FsMessageReader() { DELETE_AND_SET_NULL(_fileCache); }

ErrorCode FsMessageReader::getMessage(const ConsumptionRequest *request,
                                      MessageResponse *response,
                                      TimeoutChecker *timeoutChecker,
                                      bool &isAppending,
                                      ReaderInfo *readerInfo,
                                      ReaderInfoMap *readerInfoMap,
                                      ReadMetricsCollector *collector) {
    if (readerInfo) {
        readerInfo->setReadFile(true);
    }
    isAppending = false;
    int64_t startId = request->startid();
    FilePairPtr filePairPtr = _fileManager->getFilePairById(startId);
    if (!filePairPtr) {
        AUTIL_LOG(WARN,
                  "read partition [%s %d] start [%ld] failed, file not exist!",
                  request->topicname().c_str(),
                  request->partitionid(),
                  request->startid());
        response->clear_nextmsgid();
        response->clear_nexttimestamp();
        return ERROR_BROKER_TIMESTAMP_TOO_LATEST;
    }

    int64_t beginId = filePairPtr->beginMessageId;
    if (beginId > startId) {
        AUTIL_LOG(WARN,
                  "read partition [%s %d] start [%ld] failed, some message loss!",
                  request->topicname().c_str(),
                  request->partitionid(),
                  request->startid());
        response->set_nextmsgid(beginId);
        response->set_nexttimestamp(filePairPtr->beginTimestamp);
        return ERROR_BROKER_SOME_MESSAGE_LOST;
    }
    int64_t endId;
    ErrorCode ec = filePairPtr->getEndMessageId(endId);
    if (ec != ERROR_NONE) {
        AUTIL_LOG(WARN,
                  "read meta [%s] failed, ec [%s], block request startid [%ld]",
                  filePairPtr->metaFileName.c_str(),
                  ErrorCode_Name(ec).c_str(),
                  startId);
        response->set_nextmsgid(startId);
        response->clear_nexttimestamp();
        return ec;
    }
    isAppending = filePairPtr->isAppending();
    BlockMessageReader blockReader(filePairPtr, _fileCache, timeoutChecker, readerInfo, readerInfoMap, collector);
    ec = blockReader.fillMessage(request, response);
    collectMetrics(blockReader.getReadInfo(), readerInfo, collector);
    if (ec != ERROR_BROKER_BUSY && ec != ERROR_NONE) {
        AUTIL_LOG(ERROR,
                  "read partition [%s %d] start id [%ld] failed, ec[%s].",
                  request->topicname().c_str(),
                  request->partitionid(),
                  request->startid(),
                  ErrorCode_Name(ec).c_str());
        ec = ERROR_BROKER_SOME_MESSAGE_LOST; // compatible with old client
    }
    if (ec == ERROR_BROKER_BUSY) {
        if (collector) {
            collector->readBusyError = true;
        }
        ec = ERROR_NONE; // compatible with old client
    }
    return ec;
}

ErrorCode FsMessageReader::getMessage(const ConsumptionRequest *request,
                                      MessageResponse *response,
                                      TimeoutChecker *timeoutChecker,
                                      ReaderInfo *readerInfo,
                                      ReaderInfoMap *readerInfoMap,
                                      ReadMetricsCollector *collector) {
    bool isAppending = false;
    return getMessage(request, response, timeoutChecker, isAppending, readerInfo, readerInfoMap, collector);
}

void FsMessageReader::collectMetrics(const BlockReadInfo &blockReadInfo,
                                     ReaderInfo *readerInfo,
                                     ReadMetricsCollector *collector) {
    if (readerInfo) {
        readerInfo->metaInfo->updateRate(blockReadInfo.dfsMetaSize);
        readerInfo->dataInfo->updateRate(blockReadInfo.dfsDataSize);
        if (blockReadInfo.dfsDataSize > 0 && blockReadInfo.messageCount > 0) {
            readerInfo->dataInfo->msgSize = blockReadInfo.dfsDataSize / blockReadInfo.messageCount;
        }
    }
    if (!collector) {
        return;
    }
    collector->partitionReadFileMetaFailed = blockReadInfo.metaFail;
    collector->partitionReadFileDataFailed = blockReadInfo.dataFail;
    collector->partitionReadMetaRateFromDFS = blockReadInfo.dfsMetaSize;
    collector->partitionReadDataRateFromDFS = blockReadInfo.dfsDataSize;
    collector->partitionActualReadMetaRateFromDFS = blockReadInfo.dfsMetaSize - blockReadInfo.dfsMetaSizeFromCache;
    collector->partitionActualReadDataRateFromDFS = blockReadInfo.dfsDataSize - blockReadInfo.dfsDataSizeFromCache;
    collector->msgReadRateFromDFS = blockReadInfo.messageDataSize;
    collector->msgReadQpsFromDFS = blockReadInfo.messageCount;
    collector->msgReadQpsWithMergedFromDFS = blockReadInfo.mergedCount;
}

ErrorCode FsMessageReader::getLastMessage(MessageResponse *response) {
    FilePairPtr lastFilePair = _fileManager->getLastFilePair();
    if (!lastFilePair) {
        AUTIL_LOG(INFO, "[%s] no file in rootDir, no data recover", _partitionId.ShortDebugString().c_str());
        return ERROR_NONE;
    }
    int64_t beginMessageId = lastFilePair->beginMessageId;
    int64_t endMessageId;
    ErrorCode ec = lastFilePair->getEndMessageId(endMessageId);
    if (ec != ERROR_NONE) {
        AUTIL_LOG(WARN, "[%s] read end message id failed", _partitionId.ShortDebugString().c_str());
        return ec;
    }
    AUTIL_LOG(INFO,
              "[%s] datafile:%s, metafile:%s, beginMessageId:%ld, endMessageId:%ld",
              _partitionId.ShortDebugString().c_str(),
              lastFilePair->dataFileName.c_str(),
              lastFilePair->metaFileName.c_str(),
              beginMessageId,
              endMessageId);
    int64_t count = 1;
    if (endMessageId - count > beginMessageId) {
        beginMessageId = endMessageId - count;
    }
    protocol::ConsumptionRequest request;
    request.mutable_versioninfo()->set_supportfb(false);
    request.set_count(count);
    request.set_startid(beginMessageId);
    ec = getMessage(&request, response, NULL);
    if (needMakeOneEmptyMessage(response, endMessageId - 1)) {
        AUTIL_LOG(WARN, "[%s %d] get last messages failed", _partitionId.topicname().c_str(), _partitionId.id());
        // to support partition continue to load
        makeOneEmptyMessage(response);
        return ERROR_NONE;
    }
    return ec;
}

protocol::ErrorCode FsMessageReader::getMinMessageIdByTime(int64_t timestamp,
                                                           protocol::MessageIdResponse *response,
                                                           TimeoutChecker *timeoutChecker) {
    int64_t retMsgId;
    int64_t retTimestamp;
    ErrorCode ec = getMinMessageIdByTime(timestamp, retMsgId, retTimestamp, timeoutChecker);
    if (ec == ERROR_NONE) {
        response->set_msgid(retMsgId);
        response->set_timestamp(retTimestamp);
    }
    return ec;
}

ErrorCode FsMessageReader::getMinMessageIdByTime(int64_t timestamp,
                                                 int64_t &retMsgId,
                                                 int64_t &retTimestamp,
                                                 TimeoutChecker *timeoutChecker) {
    FilePairPtr filePair = _fileManager->getFilePairByTime(timestamp);
    if (!filePair) {
        return ERROR_BROKER_TIMESTAMP_TOO_LATEST;
    }
    if (filePair->beginTimestamp >= timestamp) {
        retMsgId = filePair->beginMessageId;
        retTimestamp = filePair->beginTimestamp;
        return ERROR_NONE;
    }
    int64_t endPos;
    ErrorCode ec = filePair->getEndMessageId(endPos);
    if (ec != ERROR_NONE) {
        AUTIL_LOG(WARN, "get end id failed for [%s]", filePair->metaFileName.c_str());
        return ec;
    }
    if (endPos == 0) {
        AUTIL_LOG(WARN, "empty file [%s]", filePair->metaFileName.c_str());
        return ERROR_BROKER_SOME_MESSAGE_LOST;
    }
    FileMessageMetaVec metaVec;
    ec = findMetaVecByTime(timestamp, filePair, timeoutChecker, metaVec);
    if (ec != ERROR_NONE) {
        AUTIL_LOG(WARN, "find meta block failed [%s]", filePair->metaFileName.c_str());
        return ec;
    }
    FileMessageMeta meta;
    if (metaVec.fillMeta(metaVec.end() - 1, meta)) {
        if (meta.timestamp < (uint64_t)timestamp) {
            if (endPos == metaVec.end() + filePair->beginMessageId) {
                filePair = filePair->getNext();
                if (filePair) {
                    retTimestamp = filePair->beginTimestamp;
                    retMsgId = filePair->beginMessageId;
                } else {
                    return ERROR_BROKER_TIMESTAMP_TOO_LATEST;
                }
            } else {
                retTimestamp = meta.timestamp + 1;
                retMsgId = metaVec.end() + filePair->beginMessageId;
            }
        } else {
            for (int64_t cursor = metaVec.start(); cursor < metaVec.end(); cursor++) {
                metaVec.fillMeta(cursor, meta);
                if (meta.timestamp >= (uint64_t)timestamp) {
                    retTimestamp = meta.timestamp;
                    retMsgId = filePair->beginMessageId + cursor;
                    break;
                }
            }
        }
    }
    return ERROR_NONE;
}
ErrorCode FsMessageReader::findMetaVecByTime(int64_t timestamp,
                                             const FilePairPtr &filePair,
                                             TimeoutChecker *timeoutChecker,
                                             FileMessageMetaVec &metaVec) {
    int64_t endPos = filePair->getEndMessageId() - filePair->beginMessageId;
    int64_t blockSize = _fileCache->getCacheBlockSize();
    int64_t msgCountInOneBlock = blockSize / FILE_MESSAGE_META_SIZE;
    int64_t blockCount;
    if (endPos % msgCountInOneBlock == 0) {
        blockCount = endPos / msgCountInOneBlock;
    } else {
        blockCount = endPos / msgCountInOneBlock + 1;
    }
    int64_t left = 0;
    int64_t right = blockCount - 1;
    int64_t middle;
    ErrorCode ec = ERROR_NONE;
    while (left <= right) {
        middle = left + (right - left) / 2;
        int64_t begPos = middle * msgCountInOneBlock;
        ec = metaVec.init(_fileCache,
                          filePair->metaFileName,
                          filePair->isAppending(),
                          begPos,
                          endPos,
                          timeoutChecker,
                          NULL,
                          NULL,
                          NULL,
                          false);
        if (ec != ERROR_NONE) {
            return ec;
        }
        FileMessageMeta metaBeg;
        metaVec.fillMeta(begPos, metaBeg);
        if ((uint64_t)timestamp < metaBeg.timestamp) {
            right = middle - 1;
            continue;
        }
        FileMessageMeta metaEnd;
        metaVec.fillMeta(metaVec.end() - 1, metaEnd);
        if ((uint64_t)timestamp > metaEnd.timestamp) {
            left = middle + 1;
            continue;
        }
        break;
    }
    return ec;
}

void FsMessageReader::makeOneEmptyMessage(protocol::MessageResponse *response) {
    int64_t msgid = getLastMessageId() + 1;
    int64_t timestamp = TimeUtility::currentTime();
    response->Clear();
    Message *msg = response->add_msgs();
    msg->set_msgid(msgid);
    msg->set_timestamp(timestamp);
    msg->set_data("");
}

bool FsMessageReader::needMakeOneEmptyMessage(MessageResponse *response, int64_t expactLastMsgId) {
    int size = response->msgs_size();
    if (size == 0) {
        return true;
    }
    const Message &lastMessage = response->msgs(size - 1);
    if (lastMessage.msgid() != expactLastMsgId) {
        return true;
    }
    for (int i = 1; i < size; ++i) {
        if (response->msgs(i - 1).msgid() + 1 != response->msgs(i).msgid()) {
            return true;
        }
    }
    return false;
}

int64_t FsMessageReader::getMinMessageId() const { return _fileManager->getMinMessageId(); }

int64_t FsMessageReader::getLastMessageId() const { return _fileManager->getLastMessageId(); }

bool FsMessageReader::getMsgFilePair(int64_t msgId, FilePairPtr &filePairPtr, int64_t &begPos, int64_t &endPos) {
    filePairPtr = _fileManager->getFilePairById(msgId);
    if (!filePairPtr) {
        AUTIL_LOG(
            WARN, "[%s %d] getFilePairById[%ld] failed", _partitionId.topicname().c_str(), _partitionId.id(), msgId);
        return false;
    }
    FileMessageMetaVec metas;
    begPos = msgId - filePairPtr->beginMessageId;
    if (begPos < 0) {
        AUTIL_LOG(INFO,
                  "[%s] get beg pos error, expect start [%ld] file start [%ld], "
                  "validate id failed",
                  _partitionId.ShortDebugString().c_str(),
                  begPos,
                  filePairPtr->beginMessageId);
        return false;
    }
    ErrorCode ec = filePairPtr->getEndMessageId(endPos);
    endPos = endPos - filePairPtr->beginMessageId;
    if (ec != ERROR_NONE) {
        AUTIL_LOG(INFO, "get end id failed for [%s]", filePairPtr->metaFileName.c_str());
        return false;
    }
    return true;
}

bool FsMessageReader::getMsgMetaVec(const FilePairPtr &filePairPtr,
                                    int64_t begPos,
                                    int64_t endPos,
                                    FileMessageMetaVec &metas,
                                    TimeoutChecker *timeoutChecker,
                                    ReadMetricsCollector *collector) {
    ErrorCode ec = metas.init(_fileCache,
                              filePairPtr->metaFileName,
                              filePairPtr->isAppending(),
                              begPos,
                              endPos,
                              timeoutChecker,
                              NULL,
                              NULL,
                              collector,
                              false);
    if (ec != ERROR_NONE) {
        AUTIL_LOG(INFO, "[%s] init meta vec failed for [%ld]", _partitionId.ShortDebugString().c_str(), begPos);
        return false;
    }
    return true;
}

bool FsMessageReader::getMsgMeta(const FilePairPtr &filePairPtr,
                                 int64_t begPos,
                                 int64_t endPos,
                                 FileMessageMeta &meta,
                                 TimeoutChecker *timeoutChecker,
                                 ReadMetricsCollector *collector) {
    FileMessageMetaVec metas;
    if (!getMsgMetaVec(filePairPtr, begPos, endPos, metas, timeoutChecker, collector)) {
        return false;
    }
    if (metas.fillMeta(begPos, meta)) {
        return true;
    } else {
        AUTIL_LOG(INFO,
                  "[%s] position [%ld] not found in meta vec from [%ld] to [%ld]",
                  _partitionId.ShortDebugString().c_str(),
                  begPos,
                  metas.start(),
                  metas.end());
        return false;
    }
}

bool FsMessageReader::messageTimeValid(uint64_t msgTime,
                                       const FileMessageMeta &lastMeta,
                                       const FileMessageMeta &curMeta) {
    return lastMeta.timestamp < msgTime && curMeta.timestamp >= msgTime;
}

bool FsMessageReader::messageIdValid(int64_t msgId,
                                     int64_t msgTime,
                                     TimeoutChecker *timeoutChecker,
                                     ReadMetricsCollector *collector) {
    if (msgId == 0) {
        return true;
    }
    assert(msgId > 0); // msgId = 0 is always valid and do not need to seek
    int64_t preMsgId = msgId - 1;
    int64_t begPos = 0;
    int64_t endPos = 0;
    FilePairPtr preMsgFilePair;
    if (!getMsgFilePair(preMsgId, preMsgFilePair, begPos, endPos)) {
        return false;
    }
    FileMessageMeta preMeta, curMeta;
    if (begPos + 1 == endPos) { // preMsg in file last
        int64_t curBegPos = 0;
        int64_t curEndPos = 0;
        FilePairPtr curMsgFilePair;
        if (!getMsgFilePair(msgId, curMsgFilePair, curBegPos, curEndPos)) {
            return false;
        }
        if (!getMsgMeta(preMsgFilePair, begPos, endPos, preMeta, timeoutChecker, collector) ||
            !getMsgMeta(curMsgFilePair, curBegPos, curEndPos, curMeta, timeoutChecker, collector)) {
            return false;
        }
    } else {
        FileMessageMetaVec metas;
        if (!getMsgMetaVec(preMsgFilePair, begPos, endPos, metas, timeoutChecker, collector)) {
            return false;
        }
        if (!metas.fillMeta(begPos, preMeta)) {
            AUTIL_LOG(INFO,
                      "[%s] preMsg position [%ld] not found in meta vec from [%ld] to [%ld]",
                      _partitionId.ShortDebugString().c_str(),
                      begPos,
                      metas.start(),
                      metas.end());
            return false;
        }
        if (!metas.fillMeta(begPos + 1, curMeta)) {
            AUTIL_LOG(INFO,
                      "[%s] curMsg position [%ld] not found in meta vec from [%ld] to [%ld], try next block",
                      _partitionId.ShortDebugString().c_str(),
                      begPos + 1,
                      metas.start(),
                      metas.end());
            if (!getMsgMeta(preMsgFilePair, begPos + 1, endPos, curMeta, timeoutChecker, collector)) {
                return false;
            }
        }
    }
    return messageTimeValid(msgTime, preMeta, curMeta);
}

void FsMessageReader::recycle(const ReaderInfoMap *readerInfoMap, int64_t metaThreshold, int64_t dataThreshold) {
    _fileCache->recycle(readerInfoMap, metaThreshold, dataThreshold);
}

void FsMessageReader::recycleFile() { _fileCache->recycleFile(); }

int64_t FsMessageReader::getCacheBlockCount() { return _fileCache->getBlockCount(); }

bool FsMessageReader::getFileBlockDis(const ReaderInfoMap *readerInfoMap,
                                      vector<int64_t> &metaDisBlocks,
                                      vector<int64_t> &dataDisBlocks) {
    return _fileCache->getBlockDis(readerInfoMap, metaDisBlocks, dataDisBlocks);
}

} // namespace storage
} // namespace swift
