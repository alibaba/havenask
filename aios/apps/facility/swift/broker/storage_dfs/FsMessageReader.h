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

#include <stddef.h>
#include <stdint.h>
#include <vector>

#include "autil/Log.h"
#include "swift/broker/storage_dfs/FileCache.h"
#include "swift/common/Common.h"
#include "swift/common/FilePair.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/ReaderRec.h"

namespace swift {
namespace common {
class FileMessageMeta;
} // namespace common
namespace monitor {
class BrokerMetricsReporter;
struct ReadMetricsCollector;
} // namespace monitor
namespace protocol {
class ConsumptionRequest;
class MessageIdResponse;
class MessageResponse;
} // namespace protocol
namespace storage {
class FileManager;
class FileMessageMetaVec;
struct BlockReadInfo;
} // namespace storage
namespace util {
class BlockPool;
class PermissionCenter;
class TimeoutChecker;
} // namespace util
} // namespace swift

namespace swift {
namespace storage {

class FsMessageReader {
public:
    FsMessageReader(const protocol::PartitionId &partitionId,
                    FileManager *fileManager,
                    util::BlockPool *cachePool,
                    int32_t oneFileFdNum,
                    int64_t fileReserveTime,
                    util::PermissionCenter *permissionCenter,
                    monitor::BrokerMetricsReporter *metricsReporter);
    ~FsMessageReader();

private:
    FsMessageReader(const FsMessageReader &);
    FsMessageReader &operator=(const FsMessageReader &);

public:
    protocol::ErrorCode getMessage(const protocol::ConsumptionRequest *request,
                                   protocol::MessageResponse *response,
                                   util::TimeoutChecker *timeoutChecker,
                                   util::ReaderInfo *readerInfo = NULL,
                                   util::ReaderInfoMap *readerInfoMap = NULL,
                                   monitor::ReadMetricsCollector *collector = NULL);
    protocol::ErrorCode getMessage(const protocol::ConsumptionRequest *request,
                                   protocol::MessageResponse *response,
                                   util::TimeoutChecker *timeoutChecker,
                                   bool &isAppending,
                                   util::ReaderInfo *readerInfo = NULL,
                                   util::ReaderInfoMap *readerInfoMap = NULL,
                                   monitor::ReadMetricsCollector *collector = NULL);
    protocol::ErrorCode getLastMessage(protocol::MessageResponse *response);
    protocol::ErrorCode getMinMessageIdByTime(int64_t timestamp,
                                              protocol::MessageIdResponse *response,
                                              util::TimeoutChecker *timeoutChecker);
    int64_t getMinMessageId() const;
    int64_t getLastMessageId() const;
    void recycle(const util::ReaderInfoMap *readerInfoMap, int64_t metaThreshold, int64_t dataThreshold);
    void recycleFile();
    int64_t getCacheBlockCount();
    bool getFileBlockDis(const util::ReaderInfoMap *readerInfoMap,
                         std::vector<int64_t> &metaDisBlocks,
                         std::vector<int64_t> &dataDisBlocks);
    int64_t getCacheFileCount() { return _fileCache->getCachFileCount(); }
    int64_t getCacheMetaBlockCount() { return _fileCache->getCacheMetaBlockCount(); }
    int64_t getCacheDataBlockCount() { return _fileCache->getCacheDataBlockCount(); }

public:
    // public for client
    protocol::ErrorCode getMinMessageIdByTime(int64_t timestamp,
                                              int64_t &retMsgId,
                                              int64_t &retTimestamp,
                                              util::TimeoutChecker *timeoutChecker);
    bool messageIdValid(int64_t msgId,
                        int64_t msgTime,
                        util::TimeoutChecker *timeoutChecker,
                        monitor::ReadMetricsCollector *collector = NULL);

private:
    void makeOneEmptyMessage(protocol::MessageResponse *response);
    static bool needMakeOneEmptyMessage(protocol::MessageResponse *response, int64_t expactLastMsgId);
    void collectMetrics(const BlockReadInfo &blockReadInfo,
                        util::ReaderInfo *readerInfo,
                        monitor::ReadMetricsCollector *collector);
    protocol::ErrorCode findMetaVecByTime(int64_t timestamp,
                                          const FilePairPtr &filePair,
                                          util::TimeoutChecker *timeoutChecker,
                                          FileMessageMetaVec &metaVec);
    bool getMsgFilePair(int64_t msgId, FilePairPtr &filePairPtr, int64_t &begPos, int64_t &endPos);
    bool getMsgMetaVec(const FilePairPtr &filePairPtr,
                       int64_t beginPos,
                       int64_t endPos,
                       FileMessageMetaVec &metas,
                       util::TimeoutChecker *timeoutChecker,
                       monitor::ReadMetricsCollector *collector);
    bool getMsgMeta(const FilePairPtr &filePairPtr,
                    int64_t beginPos,
                    int64_t endPos,
                    common::FileMessageMeta &meta,
                    util::TimeoutChecker *timeoutChecker,
                    monitor::ReadMetricsCollector *collector);
    bool
    messageTimeValid(uint64_t msgTime, const common::FileMessageMeta &lastMeta, const common::FileMessageMeta &curMeta);

private:
    protocol::PartitionId _partitionId;
    FileManager *_fileManager;
    FileCache *_fileCache;
    monitor::BrokerMetricsReporter *_metricsReporter;

private:
    AUTIL_LOG_DECLARE();
    friend class FsMessageReaderTest;
};

SWIFT_TYPEDEF_PTR(FsMessageReader);

} // namespace storage
} // namespace swift
