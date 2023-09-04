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

#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/common/FilePair.h"
#include "swift/filter/MessageFilter.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/Block.h"
#include "swift/util/ReaderRec.h"

namespace swift {
namespace common {
class FileMessageMeta;
} // namespace common
namespace monitor {
struct ReadMetricsCollector;
} // namespace monitor
namespace protocol {
class ConsumptionRequest;
class MessageResponse;
} // namespace protocol
namespace storage {
class FileCache;
class FileMessageMetaVec;
} // namespace storage
namespace util {
class TimeoutChecker;
} // namespace util
} // namespace swift

namespace swift {
namespace storage {

struct BlockReadInfo {
    BlockReadInfo()
        : dfsMetaSize(0)
        , dfsDataSize(0)
        , dfsMetaSizeFromCache(0)
        , dfsDataSizeFromCache(0)
        , messageDataSize(0)
        , messageCount(0)
        , mergedCount(0)
        , metaFail(false)
        , dataFail(false) {}
    int64_t dfsMetaSize;
    int64_t dfsDataSize;
    int64_t dfsMetaSizeFromCache;
    int64_t dfsDataSizeFromCache;
    int64_t messageDataSize;
    int64_t messageCount;
    int64_t mergedCount;
    bool metaFail;
    bool dataFail;
};

class BlockMessageReader {
public:
    BlockMessageReader(const FilePairPtr &filePair,
                       FileCache *fileCache,
                       util::TimeoutChecker *timeChecker,
                       util::ReaderInfo *readerInfo = NULL,
                       util::ReaderInfoMap *readerInfoMap = NULL,
                       monitor::ReadMetricsCollector *collector = NULL);
    ~BlockMessageReader();

private:
    BlockMessageReader(const BlockMessageReader &);
    BlockMessageReader &operator=(const BlockMessageReader &);

public:
    protocol::ErrorCode fillMessage(const protocol::ConsumptionRequest *request, protocol::MessageResponse *response);

    BlockReadInfo getReadInfo() { return _blockReadInfo; }

private:
    protocol::ErrorCode fillMessage(common::FileMessageMeta &meta, int64_t &msgId, char *&data, int64_t &dataLen);
    protocol::ErrorCode doFillMessageFB(const FileMessageMetaVec &fileMetaVec,
                                        const filter::MessageFilter &filter,
                                        int64_t maxReadCount,
                                        int64_t maxReadSize,
                                        protocol::MessageResponse *response,
                                        int64_t &cursor);
    protocol::ErrorCode doFillMessagePB(const FileMessageMetaVec &fileMetaVec,
                                        const filter::MessageFilter &filter,
                                        int64_t maxReadCount,
                                        int64_t maxReadSize,
                                        protocol::MessageResponse *response,
                                        int64_t &cursor);
    bool hasDataInBuffer(int64_t startOffset);
    protocol::ErrorCode readFile(int64_t beginPos, int64_t len, char *buf);

private:
    FilePairPtr _filePair;
    util::BlockPtr _dataBlock;
    util::TimeoutChecker *_timeoutChecker;
    FileCache *_fileCache;
    int64_t _bufferOffset;
    int64_t _bufferDataSize;
    BlockReadInfo _blockReadInfo;
    util::ReaderInfo *_readerInfo;
    util::ReaderInfoMap *_readerInfoMap;
    monitor::ReadMetricsCollector *_readMetricsCollector;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(BlockMessageReader);

} // namespace storage
} // namespace swift
