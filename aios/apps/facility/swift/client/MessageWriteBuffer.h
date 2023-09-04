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

#include <deque>
#include <map>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/ThreadPool.h"
#include "swift/client/BufferSizeLimiter.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/RangeUtil.h"
#include "swift/client/SwiftWriterConfig.h"
#include "swift/common/Common.h"
#include "swift/util/BlockPool.h"
#include "swift/util/MessageQueue.h"

namespace autil {
class ZlibCompressor;
} // namespace autil

namespace swift {
namespace common {
class ClientMemoryMessage;
class MessageInfo;
} // namespace common
namespace protocol {
class FBMessageWriter;
class ProductionRequest;
} // namespace protocol
namespace util {
class MessageConverter;
} // namespace util

namespace client {

class MessageWriteBuffer {
public:
    MessageWriteBuffer(util::BlockPoolPtr blockPtr,
                       autil::ThreadPoolPtr mergeThreadPool = autil::ThreadPoolPtr(),
                       BufferSizeLimiterPtr limter = BufferSizeLimiterPtr(),
                       int64_t mergeThreshold = SwiftWriterConfig::DEFAULT_REQUEST_SEND_BYTE);
    ~MessageWriteBuffer();

private:
    MessageWriteBuffer(const MessageWriteBuffer &);
    MessageWriteBuffer &operator=(const MessageWriteBuffer &);

public:
    bool addWriteMessage(MessageInfo &messageInfo);
    size_t fillRequest(protocol::ProductionRequest &request, size_t byteCount);
    size_t fillRequest(protocol::FBMessageWriter *writer, size_t byteCount, int64_t &fillCount);
    void updateBuffer(int64_t acceptedMsgCount, int64_t &checkPointId);
    void updateBuffer(int64_t acceptedMsgCount,
                      int64_t committedId,
                      int64_t accecptedMsgBeginId,
                      int64_t &checkPointId,
                      const std::vector<int64_t> &timestamps,
                      std::vector<std::pair<int64_t, int64_t>> &committedCpTs);
    void resetUnsendCursor();
    bool latestCheckPointId(int64_t &checkpoint) const;
    bool maxCheckPointId(int64_t &checkpoint) const;
    void clear();
    size_t getSize();
    size_t getQueueSize();
    size_t getUnsendCount();
    size_t getUnsendSize();
    size_t getUncommittedCount();
    size_t getUncommittedSize();
    void updateMergeInfo(bool mergeMsg, uint32_t mergeCount, uint32_t mergeSize, uint64_t compressThresholdInBytes);
    void updateRangeUtil(const RangeUtilPtr &rangeUtil);
    void setTopicName(const std::string &topicName);
    void setPartitionId(uint32_t partitionId);
    void setInMerge(bool flag);

    void convertMessage(); // for merge thread pool
    bool stealUnsendMessage(std::vector<common::MessageInfo> &msgInfoVec);
    void setSealed(bool sealed) { _sealed = sealed; }
    bool getSealed() { return _sealed; }

private:
    bool compressAndAddMsgToSend(MessageInfo *msgInfo, autil::ZlibCompressor *compressor, bool compressFlag = true);
    bool addMsgToSend(const MessageInfo &sendMsg);
    void updateSendBuffer(int64_t acceptedMsgCount);
    int64_t updateCommitBuffer(int64_t committedId,
                               int64_t acceptedMsgBeginId,
                               int64_t acceptedMsgCount,
                               const std::vector<int64_t> &timestamps,
                               std::vector<int64_t> &committedTs);
    int64_t popWriteMessage(int64_t count, std::vector<int64_t> &committedCp);
    size_t getMetaSize() const;
    bool mergeMessage();
    bool doConvertMessage();
    size_t getLeftToMergeCount();
    void cleanMergeInfo();
    void cleanMsgs(std::vector<MessageInfo *> &msgVec);

    bool isLastConvertFinished();
    void doMergeMessage(uint16_t hashId,
                        const std::vector<MessageInfo *> &msgVec,
                        MessageInfo &msgInfo,
                        uint64_t dataLen,
                        autil::ZlibCompressor *compressor);
    void addDataSize(int64_t size);
    void subDataSize(int64_t size);
    int64_t getUnsendCountInSendQueue();
    bool getUnSendMsgs(std::vector<common::MessageInfo> &msgInfoVec);

private:
    mutable autil::ThreadMutex _writeQueueMutex;
    mutable autil::ReadWriteLock _rwLock;
    mutable autil::ThreadMutex _dataSizeMutex;
    std::deque<MessageInfo *> _writeQueue;
    volatile int64_t _writeQueueSize;
    int64_t _mergeThreshold;
    util::MessageQueue<common::ClientMemoryMessage> _sendQueue;
    // pair <begin msgid, endmsgid>, msg timestamp vec
    std::deque<std::pair<std::pair<int64_t, int64_t>, std::vector<int64_t>>> _uncommittedMsgs;
    // for merge
    volatile bool _inMerge;
    std::deque<MessageInfo *> _toMergeQueue;
    std::map<uint16_t, std::vector<MessageInfo *>> _msgMap;
    std::map<uint16_t, uint64_t> _sizeMap;
    std::string _topicName;
    uint32_t _partitionId;
    volatile size_t _unsendCursor;
    volatile size_t _uncommittedSize;
    volatile int64_t _dataSize;
    volatile bool _canAdd;
    bool _sealed;
    int64_t _minCheckpointIdInMerge;
    int64_t _maxCheckpointIdInMerge;
    bool _ignoreUpdate;
    bool _mergeMsg;
    uint64_t _mergeCount;
    uint64_t _mergeSize;
    uint64_t _compressThresholdInBytes;
    BufferSizeLimiterPtr _limiter;
    RangeUtilPtr _rangeUtil;
    util::MessageConverter *_msgConverter;
    autil::ThreadPoolPtr _mergeThreadPool;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(MessageWriteBuffer);

} // namespace client
} // namespace swift
