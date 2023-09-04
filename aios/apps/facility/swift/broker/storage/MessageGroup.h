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

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/common/MemoryMessage.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/ReaderRec.h"

namespace swift {
namespace monitor {
struct ReadMetricsCollector;
struct WriteMetricsCollector;
} // namespace monitor
namespace protocol {
class ConsumptionRequest;
class MessageIdResponse;
class MessageResponse;
class ProductionRequest;
} // namespace protocol
namespace util {
class BlockPool;
class MessageConverter;
} // namespace util
} // namespace swift

namespace swift {
namespace storage {
class MessageDeque;
class TimestampAllocator;

struct RecycleInfo {
    RecycleInfo() {
        firstMsgTimestamp = -1;
        lastMsgTimestamp = -1;
    }
    int64_t firstMsgTimestamp;
    int64_t lastMsgTimestamp;
};

// a group of MemoryMessage, use blockPool to store the message.

class MessageGroup {
public:
    MessageGroup();
    virtual ~MessageGroup();

private:
    MessageGroup(const MessageGroup &);
    MessageGroup &operator=(const MessageGroup &);

public:
    protocol::ErrorCode init(const protocol::PartitionId &partitionId,
                             util::BlockPool *partitionPool,
                             const protocol::MessageResponse *response = NULL,
                             protocol::TopicMode topicMode = protocol::TOPIC_MODE_NORMAL,
                             bool readNotCommittedMsg = true,
                             int64_t timestampOffsetInUs = 0);
    protocol::ErrorCode getMessage(const protocol::ConsumptionRequest *request,
                                   bool memMode,
                                   protocol::MessageResponse *response,
                                   util::ReaderInfoPtr readerInfo,
                                   monitor::ReadMetricsCollector &collector);
    protocol::ErrorCode addMessage(const protocol::ProductionRequest *request,
                                   protocol::MessageResponse *response,
                                   monitor::WriteMetricsCollector &collector);
    protocol::ErrorCode getMaxMessageId(protocol::MessageIdResponse *response);
    protocol::ErrorCode
    getMinMessageIdByTime(int64_t timestamp, protocol::MessageIdResponse *response, bool &isMinInMemory);
    int64_t getMsgCountInBuffer() const;
    int64_t getLastReceivedMsgId() const;
    int64_t getLeftToBeCommittedDataSize() const;
    int64_t getLastAcceptedMsgTimeStamp() const;
    int64_t getLastAvailableMsgTimestamp() const;
    int64_t getLastAvailableMsgId() const;
    void getMemoryMessage(int64_t startId, size_t count, common::MemoryMessageVector &vec) const;
    void setCommittedId(int64_t id);
    int64_t getCommittedId() const;
    int64_t getCommittedTimestamp() const;
    int64_t getMetaSize() const;
    int64_t getDataSize() const;
    int64_t getMetaCountInOneBlock() const;
    int64_t getRemainMetaCount() const;
    int64_t getRemainDataSize() const;
    int64_t getCommitDelay() const;
    RecycleInfo getRecycleInfo() const;
    bool canRecycle() const;
    // NOTE: the message which is not committed will not be removed.
    void tryRecycle(int64_t recycleSize, int64_t &actualRecycleSize, int64_t &actualRecycleCount);
    void tryRecycleFast(double recyclePercent, int64_t &actualRecycleSize, int64_t &actualRecycleCount);
    void tryRecycleByReaderInfo(int64_t minTimeStamp, int64_t &actualRecycleSize, int64_t &actualRecycleCount);

    bool messageIdValid(int64_t msgId, uint64_t msgTime, bool &needSeek) const;
    virtual int64_t findMessageId(uint64_t msgTime);
    bool hasMsgInRange(uint16_t from, uint16_t to, int64_t begId, int64_t endId) const;

private:
    protocol::ErrorCode prepareBlocks(int64_t totalLen);
    int64_t estimateReserveBlockCount(int64_t duration);

    struct WriteStat {
        bool hasMergedMsg() const { return acceptedMsgCount < acceptedTotalMsgCount; }
        uint32_t acceptedMsgCount = 0;
        uint32_t acceptedTotalMsgCount = 0;
        uint32_t acceptedDataSize = 0;
        uint32_t compressMsgCount = 0;
        int64_t firstAcceptedMsgId = -1;
        uint64_t compressedPayload = 0;
    };
    template <typename ContainerType>
    protocol::ErrorCode addMessage(const ContainerType &container,
                                   bool replicationMode,
                                   bool fillTsForResponse,
                                   protocol::MessageResponse *response,
                                   WriteStat &stat);
    template <typename ContainerType>
    static bool checkReplicationRequest(const ContainerType &data, int64_t lastMsgId, int64_t lastMsgTime);
    bool canReadNotCommittedMsg(bool readNotCommittedMsg, protocol::TopicMode topicMode);

public:
    bool getIsMemOnly() const { return protocol::TOPIC_MODE_MEMORY_ONLY == _topicMode; }

private:
    protocol::PartitionId _partitionId;
    protocol::TopicMode _topicMode;
    mutable autil::ReadWriteLock _rwLock;
    TimestampAllocator *_timestampAllocator = nullptr;
    util::BlockPool *_partitionBlockPool = nullptr;
    MessageDeque *_msgDeque = nullptr;
    util::MessageConverter *_msgConverter = nullptr;

    bool _readNotCommittedMsg = true;

private:
    friend class MessageGroupTest;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(MessageGroup);

} // namespace storage
} // namespace swift
