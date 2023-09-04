#include "swift/broker/storage/MessageGroup.h"

#include <algorithm>
#include <iostream>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <unistd.h>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "flatbuffers/flatbuffers.h"
#include "swift/broker/storage/MessageDeque.h"
#include "swift/common/Common.h"
#include "swift/common/MemoryMessage.h"
#include "swift/monitor/BrokerMetricsReporter.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/FBMessageWriter.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/SwiftMessage_generated.h"
#include "swift/util/Block.h"
#include "swift/util/BlockPool.h"
#include "swift/util/ReaderRec.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

using namespace swift::monitor;
using namespace swift::util;
using namespace swift::common;

namespace swift {
namespace storage {

using namespace swift::protocol;

class MessageGroupTest : public TESTBASE {
public:
    void setUp() {
        _pool = new BlockPool(102400, 1024);
        _partitionId.set_topicname("topic1");
        _partitionId.set_id(0);
    }

    void tearDown() { delete _pool; }

private:
    void addMessage(MessageGroup &msgGroup, bool useFB = false);
    void addMessage(MessageGroup &msgGroup, int64_t dataCount, size_t dataSize);
    void constructRequest(ConsumptionRequest &request,
                          int64_t startId,
                          int64_t count,
                          uint16_t from = 0,
                          uint16_t to = 65535,
                          uint8_t mask = 0,
                          uint8_t maskResult = 0,
                          int64_t maxTotalSize = 67108864);
    ErrorCode doGetMessage(MessageGroup &msgGroup,
                           const ConsumptionRequest &request,
                           MessageResponse &response,
                           bool memMode = false);
    void checkResponse(const MessageResponse &response, int64_t maxMsgId, int64_t msgCount, int64_t nextMsgId);
    void innerTestGetMessage(MessageGroup &msgGroup,
                             int64_t startId,
                             int64_t count,
                             int64_t maxMsgId,
                             int retMsgCount,
                             int64_t nextMsgId,
                             int64_t maxTotalSize = 67108864,
                             uint16_t from = 0,
                             uint16_t to = 65535,
                             uint8_t mask = 0,
                             uint8_t maskResult = 0);

private:
    protocol::MessageResponse *prepareRecoverData();
    protocol::MessageResponse *prepareRecycleData(int size, int64_t begId = 0);
    protocol::ProductionRequest *prepareAddMessageRequest(int size);

private:
    util::BlockPool *_pool;
    std::string _topicName;
    protocol::PartitionId _partitionId;
};

TEST_F(MessageGroupTest, testNoData) {
    MessageGroup msgGroup;
    msgGroup.init(_partitionId, _pool);

    MessageIdResponse idResponse;
    ConsumptionRequest request;
    request.set_startid(0);
    request.set_count(1);

    MessageResponse response;
    bool isMinInMemory = false;
    monitor::ReadMetricsCollector collector(_partitionId.topicname(), _partitionId.id());
    EXPECT_EQ(ERROR_BROKER_NO_DATA, msgGroup.getMessage(&request, false, &response, ReaderInfoPtr(), collector));
    EXPECT_EQ(ERROR_BROKER_NO_DATA, msgGroup.getMaxMessageId(&idResponse));
    EXPECT_EQ(ERROR_BROKER_NO_DATA, msgGroup.getMinMessageIdByTime(0, &idResponse, isMinInMemory));
    EXPECT_EQ((int64_t)0, msgGroup.getDataSize());
    EXPECT_EQ((int64_t)-1, msgGroup.getLastReceivedMsgId());
    int64_t recycleCount;
    int64_t recycleSize;
    msgGroup.tryRecycle(0, recycleSize, recycleCount);
    EXPECT_EQ((int64_t)0, recycleCount);
}

TEST_F(MessageGroupTest, testRecover) {
    MessageGroup msgGroup;
    MessageResponse *response = prepareRecoverData();
    msgGroup.init(_partitionId, _pool, response);
    EXPECT_EQ((int64_t)45, msgGroup.getDataSize());
    EXPECT_EQ((int64_t)19, msgGroup.getLastReceivedMsgId());

    MessageIdResponse idResponse;
    MessageResponse res;

    bool isMinInMemory = false;
    Filter filter;
    monitor::ReadMetricsCollector collector(_partitionId.topicname(), _partitionId.id());
    {
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(10);
        EXPECT_EQ(ERROR_BROKER_NO_DATA_IN_MEM, msgGroup.getMessage(&request, false, &res, ReaderInfoPtr(), collector));
        EXPECT_EQ(int64_t(19), res.maxmsgid());
        EXPECT_EQ((int)0, res.msgs().size());
    }

    {
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(100);

        EXPECT_EQ(ERROR_BROKER_NO_DATA_IN_MEM, msgGroup.getMessage(&request, false, &res, ReaderInfoPtr(), collector));
        EXPECT_EQ(int64_t(19), res.maxmsgid());
        EXPECT_EQ((int)0, res.msgs().size());

        EXPECT_EQ(ERROR_NONE, msgGroup.getMessage(&request, true, &res, ReaderInfoPtr(), collector));
        EXPECT_EQ(int64_t(19), res.maxmsgid());
        EXPECT_EQ((int)10, res.msgs().size());
        res.Clear();
    }

    {
        ConsumptionRequest request;

        request.set_startid(9);
        request.set_count(11);

        EXPECT_EQ(ERROR_BROKER_NO_DATA_IN_MEM, msgGroup.getMessage(&request, false, &res, ReaderInfoPtr(), collector));
        EXPECT_EQ(int64_t(19), res.maxmsgid());
        EXPECT_EQ((int)0, res.msgs().size());

        EXPECT_EQ(ERROR_NONE, msgGroup.getMessage(&request, true, &res, ReaderInfoPtr(), collector));
        EXPECT_EQ(int64_t(19), res.maxmsgid());
        EXPECT_EQ((int)10, res.msgs().size());
    }

    for (int i = 0; i < 10; i++) {
        const Message &msg = res.msgs(i);
        EXPECT_EQ(int64_t(10 + i), msg.msgid());
        EXPECT_EQ(int64_t(100 + i), msg.timestamp());
        EXPECT_EQ(string(i, i % 26 + 'a'), msg.data());
    }

    EXPECT_EQ(ERROR_NONE, msgGroup.getMaxMessageId(&idResponse));
    EXPECT_EQ((int64_t)19, idResponse.msgid());
    EXPECT_EQ((int64_t)109, idResponse.timestamp());

    EXPECT_EQ(ERROR_NONE, msgGroup.getMinMessageIdByTime(0, &idResponse, isMinInMemory));
    EXPECT_EQ((int64_t)10, idResponse.msgid());
    EXPECT_EQ((int64_t)100, idResponse.timestamp());
    EXPECT_TRUE(isMinInMemory);

    EXPECT_EQ((int64_t)45, msgGroup.getDataSize());
    EXPECT_EQ((int64_t)19, msgGroup.getLastReceivedMsgId());
    delete response;
}

void MessageGroupTest::addMessage(MessageGroup &msgGroup, bool useFB) {
    ProductionRequest pReq;
    MessageResponse response;
    if (useFB) {
        FBMessageWriter writer;
        for (int64_t i = 0; i < 10; ++i) {
            writer.addMessage(StringUtil::toString(i), 0, 0, i);
        }
        pReq.set_messageformat(MF_FB);
        pReq.set_fbmsgs(writer.data(), writer.size());
    } else {
        for (int64_t i = 0; i < 10; ++i) {
            protocol::Message *msg = pReq.add_msgs();
            msg->set_data(StringUtil::toString(i));
            msg->set_uint16payload(i);
        }
    }
    WriteMetricsCollector collector(_partitionId.topicname(), _partitionId.id());
    msgGroup.addMessage(&pReq, &response, collector);
    ASSERT_EQ(10, response.acceptedmsgcount());
    ASSERT_EQ(0, response.timestamps_size());
}

static void constructProductionRequest(ProductionRequest &request,
                                       size_t msgCount,
                                       bool needTimestamp = false,
                                       bool replicationMode = false,
                                       int64_t startMsgId = 0,
                                       int64_t startTimestamp = 0) {
    for (size_t i = 0; i < msgCount; ++i) {
        protocol::Message *msg = request.add_msgs();
        msg->set_data(StringUtil::toString(i));
        if (replicationMode) {
            msg->set_msgid(startMsgId + i);
            msg->set_timestamp(startTimestamp + i);
        }
    }
    request.set_needtimestamp(needTimestamp);
    request.set_replicationmode(replicationMode);
}

TEST_F(MessageGroupTest, testAddMessage) {
    MessageGroup msgGroup;
    msgGroup.init(_partitionId, _pool);
    EXPECT_EQ((int64_t)0, msgGroup.getDataSize());
    ProductionRequest req;
    constructProductionRequest(req, 10, true);
    MessageResponse response;
    WriteMetricsCollector collector(_partitionId.topicname(), _partitionId.id());
    ErrorCode errCode = msgGroup.addMessage(&req, &response, collector);
    EXPECT_EQ(errCode, ERROR_NONE);
    EXPECT_EQ(response.acceptedmsgcount(), (uint32_t)10);
    EXPECT_EQ((int64_t)10, msgGroup.getDataSize());
    EXPECT_EQ((int64_t)9, msgGroup.getLastReceivedMsgId());
    EXPECT_EQ(10, response.timestamps_size());
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ((*msgGroup._msgDeque)[i].getTimestamp(), response.timestamps(i));
    }
}

TEST_F(MessageGroupTest, testAddReplicationMessage) {
    MessageGroup msgGroup;
    msgGroup.init(_partitionId, _pool);
    ProductionRequest request;
    constructProductionRequest(request, 10, false, true);
    MessageResponse response;
    WriteMetricsCollector collector(_partitionId.topicname(), _partitionId.id());
    auto ec = msgGroup.addMessage(&request, &response, collector);
    ASSERT_EQ(ERROR_NONE, ec);
    EXPECT_EQ((uint32_t)10, response.acceptedmsgcount());
    EXPECT_EQ((int64_t)9, msgGroup.getLastReceivedMsgId());
    for (int64_t i = 0; i < 10; ++i) {
        EXPECT_EQ(i, (*msgGroup._msgDeque)[i].getTimestamp());
    }

    request.Clear();
    response.Clear();
    constructProductionRequest(request, 10, false, true, 10, 10);
    ec = msgGroup.addMessage(&request, &response, collector);
    ASSERT_EQ(ERROR_NONE, ec);
    EXPECT_EQ((uint32_t)10, response.acceptedmsgcount());
    EXPECT_EQ((int64_t)19, msgGroup.getLastReceivedMsgId());
    for (int64_t i = 0; i < 20; ++i) {
        EXPECT_EQ(i, (*msgGroup._msgDeque)[i].getTimestamp());
    }

    request.Clear();
    response.Clear();
    constructProductionRequest(request, 10, false, true, 18, 20);
    ec = msgGroup.addMessage(&request, &response, collector);
    ASSERT_EQ(ERROR_BROKER_REQUEST_INVALID, ec);
    EXPECT_EQ((int64_t)19, msgGroup.getLastReceivedMsgId());

    request.Clear();
    response.Clear();
    constructProductionRequest(request, 10, false, true, 22, 22);
    ec = msgGroup.addMessage(&request, &response, collector);
    ASSERT_EQ(ERROR_BROKER_REQUEST_INVALID, ec);
    EXPECT_EQ((int64_t)19, msgGroup.getLastReceivedMsgId());
}

TEST_F(MessageGroupTest, testAddMessageMemoryOnlyMode) {
    MessageGroup msgGroup;
    msgGroup.init(_partitionId, _pool, nullptr, protocol::TOPIC_MODE_MEMORY_ONLY);
    EXPECT_EQ((int64_t)0, msgGroup.getDataSize());
    ProductionRequest pReq;
    pReq.set_needtimestamp(true);
    MessageResponse response;
    for (int64_t i = 0; i < 10; ++i) {
        protocol::Message *msg = pReq.add_msgs();
        msg->set_data(StringUtil::toString(i));
    }
    WriteMetricsCollector collector(_partitionId.topicname(), _partitionId.id());
    ErrorCode errCode = msgGroup.addMessage(&pReq, &response, collector);
    EXPECT_EQ(errCode, ERROR_NONE);
    EXPECT_EQ(response.acceptedmsgcount(), (uint32_t)10);
    EXPECT_EQ((int64_t)10, msgGroup.getDataSize());
    EXPECT_EQ((int64_t)9, msgGroup.getLastReceivedMsgId());
    EXPECT_EQ(10, response.timestamps_size());
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ((*msgGroup._msgDeque)[i].getTimestamp(), response.timestamps(i));
    }
    ASSERT_EQ(0, msgGroup.getLeftToBeCommittedDataSize());
    ASSERT_EQ(0, msgGroup.getCommitDelay());
}

TEST_F(MessageGroupTest, testGetMessageWithNoData) {
    MessageGroup msgGroup;
    msgGroup.init(_partitionId, _pool);
    EXPECT_EQ((int64_t)0, msgGroup.getDataSize());

    MessageResponse response;
    ConsumptionRequest request;
    request.set_startid(0);
    request.set_count(2);
    ReadMetricsCollector collector(_partitionId.topicname(), _partitionId.id());

    EXPECT_EQ(ERROR_BROKER_NO_DATA, msgGroup.getMessage(&request, false, &response, ReaderInfoPtr(), collector));
    EXPECT_TRUE(response.has_nextmsgid());
    EXPECT_EQ(int64_t(0), response.nextmsgid());
    EXPECT_TRUE(response.has_nexttimestamp());
    EXPECT_TRUE(!response.has_maxmsgid());
    EXPECT_TRUE(!response.has_maxtimestamp());
}

TEST_F(MessageGroupTest, testGetMessage) {
    MessageGroup msgGroup;
    msgGroup.init(_partitionId, _pool);
    EXPECT_EQ((int64_t)0, msgGroup.getDataSize());

    addMessage(msgGroup);
    ReadMetricsCollector collector(_partitionId.topicname(), _partitionId.id());
    {
        // test get(0, 0)
        MessageResponse response;
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(0);

        EXPECT_EQ(ERROR_NONE, msgGroup.getMessage(&request, false, &response, ReaderInfoPtr(), collector));
        EXPECT_TRUE(response.has_maxmsgid());
        EXPECT_TRUE(response.has_maxtimestamp());
        EXPECT_EQ(int64_t(9), response.maxmsgid());
        EXPECT_EQ(int(0), response.msgs_size());
        EXPECT_TRUE(response.has_nextmsgid());
        EXPECT_TRUE(response.has_nexttimestamp());
        EXPECT_EQ(int64_t(0), response.nextmsgid());
        EXPECT_TRUE(!response.has_fbmsgs());
    }
    msgGroup.setCommittedId(5);
    for (int64_t i = 0; i < 10; ++i) {
        for (int64_t j = 0; j <= 10; ++j) {
            MessageResponse response;
            ConsumptionRequest request;
            request.set_startid(i);
            request.set_count(j);
            EXPECT_EQ(ERROR_NONE, msgGroup.getMessage(&request, false, &response, ReaderInfoPtr(), collector));
            EXPECT_EQ(response.maxmsgid(), int64_t(9));
            EXPECT_TRUE(response.has_maxtimestamp());
            EXPECT_EQ((int)std::min(j, 10 - i), response.msgs_size());
            EXPECT_TRUE(response.has_nextmsgid());
            EXPECT_TRUE(response.has_nexttimestamp());
            EXPECT_EQ(min(i + j, int64_t(10)), response.nextmsgid());
            for (int s = 0; s < response.msgs().size(); ++s) {
                std::string expectData = autil::StringUtil::toString(s + i);
                EXPECT_EQ(expectData, response.msgs(s).data());
            }
        }
    }
    EXPECT_EQ((int64_t)10, msgGroup.getDataSize());
    EXPECT_EQ((int64_t)9, msgGroup.getLastReceivedMsgId());
}

TEST_F(MessageGroupTest, testAdd_GetMessage_FB) {
    MessageGroup msgGroup;
    msgGroup.init(_partitionId, _pool);
    EXPECT_EQ((int64_t)0, msgGroup.getDataSize());

    addMessage(msgGroup, true);
    ReadMetricsCollector collector(_partitionId.topicname(), _partitionId.id());
    {
        // test get(0, 0)
        MessageResponse response;
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportfb(true);
        request.set_startid(0);
        request.set_count(0);

        EXPECT_EQ(ERROR_NONE, msgGroup.getMessage(&request, false, &response, ReaderInfoPtr(), collector));
        EXPECT_TRUE(response.has_maxmsgid());
        EXPECT_TRUE(response.has_maxtimestamp());
        EXPECT_EQ(int64_t(9), response.maxmsgid());
        EXPECT_EQ(int(0), response.msgs_size());
        EXPECT_TRUE(response.has_nextmsgid());
        EXPECT_TRUE(response.has_fbmsgs());
        EXPECT_TRUE(response.has_nexttimestamp());
        EXPECT_EQ(int64_t(0), response.nextmsgid());
    }
    for (int64_t i = 0; i < 10; ++i) {
        for (int64_t j = 0; j <= 10; ++j) {
            MessageResponse response;
            ConsumptionRequest request;
            request.set_startid(i);
            request.set_count(j);
            request.mutable_versioninfo()->set_supportfb(true);
            ASSERT_EQ(ERROR_NONE, msgGroup.getMessage(&request, false, &response, ReaderInfoPtr(), collector));
            ASSERT_EQ(response.maxmsgid(), int64_t(9));
            EXPECT_TRUE(response.has_maxtimestamp());
            ASSERT_EQ(0, response.msgs_size());
            EXPECT_TRUE(response.has_nextmsgid());
            EXPECT_TRUE(response.has_nexttimestamp());
            ASSERT_TRUE(response.has_fbmsgs());
            ASSERT_EQ(min(i + j, int64_t(10)), response.nextmsgid());
            FBMessageReader reader;
            reader.init(response.fbmsgs());
            uint32_t msgSize = reader.size();
            EXPECT_EQ((uint32_t)std::min(j, 10 - i), msgSize);
            for (uint32_t s = 0; s < msgSize; ++s) {
                const protocol::flat::Message *fbMsg = reader.read(s);
                std::string expectData = autil::StringUtil::toString(s + i);
                ASSERT_EQ(expectData, string(fbMsg->data()->c_str(), fbMsg->data()->size()));
            }
        }
    }
    EXPECT_EQ((int64_t)10, msgGroup.getDataSize());
    EXPECT_EQ((int64_t)9, msgGroup.getLastReceivedMsgId());
}

TEST_F(MessageGroupTest, testGetMessageWithExceedMessageId) {
    MessageGroup msgGroup;
    msgGroup.init(_partitionId, _pool);
    EXPECT_EQ((int64_t)0, msgGroup.getDataSize());

    addMessage(msgGroup);

    MessageResponse response;
    ConsumptionRequest request;
    request.set_startid(20);
    request.set_count(10);
    ReadMetricsCollector collector(_partitionId.topicname(), _partitionId.id());

    EXPECT_EQ(ERROR_NONE, msgGroup.getMessage(&request, false, &response, ReaderInfoPtr(), collector));
    EXPECT_TRUE(response.has_maxmsgid());
    EXPECT_TRUE(response.has_maxtimestamp());
    EXPECT_EQ(int64_t(9), response.maxmsgid());
    EXPECT_EQ(int(0), response.msgs_size());
    EXPECT_TRUE(response.has_nextmsgid());
    EXPECT_TRUE(response.has_nexttimestamp());
    EXPECT_EQ(int64_t(10), response.nextmsgid());
}

TEST_F(MessageGroupTest, testGetMaxMessageId) {
    MessageGroup msgGroup;
    msgGroup.init(_partitionId, _pool);
    addMessage(msgGroup);

    MessageIdResponse idResponse;
    EXPECT_EQ(ERROR_NONE, msgGroup.getMaxMessageId(&idResponse));
    EXPECT_EQ((int64_t)9, idResponse.msgid());
    EXPECT_TRUE(idResponse.has_timestamp());
}

TEST_F(MessageGroupTest, testGetMinMessageIdByTime) {
    MessageGroup msgGroup;
    msgGroup.init(_partitionId, _pool);
    addMessage(msgGroup);

    ErrorCode errCode;
    MessageIdResponse idResponse;
    Filter filter;
    bool isMinInMemory = false;
    ReadMetricsCollector collector(_partitionId.topicname(), _partitionId.id());
    for (int64_t i = 0; i < 10; ++i) {
        MessageResponse response;
        ConsumptionRequest request;
        request.set_startid(i);
        request.set_count(1);
        errCode = msgGroup.getMessage(&request, false, &response, ReaderInfoPtr(), collector);
        EXPECT_EQ(ERROR_NONE, errCode);
        EXPECT_EQ(1, response.msgs().size());

        int64_t msgId = response.msgs(0).msgid();
        EXPECT_EQ(msgId, i);

        int64_t timestamp = response.msgs(0).timestamp();

        idResponse.Clear();
        errCode = msgGroup.getMinMessageIdByTime(timestamp, &idResponse, isMinInMemory);
        EXPECT_EQ(ERROR_NONE, errCode);
        EXPECT_EQ(i, (int64_t)idResponse.msgid());
        EXPECT_EQ((i == 0), isMinInMemory);

        idResponse.Clear();
        errCode = msgGroup.getMinMessageIdByTime(timestamp + 1, &idResponse, isMinInMemory);
        EXPECT_EQ(false, isMinInMemory);
        if (i == 9) {
            EXPECT_EQ((int64_t)9, (int64_t)idResponse.msgid());
            EXPECT_EQ(ERROR_BROKER_TIMESTAMP_TOO_LATEST, errCode);
        } else {
            EXPECT_EQ(i + 1, (int64_t)idResponse.msgid());
            EXPECT_EQ(ERROR_NONE, errCode);
        }
    }

    errCode = msgGroup.getMinMessageIdByTime(0, &idResponse, isMinInMemory);
    EXPECT_EQ(ERROR_NONE, errCode);
    EXPECT_EQ(0, (int)idResponse.msgid());
    EXPECT_EQ(true, isMinInMemory);
}

TEST_F(MessageGroupTest, testCanRecycle) {
    BlockPool *centerPool = new BlockPool(MEMORY_MESSAGE_META_SIZE * 210, MEMORY_MESSAGE_META_SIZE);
    BlockPool *partPool = new BlockPool(centerPool, MEMORY_MESSAGE_META_SIZE * 200, MEMORY_MESSAGE_META_SIZE * 40);
    {
        MessageGroup msgGroup;
        ErrorCode ec = msgGroup.init(_partitionId, partPool, nullptr);
        EXPECT_TRUE(ec == ERROR_NONE);
        ASSERT_FALSE(msgGroup.canRecycle());
    }
    {
        MessageGroup msgGroup;
        MessageResponse *response = prepareRecycleData(100, 10);
        unique_ptr<MessageResponse> ptr(response);
        ErrorCode ec = msgGroup.init(_partitionId, partPool, response);
        EXPECT_TRUE(ec == ERROR_NONE);
        msgGroup.setCommittedId(0);
        ASSERT_FALSE(msgGroup.canRecycle());
        msgGroup.setCommittedId(12);
        ASSERT_TRUE(msgGroup.canRecycle());
    }
    DELETE_AND_SET_NULL(partPool);
    DELETE_AND_SET_NULL(centerPool);
}

TEST_F(MessageGroupTest, testCanRecycleLessThanMinBlock) {
    BlockPool *centerPool = new BlockPool(MEMORY_MESSAGE_META_SIZE * 210, MEMORY_MESSAGE_META_SIZE);
    BlockPool *partPool = new BlockPool(centerPool, MEMORY_MESSAGE_META_SIZE * 200, MEMORY_MESSAGE_META_SIZE * 40);
    {
        MessageGroup msgGroup;
        MessageResponse *response = prepareRecycleData(15, 10);
        unique_ptr<MessageResponse> ptr(response);
        ErrorCode ec = msgGroup.init(_partitionId, partPool, response);
        EXPECT_TRUE(ec == ERROR_NONE);
        msgGroup.setCommittedId(20);
        ASSERT_FALSE(msgGroup.canRecycle());
    }
    DELETE_AND_SET_NULL(partPool);
    DELETE_AND_SET_NULL(centerPool);
}

TEST_F(MessageGroupTest, testRecycleBySize) {
    BlockPool *centerPool = new BlockPool(MEMORY_MESSAGE_META_SIZE * 210, MEMORY_MESSAGE_META_SIZE);
    BlockPool *partPool = new BlockPool(centerPool, MEMORY_MESSAGE_META_SIZE * 200, MEMORY_MESSAGE_META_SIZE * 40);

    MessageGroup *msgGroup = new MessageGroup;

    MessageResponse *response = prepareRecycleData(100, 10);
    unique_ptr<MessageResponse> ptr(response);
    ErrorCode ec = msgGroup->init(_partitionId, partPool, response);
    EXPECT_TRUE(ec == ERROR_NONE);
    msgGroup->setCommittedId(0);
    EXPECT_EQ((int64_t)MEMORY_MESSAGE_META_SIZE * 100, msgGroup->getDataSize());
    EXPECT_EQ((int64_t)(100 * MEMORY_MESSAGE_META_SIZE), msgGroup->getMetaSize());
    EXPECT_EQ((int64_t)109, msgGroup->getLastReceivedMsgId());
    int64_t recycled;
    int64_t recycleCount;
    msgGroup->tryRecycle(0, recycled, recycleCount);
    EXPECT_EQ((int64_t)0, recycled);

    msgGroup->setCommittedId(9);
    msgGroup->tryRecycle(1000, recycled, recycleCount);
    EXPECT_EQ((int64_t)0, recycled);

    msgGroup->setCommittedId(10);
    msgGroup->tryRecycle(MEMORY_MESSAGE_META_SIZE * 4, recycled, recycleCount);
    EXPECT_EQ((int64_t)0, recycled);
    EXPECT_EQ((int64_t)0, partPool->getUnusedBlockCount());
    EXPECT_EQ((int64_t)1, partPool->getReserveBlockCount());

    msgGroup->setCommittedId(25);
    msgGroup->tryRecycle(MEMORY_MESSAGE_META_SIZE * 10, recycled, recycleCount);
    EXPECT_EQ((int64_t)MEMORY_MESSAGE_META_SIZE * 10, recycled);
    EXPECT_EQ((int64_t)1, partPool->getUnusedBlockCount());
    EXPECT_EQ((int64_t)1, partPool->getReserveBlockCount());

    msgGroup->tryRecycle(MEMORY_MESSAGE_META_SIZE * 20, recycled, recycleCount);
    EXPECT_EQ((int64_t)MEMORY_MESSAGE_META_SIZE * 20, recycled);
    EXPECT_EQ((int64_t)1, partPool->getUnusedBlockCount());
    EXPECT_EQ((int64_t)1, partPool->getReserveBlockCount());

    msgGroup->setCommittedId(108);
    msgGroup->tryRecycle(MEMORY_MESSAGE_META_SIZE * 300, recycled, recycleCount);
    EXPECT_EQ((int64_t)MEMORY_MESSAGE_META_SIZE * 166, recycled);
    EXPECT_EQ((int64_t)36, partPool->getUnusedBlockCount());
    EXPECT_EQ((int64_t)1, partPool->getReserveBlockCount());

    msgGroup->setCommittedId(109);
    msgGroup->tryRecycle(MEMORY_MESSAGE_META_SIZE * 100, recycled, recycleCount);
    EXPECT_EQ((int64_t)MEMORY_MESSAGE_META_SIZE * 2, recycled);
    EXPECT_EQ((int64_t)38, partPool->getUnusedBlockCount());
    EXPECT_EQ((int64_t)1, partPool->getReserveBlockCount());
    delete msgGroup;
    delete partPool;
    delete centerPool;
}

TEST_F(MessageGroupTest, testRecycleByReaderInfo) {
    BlockPool *centerPool = new BlockPool(MEMORY_MESSAGE_META_SIZE * 210, MEMORY_MESSAGE_META_SIZE);
    BlockPool *partPool = new BlockPool(centerPool, MEMORY_MESSAGE_META_SIZE * 200, MEMORY_MESSAGE_META_SIZE * 40);

    MessageGroup *msgGroup = new MessageGroup;

    MessageResponse *response = prepareRecycleData(100, 0);
    unique_ptr<MessageResponse> ptr(response);
    ErrorCode ec = msgGroup->init(_partitionId, partPool, response);
    EXPECT_TRUE(ec == ERROR_NONE);

    int64_t recycleSize;
    int64_t recycleCount;
    int64_t minTimestamp = 0;
    msgGroup->setCommittedId(10);
    msgGroup->tryRecycleByReaderInfo(minTimestamp, recycleSize, recycleCount);
    EXPECT_EQ(0, recycleSize);
    EXPECT_EQ(0, partPool->getUnusedBlockCount());
    EXPECT_EQ(1, partPool->getReserveBlockCount());

    minTimestamp = 1000 * (10 + 1);
    msgGroup->tryRecycleByReaderInfo(minTimestamp, recycleSize, recycleCount);
    EXPECT_EQ(MEMORY_MESSAGE_META_SIZE * 20, recycleSize);
    EXPECT_EQ(1, partPool->getUnusedBlockCount());
    EXPECT_EQ(1, partPool->getReserveBlockCount());

    msgGroup->setCommittedId(98);
    msgGroup->tryRecycleByReaderInfo(minTimestamp, recycleSize, recycleCount);
    EXPECT_EQ((int64_t)0, recycleSize);
    EXPECT_EQ((int64_t)1, partPool->getUnusedBlockCount());
    EXPECT_EQ((int64_t)1, partPool->getReserveBlockCount());

    minTimestamp = 1000 * (99 + 1);
    msgGroup->tryRecycleByReaderInfo(minTimestamp, recycleSize, recycleCount);
    EXPECT_EQ(MEMORY_MESSAGE_META_SIZE * 176, recycleSize);
    EXPECT_EQ(36, partPool->getUnusedBlockCount());
    EXPECT_EQ(1, partPool->getReserveBlockCount());

    msgGroup->setCommittedId(99);
    minTimestamp = 1000 * (100 + 1);
    msgGroup->tryRecycleByReaderInfo(minTimestamp, recycleSize, recycleCount);
    EXPECT_EQ(80, recycleSize);
    EXPECT_EQ(38, partPool->getUnusedBlockCount());
    EXPECT_EQ(1, partPool->getReserveBlockCount());

    delete msgGroup;
    delete partPool;
    delete centerPool;
}

TEST_F(MessageGroupTest, testRecycleLargeSize) {
    const int MESSAGE_IN_BLOCK = 1024 * 1024 / MEMORY_MESSAGE_META_SIZE; // 1024 * 1024 / 56
    const int BLOCK_SIZE = MEMORY_MESSAGE_META_SIZE * MESSAGE_IN_BLOCK;

    BlockPool *centerPool = new BlockPool(60 * BLOCK_SIZE, BLOCK_SIZE);
    BlockPool *partPool = new BlockPool(centerPool, 60 * BLOCK_SIZE, 15 * BLOCK_SIZE);
    int size = 20 * MESSAGE_IN_BLOCK; // 20M data
    MessageGroup *msgGroup = new MessageGroup;
    MessageResponse *response = prepareRecycleData(size);
    unique_ptr<MessageResponse> responsePtr(response);
    ErrorCode ec = msgGroup->init(_partitionId, partPool, response);
    EXPECT_TRUE(ec == ERROR_NONE);
    msgGroup->setCommittedId(0);
    EXPECT_EQ((int64_t)MEMORY_MESSAGE_META_SIZE * size, msgGroup->getDataSize());
    EXPECT_EQ((int64_t)(size * MEMORY_MESSAGE_META_SIZE), msgGroup->getMetaSize());
    EXPECT_EQ((int64_t)size - 1, msgGroup->getLastReceivedMsgId());
    EXPECT_EQ((int64_t)40, partPool->getUsedBlockCount());
    EXPECT_EQ((int64_t)40, centerPool->getUsedBlockCount());
    ProductionRequest *request = prepareAddMessageRequest(size);
    unique_ptr<ProductionRequest> requestPtr(request);
    responsePtr.reset(new MessageResponse);
    WriteMetricsCollector collector(_partitionId.topicname(), _partitionId.id());
    ec = msgGroup->addMessage(requestPtr.get(), responsePtr.get(), collector);
    EXPECT_TRUE(ec == ERROR_BROKER_BUSY);
    EXPECT_EQ((uint32_t)10 * MESSAGE_IN_BLOCK, responsePtr->acceptedmsgcount());
    int64_t recycled;
    int64_t recycleCount;
    msgGroup->tryRecycle(0, recycled, recycleCount);
    EXPECT_EQ((int64_t)0, recycled);
    msgGroup->setCommittedId(msgGroup->getLastReceivedMsgId());
    msgGroup->tryRecycle(50 * BLOCK_SIZE, recycled, recycleCount);
    EXPECT_EQ((int64_t)50 * BLOCK_SIZE, recycled);
    EXPECT_EQ((int64_t)10, partPool->getUsedBlockCount());
    EXPECT_EQ((int64_t)10, partPool->getUnusedBlockCount());
    EXPECT_EQ((int64_t)20, centerPool->getUsedBlockCount());
    EXPECT_EQ((int64_t)40, centerPool->getUnusedBlockCount());

    ec = msgGroup->addMessage(requestPtr.get(), responsePtr.get(), collector);
    EXPECT_TRUE(ec == ERROR_NONE);
    EXPECT_EQ((uint32_t)20 * MESSAGE_IN_BLOCK, responsePtr->acceptedmsgcount());
    EXPECT_EQ((int64_t)50, partPool->getUsedBlockCount());
    EXPECT_EQ((int64_t)0, partPool->getUnusedBlockCount());
    EXPECT_EQ((int64_t)50, centerPool->getUsedBlockCount());
    EXPECT_EQ((int64_t)10, centerPool->getUnusedBlockCount());
    delete msgGroup;
    delete partPool;
    delete centerPool;
}

TEST_F(MessageGroupTest, testSetCommittedIdWithMemOnly) {
    MessageGroup msgGroup;
    msgGroup.init(_partitionId, _pool, NULL, protocol::TOPIC_MODE_MEMORY_ONLY);
    msgGroup.setCommittedId((int64_t)-1);
    EXPECT_EQ((int64_t)0, msgGroup.getLeftToBeCommittedDataSize());
    addMessage(msgGroup, (int64_t)20, (size_t)10);
    EXPECT_EQ((int64_t)0, msgGroup.getLeftToBeCommittedDataSize());
    msgGroup.setCommittedId((int64_t)19);
    EXPECT_EQ((int64_t)0, msgGroup.getLeftToBeCommittedDataSize());
}

TEST_F(MessageGroupTest, testSetCommittedId) {
    MessageGroup msgGroup;
    msgGroup.init(_partitionId, _pool);
    msgGroup.setCommittedId((int64_t)-1);
    EXPECT_EQ((int64_t)0, msgGroup.getLeftToBeCommittedDataSize());

    addMessage(msgGroup, (int64_t)1, (size_t)10);
    EXPECT_EQ((int64_t)10, msgGroup.getLeftToBeCommittedDataSize());
    msgGroup.setCommittedId((int64_t)0);
    EXPECT_EQ((int64_t)0, msgGroup.getLeftToBeCommittedDataSize());

    addMessage(msgGroup, (int64_t)1, (size_t)15);
    addMessage(msgGroup, (int64_t)1, (size_t)20);
    EXPECT_EQ((int64_t)35, msgGroup.getLeftToBeCommittedDataSize());
    msgGroup.setCommittedId((int64_t)1);
    EXPECT_EQ((int64_t)20, msgGroup.getLeftToBeCommittedDataSize());
    msgGroup.setCommittedId((int64_t)0);
    EXPECT_EQ((int64_t)20, msgGroup.getLeftToBeCommittedDataSize());
    msgGroup.setCommittedId((int64_t)2);
    EXPECT_EQ((int64_t)0, msgGroup.getLeftToBeCommittedDataSize());
    msgGroup.setCommittedId((int64_t)0);
    EXPECT_EQ((int64_t)0, msgGroup.getLeftToBeCommittedDataSize());

    addMessage(msgGroup, (int64_t)1, (size_t)5);
    addMessage(msgGroup, (int64_t)1, (size_t)10);
    addMessage(msgGroup, (int64_t)1, (size_t)15);
    addMessage(msgGroup, (int64_t)1, (size_t)20);
    addMessage(msgGroup, (int64_t)1, (size_t)25);
    addMessage(msgGroup, (int64_t)1, (size_t)30);
    EXPECT_EQ((int64_t)105, msgGroup.getLeftToBeCommittedDataSize());
    msgGroup.setCommittedId((int64_t)4);
    EXPECT_EQ((int64_t)90, msgGroup.getLeftToBeCommittedDataSize());
    msgGroup.setCommittedId((int64_t)6);
    EXPECT_EQ((int64_t)55, msgGroup.getLeftToBeCommittedDataSize());
    msgGroup.setCommittedId((int64_t)4);
    EXPECT_EQ((int64_t)55, msgGroup.getLeftToBeCommittedDataSize());
    msgGroup.setCommittedId((int64_t)8);
    EXPECT_EQ((int64_t)0, msgGroup.getLeftToBeCommittedDataSize());
    msgGroup.setCommittedId((int64_t)18);
    EXPECT_EQ((int64_t)0, msgGroup.getLeftToBeCommittedDataSize());
}

TEST_F(MessageGroupTest, testGetMessageWithRange) {
    MessageGroup msgGroup;
    PartitionId partitionId = _partitionId;
    ReadMetricsCollector collector(_partitionId.topicname(), _partitionId.id());
    msgGroup.init(partitionId, _pool);
    addMessage(msgGroup);
    {
        // test get(0, 0)
        MessageResponse response;
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(0);

        EXPECT_EQ(ERROR_NONE, msgGroup.getMessage(&request, false, &response, ReaderInfoPtr(), collector));
        EXPECT_EQ(response.maxmsgid(), int64_t(9));
        EXPECT_EQ((int)0, response.msgs().size());
    }
    for (int64_t i = 0; i < 10; ++i) {
        for (int64_t j = 0; j <= 10; ++j) {
            MessageResponse response;
            ConsumptionRequest request;
            request.set_startid(i);
            request.set_count(j);
            request.mutable_filter()->set_from(0);
            request.mutable_filter()->set_to(6);
            EXPECT_EQ(ERROR_NONE, msgGroup.getMessage(&request, false, &response, ReaderInfoPtr(), collector));
            EXPECT_EQ(response.maxmsgid(), int64_t(9));
            EXPECT_TRUE(response.has_maxtimestamp());
            EXPECT_EQ((int)(std::min(j, 7 - i) > 0 ? std::min(j, 7 - i) : 0), response.msgs().size());
            for (int s = 0; s < response.msgs().size(); ++s) {
                std::string expectData = autil::StringUtil::toString(s + i);
                EXPECT_EQ(expectData, response.msgs(s).data());
            }
        }
    }
}

void MessageGroupTest::addMessage(MessageGroup &msgGroup, int64_t dataCount, size_t dataSize) {
    ProductionRequest pReq;
    MessageResponse response;
    for (int64_t i = 0; i < dataCount; ++i) {
        protocol::Message *msg = pReq.add_msgs();
        string data(dataSize, StringUtil::toString(i)[0]);
        msg->set_data(data);
        msg->set_uint16payload(i % 10);
        msg->set_uint8maskpayload(i % 256);
    }
    WriteMetricsCollector collector(_partitionId.topicname(), _partitionId.id());
    msgGroup.addMessage(&pReq, &response, collector);
}

TEST_F(MessageGroupTest, testReadCommittedMsg) {
    MessageGroup msgGroup;
    msgGroup.init(_partitionId, _pool); // TOPIC_MODE_NORMAL
    addMessage(msgGroup, 100, 10);

    ConsumptionRequest request;
    constructRequest(request, 0, 10);
    request.set_readcommittedmsg(true);

    MessageResponse response;
    auto ec = doGetMessage(msgGroup, request, response);
    ASSERT_EQ(ERROR_BROKER_NO_DATA, ec);
    checkResponse(response, 0, 0, 0);

    msgGroup.setCommittedId(8);
    response.Clear();
    ec = doGetMessage(msgGroup, request, response);
    ASSERT_EQ(ERROR_NONE, ec);
    checkResponse(response, 8, 9, 9);
}

TEST_F(MessageGroupTest, testGetMessageWithMaxSize) {
    MessageGroup msgGroup;
    msgGroup.init(_partitionId, _pool);
    addMessage(msgGroup, 100, 10);

    innerTestGetMessage(msgGroup, 0, 10, 99, 5, 5, 50);
    innerTestGetMessage(msgGroup, 0, 10, 99, 6, 6, 65);
    innerTestGetMessage(msgGroup, 0, 10, 99, 1, 1, 9);
    innerTestGetMessage(msgGroup, 1, 10, 99, 1, 2, 9);
    innerTestGetMessage(msgGroup, 0, 10, 99, 10, 10, 155);
    innerTestGetMessage(msgGroup, 0, 10, 99, 10, 32, 155, 1, 3);
    innerTestGetMessage(msgGroup, 0, 12, 99, 12, 41, 155, 1, 3);
    innerTestGetMessage(msgGroup, 0, 33, 99, 30, 100, 10000, 1, 3);
}

TEST_F(MessageGroupTest, testGetMessageWithMask) {
    MessageGroup msgGroup;
    msgGroup.init(_partitionId, _pool);
    addMessage(msgGroup, 100, 10);

    innerTestGetMessage(msgGroup, 0, 10, 99, 1, 100, 10000, 0, 65535, 0xff, 0x45);
    innerTestGetMessage(msgGroup, 0, 10, 99, 10, 84, 10000, 0, 65535, 0x07, 0x04);
    innerTestGetMessage(msgGroup, 0, 20, 99, 12, 100, 10000, 0, 65535, 0x07, 0x04);
    innerTestGetMessage(msgGroup, 0, 20, 99, 0, 100, 10000, 0, 65535, 0x08, 0x04);

    innerTestGetMessage(msgGroup, 0, 3, 99, 3, 60, 10000, 0, 2, 0x07, 0x04);
    innerTestGetMessage(msgGroup, 0, 10, 99, 5, 100, 10000, 0, 2, 0x07, 0x04);
}

TEST_F(MessageGroupTest, testAddMessageExceedMaxMemSize) {
    delete _pool;
    _pool = new BlockPool(128, 56);
    MessageGroup msgGroup;
    msgGroup.init(_partitionId, _pool);

    EXPECT_EQ((int64_t)0, msgGroup.getDataSize());

    ProductionRequest pReq;
    MessageResponse response;
    protocol::Message *msg = pReq.add_msgs();
    msg->set_data("abcde");

    WriteMetricsCollector collector(_partitionId.topicname(), _partitionId.id());
    ErrorCode errCode = msgGroup.addMessage(&pReq, &response, collector);
    EXPECT_EQ(errCode, ERROR_NONE);
    EXPECT_EQ(response.acceptedmsgcount(), (uint32_t)1);
    EXPECT_EQ((int64_t)5, msgGroup.getDataSize());
    EXPECT_EQ((int64_t)0, msgGroup.getLastReceivedMsgId());

    errCode = msgGroup.addMessage(&pReq, &response, collector);
    EXPECT_EQ(errCode, ERROR_NONE);
    EXPECT_EQ(response.acceptedmsgcount(), (uint32_t)1);
    EXPECT_EQ((int64_t)10, msgGroup.getDataSize());
    EXPECT_EQ((int64_t)1, msgGroup.getLastReceivedMsgId());

    errCode = msgGroup.addMessage(&pReq, &response, collector);
    EXPECT_EQ(errCode, ERROR_BROKER_BUSY);
    EXPECT_EQ(response.acceptedmsgcount(), (uint32_t)0);
    EXPECT_EQ((int64_t)10, msgGroup.getDataSize());
    EXPECT_EQ((int64_t)1, msgGroup.getLastReceivedMsgId());
}

TEST_F(MessageGroupTest, testValidateMessageId) {
    delete _pool;
    _pool = new BlockPool(128, 56);
    MessageGroup msgGroup;
    msgGroup.init(_partitionId, _pool);

    bool valid = false;
    // empty
    EXPECT_TRUE(msgGroup.messageIdValid(10, 1024, valid));
    EXPECT_TRUE(!valid);
    EXPECT_TRUE(msgGroup.messageIdValid(0, 1024, valid));
    EXPECT_TRUE(valid);

    ProductionRequest req;
    MessageResponse res;
    protocol::Message *msg = req.add_msgs();
    msg->set_data("abcde");

    WriteMetricsCollector collector(_partitionId.topicname(), _partitionId.id());
    ErrorCode ec = msgGroup.addMessage(&req, &res, collector);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(int64_t(1), msgGroup._msgDeque->size());
    const MemoryMessage &memMsg = (*(msgGroup._msgDeque))[0];
    EXPECT_TRUE(msgGroup.messageIdValid(memMsg.getMsgId(), memMsg.getTimestamp(), valid));
    EXPECT_TRUE(valid);

    EXPECT_TRUE(!msgGroup.messageIdValid(memMsg.getMsgId(), memMsg.getTimestamp() - 10, valid));
    EXPECT_TRUE(!valid);

    EXPECT_TRUE(!msgGroup.messageIdValid(memMsg.getMsgId(), memMsg.getTimestamp() + 10, valid));
    EXPECT_TRUE(!valid);

    EXPECT_TRUE(msgGroup.messageIdValid(memMsg.getMsgId() + 1, memMsg.getTimestamp() + 10, valid));
    EXPECT_TRUE(valid);

    EXPECT_TRUE(msgGroup.messageIdValid(memMsg.getMsgId() + 1, memMsg.getTimestamp() - 10, valid));
    EXPECT_TRUE(!valid);

    EXPECT_TRUE(!msgGroup.messageIdValid(memMsg.getMsgId() + 2, memMsg.getTimestamp() + 10, valid));
    EXPECT_TRUE(!valid);
}

TEST_F(MessageGroupTest, testFindMessageId) {
    MessageGroup msgGroup;
    msgGroup.init(_partitionId, _pool);
    { // empty
        EXPECT_EQ(-1, msgGroup.findMessageId(TimeUtility::currentTime()));
    }
    {
        ProductionRequest req;
        MessageResponse res;
        protocol::Message *msg = req.add_msgs();
        msg->set_data("abcde");
        WriteMetricsCollector collector(_partitionId.topicname(), _partitionId.id());
        ErrorCode ec = msgGroup.addMessage(&req, &res, collector);
        EXPECT_EQ(ERROR_NONE, ec);
        usleep(1000);
        ec = msgGroup.addMessage(&req, &res, collector);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ(int64_t(2), msgGroup._msgDeque->size());
        const MemoryMessage &memMsg1 = (*(msgGroup._msgDeque))[0];
        int64_t msg1Time = memMsg1.getTimestamp();
        const MemoryMessage &memMsg2 = (*(msgGroup._msgDeque))[1];
        int64_t msg2Time = memMsg2.getTimestamp();
        EXPECT_EQ(-1, msgGroup.findMessageId(0));
        EXPECT_EQ(-1, msgGroup.findMessageId(msg1Time - 1));
        EXPECT_EQ(0, msgGroup.findMessageId(msg1Time));
        EXPECT_EQ(0, msgGroup.findMessageId(msg1Time + 1));
        EXPECT_EQ(0, msgGroup.findMessageId(msg2Time - 1));
        EXPECT_EQ(1, msgGroup.findMessageId(msg2Time));
        EXPECT_EQ(1, msgGroup.findMessageId(msg2Time + 1));

        EXPECT_EQ(-1, msgGroup.findMessageId(0));
    }
}

void MessageGroupTest::constructRequest(ConsumptionRequest &request,
                                        int64_t startId,
                                        int64_t count,
                                        uint16_t from,
                                        uint16_t to,
                                        uint8_t mask,
                                        uint8_t maskResult,
                                        int64_t maxTotalSize) {
    request.set_startid(startId);
    request.set_count(count);
    request.set_maxtotalsize(maxTotalSize);
    request.mutable_filter()->set_from(from);
    request.mutable_filter()->set_to(to);
    request.mutable_filter()->set_uint8filtermask(mask);
    request.mutable_filter()->set_uint8maskresult(maskResult);
}

ErrorCode MessageGroupTest::doGetMessage(MessageGroup &msgGroup,
                                         const ConsumptionRequest &request,
                                         MessageResponse &response,
                                         bool memMode) {
    ReadMetricsCollector collector(_partitionId.topicname(), _partitionId.id());
    return msgGroup.getMessage(&request, memMode, &response, ReaderInfoPtr(), collector);
}

void MessageGroupTest::checkResponse(const MessageResponse &response,
                                     int64_t maxMsgId,
                                     int64_t msgCount,
                                     int64_t nextMsgId) {
    EXPECT_EQ(maxMsgId, response.maxmsgid());
    EXPECT_EQ(msgCount, response.msgs().size());
    EXPECT_EQ(nextMsgId, response.nextmsgid());
}

void MessageGroupTest::innerTestGetMessage(MessageGroup &msgGroup,
                                           int64_t startId,
                                           int64_t count,
                                           int64_t maxMsgId,
                                           int retMsgCount,
                                           int64_t nextMsgId,
                                           int64_t maxTotalSize,
                                           uint16_t from,
                                           uint16_t to,
                                           uint8_t mask,
                                           uint8_t maskResult) {
    ConsumptionRequest request;
    constructRequest(request, startId, count, from, to, mask, maskResult, maxTotalSize);
    MessageResponse response;
    auto ec = doGetMessage(msgGroup, request, response, false);
    EXPECT_EQ(ERROR_NONE, ec);
    checkResponse(response, maxMsgId, retMsgCount, nextMsgId);
}

MessageResponse *MessageGroupTest::prepareRecoverData() {
    MessageResponse *response = new MessageResponse;
    for (int i = 0; i < 10; i++) {
        Message *msg = response->add_msgs();
        msg->set_data(string(i, i % 26 + 'a'));
        msg->set_msgid(10 + i);
        msg->set_timestamp(i + 100);
        msg->set_uint16payload(i);
    }
    return response;
}

MessageResponse *MessageGroupTest::prepareRecycleData(int size, int64_t begId) {
    MessageResponse *response = new MessageResponse;
    for (int i = 0; i < size; i++) {
        Message *msg = response->add_msgs();
        msg->set_data(string(MEMORY_MESSAGE_META_SIZE, i % 26 + 'a'));
        msg->set_msgid(begId + i);
        msg->set_timestamp(1000 * (i + 1));
        msg->set_uint16payload(i % 65536);
    }
    return response;
}

ProductionRequest *MessageGroupTest::prepareAddMessageRequest(int size) {
    ProductionRequest *request = new ProductionRequest;
    for (int i = 0; i < size; i++) {
        Message *msg = request->add_msgs();
        msg->set_data(string(MEMORY_MESSAGE_META_SIZE, i % 26 + 'a'));
    }
    return request;
}

TEST_F(MessageGroupTest, testGetMessageTime) {
    int64_t poolSize = 4096LL * 1024 * 1024;
    BlockPool *centerPool = new BlockPool(poolSize, 1024 * 1024);
    BlockPool *partPool = new BlockPool(centerPool, poolSize, 1024 * 1024);

    MessageGroup *msgGroup = new MessageGroup;
    MessageResponse *response = prepareRecycleData(1000000);
    unique_ptr<MessageResponse> ptr(response);
    ErrorCode ec = msgGroup->init(_partitionId, partPool, response);
    ReadMetricsCollector collector(_partitionId.topicname(), _partitionId.id());
    EXPECT_TRUE(ec == ERROR_NONE);
    for (int i = 0; i < 10; i++) {
        ConsumptionRequest request;
        MessageResponse response2;
        request.set_startid(0);
        request.set_count(100000);
        request.set_maxtotalsize(100000000);
        int64_t beg = TimeUtility::currentTime();
        msgGroup->getMessage(&request, true, &response2, ReaderInfoPtr(), collector);
        int64_t end = TimeUtility::currentTime();
        cout << "get msg " << i << " use time: " << end - beg << endl;
    }
    delete msgGroup;
    delete partPool;
    delete centerPool;
}

TEST_F(MessageGroupTest, testHasMsgInRange) {
    int64_t poolSize = 4096LL * 1024 * 1024;
    BlockPool *centerPool = new BlockPool(poolSize, 1024 * 1024);
    BlockPool *partPool = new BlockPool(centerPool, poolSize, 1024 * 1024);

    MessageGroup *msgGroup = new MessageGroup;
    MessageResponse *response = prepareRecycleData(65536);
    unique_ptr<MessageResponse> ptr(response);
    ErrorCode ec = msgGroup->init(_partitionId, partPool, response, TOPIC_MODE_MEMORY_PREFER);
    EXPECT_TRUE(ec == ERROR_NONE);
    ASSERT_TRUE(msgGroup->hasMsgInRange(0, 1, 0, 1));
    ASSERT_TRUE(msgGroup->hasMsgInRange(0, 100, 0, 100));
    ASSERT_FALSE(msgGroup->hasMsgInRange(0, 100, 101, 105));
    ASSERT_FALSE(msgGroup->hasMsgInRange(60000, 65535, 1000, 2000));
    ASSERT_TRUE(msgGroup->hasMsgInRange(60000, 65535, 1000, 65536));
    delete msgGroup;
    delete partPool;
    delete centerPool;
}

TEST_F(MessageGroupTest, testCanReadNotCommittedMsg) {
    MessageGroup msgGroup;
    EXPECT_FALSE(msgGroup.canReadNotCommittedMsg(true, TOPIC_MODE_SECURITY));
    EXPECT_FALSE(msgGroup.canReadNotCommittedMsg(false, TOPIC_MODE_SECURITY));
    EXPECT_TRUE(msgGroup.canReadNotCommittedMsg(true, TOPIC_MODE_MEMORY_PREFER));
    EXPECT_TRUE(msgGroup.canReadNotCommittedMsg(false, TOPIC_MODE_MEMORY_PREFER));
    EXPECT_TRUE(msgGroup.canReadNotCommittedMsg(true, TOPIC_MODE_MEMORY_ONLY));
    EXPECT_TRUE(msgGroup.canReadNotCommittedMsg(false, TOPIC_MODE_MEMORY_ONLY));
    EXPECT_FALSE(msgGroup.canReadNotCommittedMsg(false, TOPIC_MODE_NORMAL));
    EXPECT_TRUE(msgGroup.canReadNotCommittedMsg(true, TOPIC_MODE_NORMAL));
    EXPECT_FALSE(msgGroup.canReadNotCommittedMsg(false, TOPIC_MODE_PERSIST_DATA));
    EXPECT_TRUE(msgGroup.canReadNotCommittedMsg(true, TOPIC_MODE_PERSIST_DATA));
}

} // namespace storage
} // namespace swift
