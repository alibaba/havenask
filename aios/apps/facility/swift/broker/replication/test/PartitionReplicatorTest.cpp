#include "swift/broker/replication/PartitionReplicator.h"

#include <cstdint>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock-nice-strict.h>
#include <gtest/gtest-matchers.h>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "autil/result/Result.h"
#include "swift/broker/replication/MessageConsumer.h"
#include "swift/broker/replication/MessageProducer.h"
#include "swift/broker/replication/test/Mock.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "unittest/unittest.h"

using namespace testing;

namespace swift {
namespace replication {

class PartitionReplicatorTest : public TESTBASE {
public:
    void doSetup(int64_t selfVersion = 1024) {
        _pid.set_topicname("t1");
        _pid.set_id(0);
        _pid.set_from(0);
        _pid.set_to(65535);

        auto producer = std::make_unique<NiceMockMessageProducer>(selfVersion, true);
        _mockProducer = producer.get();
        _producer = std::move(producer);
        auto consumer = std::make_unique<NiceMockMessageConsumer>();
        _mockConsumer = consumer.get();
        _consumer = std::move(consumer);
    }

protected:
    protocol::PartitionId _pid;
    MockMessageProducer *_mockProducer = nullptr;
    MockMessageConsumer *_mockConsumer = nullptr;
    std::unique_ptr<MessageProducer> _producer;
    std::unique_ptr<MessageConsumer> _consumer;
};

TEST_F(PartitionReplicatorTest, testSimple) {
    doSetup();
    PartitionReplicator replicator(&_pid, std::move(_producer), std::move(_consumer));
    ASSERT_EQ(PartitionReplicator::UNKNOWN, replicator._current);
    ASSERT_FALSE(replicator.canServe());
    ASSERT_FALSE(replicator.canWrite());
}

ACTION_P2(checkRequest, selfVersion, startId, count, readCommittedMsg) {
    ASSERT_EQ(selfVersion, arg0->selfversion());
    ASSERT_EQ(startId, arg0->startid());
    ASSERT_EQ(count, arg0->count());
    ASSERT_EQ(readCommittedMsg, arg0->readcommittedmsg());
}

ACTION_P2(fillResponse, selfVersion, mirrorCommittedId, startId, count) {
    if (selfVersion == 0) {
        arg1->mutable_errorinfo()->set_errcode(protocol::ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND);
        return;
    }
    arg1->set_messageformat(protocol::MF_PB);
    arg1->set_selfversion(selfVersion);
    arg1->set_maxmsgid(mirrorCommittedId);
    for (auto i = 0; i < count; ++i) {
        auto *msg = arg1->add_msgs();
        msg->set_msgid(startId + i);
        msg->set_timestamp(startId + i);
        msg->set_data(std::to_string(startId + i));
    }
}

TEST_F(PartitionReplicatorTest, testStateTrans) {
    doSetup();
    PartitionReplicator replicator(&_pid, std::move(_producer), std::move(_consumer), -1);
    ASSERT_EQ(PartitionReplicator::UNKNOWN, replicator._current);
    EXPECT_CALL(*_mockProducer, produce(_, _))
        .WillOnce(DoAll(checkRequest(1024, 0, 1, true), fillResponse(1000, 4, 0, 1)));
    EXPECT_CALL(*_mockConsumer, doConsume(_)).Times(0);
    replicator.work();
    ASSERT_EQ(PartitionReplicator::LEADER, replicator._current);
    ASSERT_EQ(1000, replicator._mirrorVersion);
    ASSERT_TRUE(replicator._maxCommittedIdOfMirror.has_value());
    ASSERT_EQ(4, replicator._maxCommittedIdOfMirror.value());
    ASSERT_FALSE(replicator.canServe());
    ASSERT_FALSE(replicator.canWrite());

    EXPECT_CALL(*_mockProducer, produce(_, _))
        .WillOnce(DoAll(checkRequest(1024, 0, 1000, true), fillResponse(1000, 9, 0, 5)));
    EXPECT_CALL(*_mockConsumer, doConsume(_)).WillOnce(Return(ByMove(autil::Result<int64_t>{4})));
    replicator.work();
    ASSERT_EQ(PartitionReplicator::LEADER, replicator._current);
    ASSERT_EQ(1000, replicator._mirrorVersion);
    ASSERT_EQ(9, replicator._maxCommittedIdOfMirror.value());
    ASSERT_EQ(4, replicator._committedId);
    ASSERT_FALSE(replicator.canServe());
    ASSERT_FALSE(replicator.canWrite());

    EXPECT_CALL(*_mockProducer, produce(_, _))
        .WillOnce(DoAll(checkRequest(1024, 5, 1000, true), fillResponse(1000, 9, 5, 5)));
    EXPECT_CALL(*_mockConsumer, doConsume(_)).WillOnce(Return(ByMove(autil::Result<int64_t>{9})));
    replicator.work();
    ASSERT_EQ(PartitionReplicator::LEADER, replicator._current);
    ASSERT_EQ(1000, replicator._mirrorVersion);
    ASSERT_EQ(9, replicator._maxCommittedIdOfMirror.value());
    ASSERT_EQ(9, replicator._committedId);
    ASSERT_TRUE(replicator.canServe());
    ASSERT_TRUE(replicator.canWrite());

    EXPECT_CALL(*_mockProducer, produce(_, _))
        .WillOnce(DoAll(checkRequest(1024, 10, 1, true), fillResponse(1000, 9, 10, 0)));
    EXPECT_CALL(*_mockConsumer, doConsume(_)).Times(0);
    replicator.work();
    ASSERT_EQ(PartitionReplicator::LEADER, replicator._current);
    ASSERT_EQ(1000, replicator._mirrorVersion);
    ASSERT_EQ(9, replicator._maxCommittedIdOfMirror.value());
    ASSERT_EQ(9, replicator._committedId);
    ASSERT_TRUE(replicator.canServe());
    ASSERT_TRUE(replicator.canWrite());

    EXPECT_CALL(*_mockProducer, produce(_, _))
        .WillOnce(DoAll(checkRequest(1024, 10, 1, true), fillResponse(1025, 9, 10, 0)));
    EXPECT_CALL(*_mockConsumer, doConsume(_)).Times(0);
    replicator.work();
    ASSERT_EQ(PartitionReplicator::FOLLOWER, replicator._current);
    ASSERT_EQ(1025, replicator._mirrorVersion);
    ASSERT_EQ(9, replicator._maxCommittedIdOfMirror.value());
    ASSERT_EQ(9, replicator._committedId);
    ASSERT_TRUE(replicator.canServe());
    ASSERT_FALSE(replicator.canWrite());

    EXPECT_CALL(*_mockProducer, produce(_, _))
        .WillOnce(DoAll(checkRequest(1024, 10, 1000, true), fillResponse(1000, 9, 10, 0)));
    EXPECT_CALL(*_mockConsumer, doConsume(_)).WillOnce(Return(ByMove(autil::Result<int64_t>{9})));
    replicator.work();
    ASSERT_EQ(PartitionReplicator::LEADER, replicator._current);
    ASSERT_EQ(1000, replicator._mirrorVersion);
    ASSERT_EQ(9, replicator._maxCommittedIdOfMirror.value());
    ASSERT_EQ(9, replicator._committedId);
    ASSERT_TRUE(replicator.canServe());
    ASSERT_TRUE(replicator.canWrite());
}

} // namespace replication
} // namespace swift
