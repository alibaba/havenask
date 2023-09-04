#include "swift/broker/service/LongPollingRequestHandler.h"

#include <algorithm>
#include <cstdint>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>
#include <iosfwd>
#include <list>
#include <memory>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

#include "autil/TimeUtility.h"
#include "swift/broker/service/BrokerPartition.h"
#include "swift/broker/service/TopicPartitionSupervisor.h"
#include "swift/broker/service/test/FakeClosure.h"
#include "swift/config/BrokerConfig.h"
#include "swift/log/BrokerLogClosure.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/util/Block.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace swift::util;
using namespace swift::config;
using namespace autil;

namespace swift {
namespace service {

class MockTopicPartitionSupervisor : public TopicPartitionSupervisor {
public:
    MockTopicPartitionSupervisor() : TopicPartitionSupervisor(nullptr, nullptr, nullptr) {}
    MOCK_METHOD1(getAllPartitions, void(std::vector<protocol::PartitionId> &));
    // MOCK_METHOD0(getAllPartitions, std::vector<protocol::PartitionId>());
    MOCK_METHOD1(getBrokerPartition, BrokerPartitionPtr(const protocol::PartitionId &));
};

class MockBrokerPartition : public BrokerPartition {
public:
    MockBrokerPartition() : BrokerPartition(nullptr, nullptr) {}
    MOCK_METHOD((protocol::PartitionStatus), getPartitionStatus, (), (const, override));
    MOCK_METHOD((protocol::PartitionId), getPartitionId, (), (const, override));
    MOCK_METHOD4(stealTimeoutPolling, size_t(bool, int64_t, int64_t, ReadRequestQueue &));
};

class LongPollingRequestHandlerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void LongPollingRequestHandlerTest::setUp() {}

void LongPollingRequestHandlerTest::tearDown() {}

TEST_F(LongPollingRequestHandlerTest, testCheckTimeout) {
    protocol::ConsumptionRequest request;
    protocol::MessageResponse response;
    FakeClosure done;
    ConsumptionLogClosurePtr logClosure(new ConsumptionLogClosure(&request, &response, &done, "test"));
    BrokerPartition::ReadRequestQueue mockTimeoutReqs;
    size_t timeout = 2000000;
    mockTimeoutReqs.push_back(std::make_pair(TimeUtility::currentTime() + timeout, logClosure));

    auto mockBrokerPartition = std::make_shared<MockBrokerPartition>();
    EXPECT_CALL(*mockBrokerPartition, getPartitionStatus())
        .Times(1)
        .WillOnce(Return(protocol::PARTITION_STATUS_RUNNING));
    EXPECT_CALL(*mockBrokerPartition, getPartitionId()).WillRepeatedly(Return(protocol::PartitionId()));
    EXPECT_CALL(*mockBrokerPartition, stealTimeoutPolling(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgReferee<3>(mockTimeoutReqs), Return(0)));

    MockTopicPartitionSupervisor tp;
    std::vector<protocol::PartitionId> mockPartitionIds;
    mockPartitionIds.resize(1);
    EXPECT_CALL(tp, getAllPartitions(_))
        .WillOnce(SetArgReferee<0>(mockPartitionIds))
        .WillRepeatedly(SetArgReferee<0>(decltype(mockPartitionIds)()));
    EXPECT_CALL(tp, getBrokerPartition(_)).WillRepeatedly(Return(mockBrokerPartition));

    BrokerConfig config;
    config._requestTimeout = timeout;             // 2 sec
    config._noDataRequestNotfiyIntervalMs = 1000; //  1 sec
    LongPollingRequestHandler handler(nullptr, &tp);
    handler.init(&config);
    usleep(2 * 1000 * 1000);
}

TEST_F(LongPollingRequestHandlerTest, testTimeBack) {
    LongPollingRequestHandler handler(nullptr, nullptr);
    auto timeInfo = handler.currentTime();
    ASSERT_FALSE(timeInfo.first);

    handler._lastTime = handler._lastTime + 2000000;
    auto oldLastTime = handler._lastTime;
    auto timeInfo2 = handler.currentTime();
    ASSERT_TRUE(timeInfo2.first);

    ASSERT_EQ(handler._lastTime, oldLastTime + 1);
}

}; // namespace service
}; // namespace swift
