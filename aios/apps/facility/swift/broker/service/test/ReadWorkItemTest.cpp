#include "swift/broker/service/ReadWorkItem.h"

#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>
#include <iosfwd>
#include <memory>
#include <string>
#include <type_traits>

#include "autil/Log.h"
#include "swift/broker/service/BrokerPartition.h"
#include "swift/broker/service/LongPollingRequestHandler.h"
#include "swift/log/BrokerLogClosure.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/IpMapper.h"
#include "swift/util/ZkDataAccessor.h"
#include "unittest/unittest.h"

namespace swift {
namespace monitor {
struct PartitionMetricsCollector;
} // namespace monitor
} // namespace swift

using namespace std;
using namespace swift::protocol;
using namespace swift::util;

namespace swift {
namespace service {

class MyMockBrokerPartition : public BrokerPartition {
public:
    MyMockBrokerPartition() : BrokerPartition(nullptr, nullptr, nullptr) {}
    MOCK_METHOD3(getMessage,
                 protocol::ErrorCode(const protocol::ConsumptionRequest *request,
                                     protocol::MessageResponse *response,
                                     const std::string *srcIpPort));
    MOCK_METHOD1(collectMetrics, void(monitor::PartitionMetricsCollector &collector));
    MOCK_CONST_METHOD0(isEnableLongPolling, bool());
};

class MyMockLongPollingRequestHandler : public LongPollingRequestHandler {
public:
    MyMockLongPollingRequestHandler() : LongPollingRequestHandler(nullptr, nullptr) {}
    MOCK_METHOD1(addHoldRequest, void(const util::ConsumptionLogClosurePtr &closure));
    MOCK_METHOD3(addLongPollingRequest,
                 void(BrokerPartitionPtr &brokerPartition,
                      const util::ConsumptionLogClosurePtr &closure,
                      ::swift::protocol::LongPollingTracer *tracer));
};

class ReadWorkItemTest : public TESTBASE {
public:
    ReadWorkItemTest();
    ~ReadWorkItemTest();

public:
    void setUp();
    void tearDown();

private:
    ConsumptionRequest _request;
    MessageResponse _response;
    std::shared_ptr<MyMockBrokerPartition> _mockBrokerPartition;
    std::shared_ptr<ReadWorkItem> _workItem;
    std::shared_ptr<MyMockLongPollingRequestHandler> _longPolling;

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(swift, ReadWorkItemTest);

ReadWorkItemTest::ReadWorkItemTest() {}

ReadWorkItemTest::~ReadWorkItemTest() {}

void ReadWorkItemTest::setUp() {
    AUTIL_LOG(DEBUG, "setUp!");
    _mockBrokerPartition = std::make_shared<MyMockBrokerPartition>();
    _longPolling = std::make_shared<MyMockLongPollingRequestHandler>();
    ConsumptionLogClosurePtr closure(new ConsumptionLogClosure(&_request, &_response, nullptr, ""));
    std::shared_ptr<BrokerPartition> brokerPartition;
    _workItem = std::make_shared<ReadWorkItem>(brokerPartition, closure, _longPolling.get(), nullptr);
    _workItem->_brokerPartition = _mockBrokerPartition;
}

void ReadWorkItemTest::tearDown() { AUTIL_LOG(DEBUG, "tearDown!"); }

TEST_F(ReadWorkItemTest, testProcessBrokerBusy) {
    auto mockGetMessage = [](const protocol::ConsumptionRequest *request,
                             protocol::MessageResponse *response,
                             const std::string *srcIpPort) { return ERROR_BROKER_BUSY; };
    EXPECT_CALL(*_mockBrokerPartition, isEnableLongPolling()).WillOnce(Return(false));
    EXPECT_CALL(*_mockBrokerPartition, getMessage(_, _, _)).WillOnce(Invoke(mockGetMessage));
    EXPECT_CALL(*_longPolling, addHoldRequest(_)).Times(1);
    _workItem->process();
}

TEST_F(ReadWorkItemTest, testReadEmptyTopic) {
    auto mockGetMessage = [](const protocol::ConsumptionRequest *request,
                             protocol::MessageResponse *response,
                             const std::string *srcIpPort) { return ERROR_BROKER_NO_DATA; };
    EXPECT_CALL(*_mockBrokerPartition, isEnableLongPolling()).WillOnce(Return(false));
    EXPECT_CALL(*_mockBrokerPartition, getMessage(_, _, _)).WillOnce(Invoke(mockGetMessage));
    EXPECT_CALL(*_longPolling, addHoldRequest(_)).Times(1);
    _workItem->process();
}

TEST_F(ReadWorkItemTest, testReadFinish) {
    auto mockGetMessage = [](const protocol::ConsumptionRequest *request,
                             protocol::MessageResponse *response,
                             const std::string *srcIpPort) {
        response->set_maxmsgid(200);
        response->set_nextmsgid(201);
        response->set_totalmsgcount(0);
        return ERROR_NONE;
    };
    EXPECT_CALL(*_mockBrokerPartition, isEnableLongPolling()).WillOnce(Return(false));
    EXPECT_CALL(*_mockBrokerPartition, getMessage(_, _, _)).WillOnce(Invoke(mockGetMessage));
    EXPECT_CALL(*_longPolling, addHoldRequest(_)).Times(0);
    _workItem->process();
}

TEST_F(ReadWorkItemTest, testProcessReadEmptyByFilter) {
    auto mockGetMessage = [](const protocol::ConsumptionRequest *request,
                             protocol::MessageResponse *response,
                             const std::string *srcIpPort) {
        response->set_maxmsgid(200);
        response->set_nextmsgid(150);
        response->set_totalmsgcount(0);
        return ERROR_NONE;
    };
    EXPECT_CALL(*_mockBrokerPartition, isEnableLongPolling()).WillOnce(Return(false));
    EXPECT_CALL(*_mockBrokerPartition, getMessage(_, _, _)).WillOnce(Invoke(mockGetMessage));
    EXPECT_CALL(*_longPolling, addHoldRequest(_)).Times(0);
    _workItem->process();
}

TEST_F(ReadWorkItemTest, testLongPollingSimple) {
    auto mockGetMessage = [](const protocol::ConsumptionRequest *request,
                             protocol::MessageResponse *response,
                             const std::string *srcIpPort) {
        response->set_maxmsgid(200);
        response->set_nextmsgid(201);
        response->set_totalmsgcount(0);
        return ERROR_NONE;
    };
    EXPECT_CALL(*_mockBrokerPartition, isEnableLongPolling()).WillOnce(Return(true));
    EXPECT_CALL(*_mockBrokerPartition, getMessage(_, _, _)).WillOnce(Invoke(mockGetMessage));
    EXPECT_CALL(*_longPolling, addHoldRequest(_)).Times(0);
    EXPECT_CALL(*_longPolling, addLongPollingRequest(_, _, _)).Times(1);
    _workItem->process();
}

TEST_F(ReadWorkItemTest, testLongPollingEmptyTopic) {
    auto mockGetMessage = [](const protocol::ConsumptionRequest *request,
                             protocol::MessageResponse *response,
                             const std::string *srcIpPort) { return ERROR_BROKER_NO_DATA; };
    EXPECT_CALL(*_mockBrokerPartition, isEnableLongPolling()).WillOnce(Return(true));
    EXPECT_CALL(*_mockBrokerPartition, getMessage(_, _, _)).WillOnce(Invoke(mockGetMessage));
    EXPECT_CALL(*_longPolling, addHoldRequest(_)).Times(0);
    EXPECT_CALL(*_longPolling, addLongPollingRequest(_, _, _)).Times(1);
    _workItem->process();
}

TEST_F(ReadWorkItemTest, testNoNeedLongPolling) {
    auto mockGetMessage = [](const protocol::ConsumptionRequest *request,
                             protocol::MessageResponse *response,
                             const std::string *srcIpPort) {
        response->set_maxmsgid(200);
        response->set_nextmsgid(201);
        response->set_totalmsgcount(1);
        return ERROR_NONE;
    };
    EXPECT_CALL(*_mockBrokerPartition, isEnableLongPolling()).WillOnce(Return(true));
    EXPECT_CALL(*_mockBrokerPartition, getMessage(_, _, _)).WillOnce(Invoke(mockGetMessage));
    EXPECT_CALL(*_mockBrokerPartition, collectMetrics(_)).Times(1);
    EXPECT_CALL(*_longPolling, addHoldRequest(_)).Times(0);
    EXPECT_CALL(*_longPolling, addLongPollingRequest(_, _, _)).Times(0);
    _workItem->process();
}
} // namespace service
} // namespace swift
