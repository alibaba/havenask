#include "swift/broker/service/RequestChecker.h"

#include <iosfwd>
#include <limits>
#include <stdint.h>

#include "autil/Log.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "unittest/unittest.h"

using namespace std;
namespace swift {
namespace service {

class RequestCheckerTest : public TESTBASE {
public:
    RequestCheckerTest();
    ~RequestCheckerTest();

public:
    void setUp();
    void tearDown();

protected:
    void
    innerTestSendMessage(bool hasTopicName, bool hasPartitionId, uint32_t payload, uint32_t maskPayload, bool result);
    void innerTestGetMessage(bool hasTopicName,
                             bool hasPartitionId,
                             bool hasStartId,
                             uint32_t rangeFrom,
                             uint32_t rangeTo,
                             uint32_t filterMask,
                             uint32_t maskResult,
                             bool result);

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(swift, RequestCheckerTest);

RequestCheckerTest::RequestCheckerTest() {}

RequestCheckerTest::~RequestCheckerTest() {}

void RequestCheckerTest::setUp() { AUTIL_LOG(DEBUG, "setUp!"); }

void RequestCheckerTest::tearDown() { AUTIL_LOG(DEBUG, "tearDown!"); }

TEST_F(RequestCheckerTest, testValidPayload) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    RequestChecker requestChecker;

    ASSERT_TRUE(requestChecker.isValidPayload(1000));
    ASSERT_TRUE(requestChecker.isValidPayload(numeric_limits<uint16_t>::min()));
    ASSERT_TRUE(requestChecker.isValidPayload(numeric_limits<uint16_t>::max()));
    ASSERT_TRUE(!requestChecker.isValidPayload(1000000));
}

TEST_F(RequestCheckerTest, testValidMaskPayload) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    RequestChecker requestChecker;

    ASSERT_TRUE(requestChecker.isValidMaskPayload(10));
    ASSERT_TRUE(requestChecker.isValidMaskPayload(numeric_limits<uint8_t>::min()));
    ASSERT_TRUE(requestChecker.isValidMaskPayload(numeric_limits<uint8_t>::max()));
    ASSERT_TRUE(!requestChecker.isValidMaskPayload(1000000));
}

TEST_F(RequestCheckerTest, testSendMessage) {
    innerTestSendMessage(true, true, 100, 100, true);
    innerTestSendMessage(false, true, 100, 100, false);
    innerTestSendMessage(true, false, 100, 100, false);
    innerTestSendMessage(true, true, 1000000, 100, false);
    innerTestSendMessage(true, true, 100, 1000000, false);
}

TEST_F(RequestCheckerTest, testGetMessage) {
    innerTestGetMessage(true, true, true, 0, 100, 10, 10, true);
    innerTestGetMessage(true, true, true, 3333, 0, 10, 10, true);
    innerTestGetMessage(false, true, true, 100, 100, 10, 10, false);
    innerTestGetMessage(true, false, true, 100, 100, 10, 10, false);
    innerTestGetMessage(true, true, false, 100, 100, 10, 10, false);
    innerTestGetMessage(true, false, false, 100, 100, 10, 10, false);
    innerTestGetMessage(false, false, false, 100, 100, 10, 10, false);
    innerTestGetMessage(true, true, true, 0, 65536, 10, 10, false);
    innerTestGetMessage(true, true, true, 65536, 0, 10, 10, false);
    innerTestGetMessage(true, true, true, 0, 100, 1000, 10, false);
    innerTestGetMessage(true, true, true, 0, 100, 10, 1000, false);
}

void RequestCheckerTest::innerTestSendMessage(
    bool hasTopicName, bool hasPartitionId, uint32_t payload, uint32_t maskPayload, bool result) {
    RequestChecker requestChecker;
    protocol::ProductionRequest request;
    if (hasTopicName) {
        request.set_topicname("topic_name");
    }
    if (hasPartitionId) {
        request.set_partitionid(0);
    }
    protocol::Message *msg = request.add_msgs();
    msg->set_uint16payload(payload);
    msg->set_uint8maskpayload(maskPayload);

    ASSERT_TRUE(result == requestChecker.sendMessage(&request));
}

void RequestCheckerTest::innerTestGetMessage(bool hasTopicName,
                                             bool hasPartitionId,
                                             bool hasStartId,
                                             uint32_t rangeFrom,
                                             uint32_t rangeTo,
                                             uint32_t filterMask,
                                             uint32_t maskResult,
                                             bool result) {
    RequestChecker requestChecker;
    protocol::ConsumptionRequest request;
    if (hasTopicName) {
        request.set_topicname("topic_name");
    }
    if (hasPartitionId) {
        request.set_partitionid(0);
    }
    if (hasStartId) {
        request.set_startid(0);
    }
    request.mutable_filter()->set_from(rangeFrom);
    request.mutable_filter()->set_to(rangeTo);
    request.mutable_filter()->set_uint8filtermask(filterMask);
    request.mutable_filter()->set_uint8maskresult(maskResult);

    ASSERT_TRUE(result == requestChecker.getMessage(&request));
}

} // namespace service
} // namespace swift
