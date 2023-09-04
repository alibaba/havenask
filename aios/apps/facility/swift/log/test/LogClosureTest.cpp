#include "swift/log/LogClosure.h"

#include <cstddef>
#include <gmock/gmock-function-mocker.h>
#include <google/protobuf/stubs/callback.h>
#include <stdint.h>
#include <string>

#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/util/SwiftUuidGenerator.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace swift::protocol;

namespace swift {
namespace util {

class FakeClosure : public google::protobuf::Closure {
public:
    FakeClosure() { done = false; }
    void Run() { done = true; }

public:
    bool done;
};

class MockLogClosure : public LogClosure<ConsumptionRequest, MessageResponse> {
public:
    MockLogClosure(const ConsumptionRequest *request,
                   MessageResponse *response,
                   google::protobuf::Closure *done,
                   std::string methodName)
        : LogClosure<ConsumptionRequest, MessageResponse>(request, response, done, methodName) {
        uint64_t uuid = request->requestuuid();
        IdData idData;
        idData.id = uuid;
        _forceLog = idData.requestId.logFlag;
    }
    MOCK_METHOD0(doAccessLog, void());
};

class LogClosureTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void LogClosureTest::setUp() {}

void LogClosureTest::tearDown() {}

TEST_F(LogClosureTest, testDone) {
    FakeClosure closure;
    { LogClosure<ConsumptionRequest, MessageResponse> logClosure(NULL, NULL, &closure, "test"); }
    ASSERT_TRUE(closure.done);
    closure.done = false;
    {
        LogClosure<ConsumptionRequest, MessageResponse> logClosure(NULL, NULL, &closure, "test");
        logClosure.setAutoDone(false);
    }
    ASSERT_TRUE(!closure.done);
}

TEST_F(LogClosureTest, testLogInterval) {
    {
        FakeClosure closure;
        ConsumptionRequest request;
        MessageResponse response;
        MockLogClosure logClosure(&request, &response, &closure, "test");
        logClosure.setLogInterval(1);
        EXPECT_CALL(logClosure, doAccessLog()).Times(11);
        for (int i = 0; i < 11; i++) {
            logClosure.doLog();
        }
    }
    {
        FakeClosure closure;
        ConsumptionRequest request;
        MessageResponse response;
        MockLogClosure logClosure(&request, &response, &closure, "test");
        logClosure.setLogInterval(100);
        EXPECT_CALL(logClosure, doAccessLog()).Times(1);
        for (int i = 0; i < 100; i++) {
            logClosure.doLog();
        }
    }
    { // force log
        FakeClosure closure;
        ConsumptionRequest request;
        MessageResponse response;
        IdData idData;
        idData.requestId.logFlag = true;
        request.set_requestuuid(idData.id);
        MockLogClosure logClosure(&request, &response, &closure, "test");
        logClosure.setLogInterval(100);
        EXPECT_CALL(logClosure, doAccessLog()).Times(100);
        for (int i = 0; i < 100; i++) {
            logClosure.doLog();
        }
    }
    { // close force log
        FakeClosure closure;
        ConsumptionRequest request;
        MessageResponse response;
        IdData idData;
        idData.requestId.logFlag = true;

        request.set_requestuuid(idData.id);
        MockLogClosure logClosure(&request, &response, &closure, "test");
        logClosure.setLogInterval(100);
        logClosure.setCloseForceLog(true);
        EXPECT_CALL(logClosure, doAccessLog()).Times(1);
        for (int i = 0; i < 100; i++) {
            logClosure.doLog();
        }
    }
}

}; // namespace util
}; // namespace swift
