#include "swift/client/trace/TraceSwiftLogger.h"

#include <iosfwd>
#include <string>

#include "swift/client/fake_client/FakeSwiftWriter.h"
#include "swift/client/trace/TraceLogger.h"
#include "swift/common/MessageInfo.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace swift {
namespace client {

class TraceSwiftLoggerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void TraceSwiftLoggerTest::setUp() {}

void TraceSwiftLoggerTest::tearDown() {}

TEST_F(TraceSwiftLoggerTest, testSimple) {
    FakeSwiftWriter fakeWriter;
    MessageMeta meta;
    meta.topicName = "test_topic";
    meta.hashVal = 100;
    meta.mask = 1;
    string content = "aaaa";
    {
        TraceSwiftLogger swiftLogger(meta, &fakeWriter);
        ASSERT_TRUE(swiftLogger.write(content));
    }
    common::MessageInfo msgInfo = fakeWriter._lastMsgInfo;
    ASSERT_EQ(meta.mask, msgInfo.uint8Payload);
    ASSERT_EQ(meta.hashVal, msgInfo.uint16Payload);
    ASSERT_EQ(content, msgInfo.data.substr(sizeof(LogHeader)));
}

}; // namespace client
}; // namespace swift
