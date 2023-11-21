#include "swift/client/trace/ReadMessageTracer.h"

#include <iosfwd>
#include <memory>
#include <string>

#include "swift/client/fake_client/FakeSwiftWriter.h"
#include "swift/client/trace/TraceLogger.h"
#include "swift/client/trace/TraceSwiftLogger.h"
#include "swift/common/MessageInfo.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/TraceMessage.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace swift {
namespace client {

class ReadMessageTracerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void ReadMessageTracerTest::setUp() {}

void ReadMessageTracerTest::tearDown() {}

TEST_F(ReadMessageTracerTest, testSimple) {
    FakeSwiftWriter fakeWriter;
    MessageMeta meta;
    meta.hashVal = 100;
    meta.mask = 1;
    TraceSwiftLoggerPtr swiftLogger(new TraceSwiftLogger(meta, &fakeWriter));
    protocol::ReaderInfo readerInfo;
    readerInfo.set_topicname("test");
    protocol::Message msg;
    msg.set_data("test_data");
    msg.set_msgid(12345);
    msg.set_timestamp(2000000000);
    msg.set_requestuuid(1);
    msg.set_uint16payload(100);
    msg.set_uint8maskpayload(2);
    {
        ReadMessageTracer tracer({swiftLogger});
        tracer.setReaderInfo(readerInfo);
        tracer.tracingMsg(msg);
    }
    common::MessageInfo msgInfo = fakeWriter._lastMsgInfo;
    ASSERT_EQ(meta.mask, msgInfo.uint8Payload);
    ASSERT_EQ(meta.hashVal, msgInfo.uint16Payload);
    ASSERT_TRUE(msgInfo.data.size() >= sizeof(LogHeader));
    TraceMsgContainer container;
    ASSERT_TRUE(container.fromString(msgInfo.data.substr(sizeof(LogHeader))));

    ASSERT_EQ(1, container.writeMessage.tracemessages_size());
    ASSERT_EQ(readerInfo.topicname(), container.writeMessage.readerinfo().topicname());
    const auto &traceMessage = container.writeMessage.tracemessages(0);
    ASSERT_EQ(12345, traceMessage.msgid());
    ASSERT_EQ(1, traceMessage.requestuuid());
    ASSERT_EQ(2000, traceMessage.timestampinsecond());
    ASSERT_EQ(100, traceMessage.uint16payload());
    ASSERT_EQ(2, traceMessage.uint8maskpayload());
}

}; // namespace client
}; // namespace swift
