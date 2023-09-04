#include <cstddef>
#include <stdint.h>
#include <string>

#include "autil/ShortString.h"
#include "autil/StringUtil.h"
#include "flatbuffers/flatbuffers.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/FBMessageWriter.h"
#include "swift/protocol/SwiftMessage_generated.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;

namespace swift {
namespace protocol {

class FBMessageTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void FBMessageTest::setUp() {}

void FBMessageTest::tearDown() {}

TEST_F(FBMessageTest, testWriterSize) {
    FBMessageWriter writer;
    writer.finish();
    ASSERT_EQ(24, writer.size());
    ASSERT_EQ(256 * 1024, writer._fbAllocator->getAllocatedSize());
}

TEST_F(FBMessageTest, testReadAndWrite) {
    FBMessageWriter writer;
    uint32_t msgCount = 1000;
    for (uint32_t i = 0; i < msgCount; i++) {
        writer.addMessage(StringUtil::toString(i), i + 1, i + 2, 3, 4, true);
    }
    ASSERT_FALSE(writer.isFinish());
    FBMessageReader reader;
    ASSERT_TRUE(reader.init(writer.data(), writer.size()));
    ASSERT_TRUE(writer.isFinish());
    ASSERT_EQ(msgCount, reader.size());
    for (uint32_t i = 0; i < msgCount; i++) {
        const swift::protocol::flat::Message *msg = reader.read(i);
        ASSERT_EQ(StringUtil::toString(i), string(msg->data()->c_str(), msg->data()->size()));
        ASSERT_EQ(i + 1, msg->msgId());
        ASSERT_EQ(i + 2, msg->timestamp());
        ASSERT_EQ(3, msg->uint16Payload());
        ASSERT_EQ(4, msg->uint8MaskPayload());
        ASSERT_TRUE(msg->compress());
    }
    ASSERT_TRUE(reader.read(msgCount) == NULL);
}

TEST_F(FBMessageTest, testWriteReset) {
    FBMessageWriter writer;
    ASSERT_EQ(0, writer._fbAllocator->getAllocatedSize());
    uint32_t msgCount = 1000000;
    for (uint32_t i = 0; i < msgCount; i++) {
        writer.addMessage(StringUtil::toString(i), i + 1, i + 2, 3, 4, true);
    }
    ASSERT_FALSE(writer.isFinish());
    ASSERT_TRUE((1 << 21) < writer._fbAllocator->getAllocatedSize());
    writer.finish();
    ASSERT_TRUE(writer.isFinish());
    ASSERT_TRUE((1 << 21) < writer._fbAllocator->getAllocatedSize());
    writer.reset();
    ASSERT_EQ(0, writer._fbAllocator->getAllocatedSize());
}

TEST_F(FBMessageTest, testInitFail) {
    FBMessageReader reader;
    ASSERT_FALSE(reader.init(string()));
}

TEST_F(FBMessageTest, testReadAndWriteEmptyMsg) {
    FBMessageWriter writer;
    uint32_t msgCount = 1000;
    string empty;
    for (uint32_t i = 0; i < msgCount; i++) {
        writer.addMessage(empty, i + 1, i + 2, 3, 4, true);
    }
    ASSERT_FALSE(writer.isFinish());
    FBMessageReader reader;
    ASSERT_TRUE(reader.init(writer.data(), writer.size()));
    ASSERT_TRUE(writer.isFinish());
    ASSERT_EQ(msgCount, reader.size());
    for (uint32_t i = 0; i < msgCount; i++) {
        const swift::protocol::flat::Message *msg = reader.read(i);
        ASSERT_EQ(empty, string(msg->data()->c_str(), msg->data()->size()));
        ASSERT_EQ(i + 1, msg->msgId());
        ASSERT_EQ(i + 2, msg->timestamp());
        ASSERT_EQ(3, msg->uint16Payload());
        ASSERT_EQ(4, msg->uint8MaskPayload());
        ASSERT_TRUE(msg->compress());
    }
    ASSERT_TRUE(reader.read(msgCount) == NULL);
}

}; // namespace protocol
}; // namespace swift
