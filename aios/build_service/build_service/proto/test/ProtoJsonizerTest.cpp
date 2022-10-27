#include "build_service/test/unittest.h"
#include "build_service/proto/ProtoJsonizer.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/test/TestJsonize.pb.h"
using namespace std;
using namespace testing;

namespace build_service {
namespace proto {

class ProtoJsonizerTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    void checkTestSimpleMessage(const TestSimpleMessage &message);
    void checkTestRepeatedMessage(const TestRepeatedMessage &message);
};

void ProtoJsonizerTest::setUp() {
}

void ProtoJsonizerTest::tearDown() {
}

TEST_F(ProtoJsonizerTest, testFromJsonString) {
    string jsonStr = "{"
                     "\"boolValue\":true,"
                     "\"int32Value\":-10,"
                     "\"uint32Value\":10,"
                     "\"int64Value\":-22222,"
                     "\"uint64Value\":22222,"
                     "\"floatValue\":22222.22,"
                     "\"doubleValue\":-22222.22,"
                     "\"stringValue\":\"string_value\","
                     "\"bytesValue\":\"bytes_value\","
                     "\"enumValue\":\"ENUM_VALUE0\""
                     "}";
    TestSimpleMessage message;
    ASSERT_TRUE(ProtoJsonizer::fromJsonString(jsonStr, &message));
    checkTestSimpleMessage(message);
}

TEST_F(ProtoJsonizerTest, testFromJsonStringWithRepeatedItem) {
    string jsonStr = "{"
                     "\"boolValue\":[true, false, true],"
                     "\"int32Value\":[-10],"
                     "\"uint32Value\":[10],"
                     "\"int64Value\":[-22222, -11111],"
                     "\"uint64Value\":[22222, 11111],"
                     "\"floatValue\":[22222.22, 11111.11],"
                     "\"doubleValue\":[-22222.22, -11111.11],"
                     "\"stringValue\":[\"string_value\", \"string_value2\"],"
                     "\"bytesValue\":[\"bytes_value\", \"bytes_value2\"],"
                     "\"enumValue\":[\"ENUM_VALUE0\", \"ENUM_VALUE1\"]"
                     "}";
    TestRepeatedMessage message;
    ASSERT_TRUE(ProtoJsonizer::fromJsonString(jsonStr, &message));
    checkTestRepeatedMessage(message);
}

TEST_F(ProtoJsonizerTest, testFromJsonWithMessage) {
    string jsonStr = "{"
                     "\"testMessageMember1\": {"
                     "\"simpleMessages\" : {"
                     "\"boolValue\":true,"
                     "\"int32Value\":-10,"
                     "\"uint32Value\":10,"
                     "\"int64Value\":-22222,"
                     "\"uint64Value\":22222,"
                     "\"floatValue\":22222.22,"
                     "\"doubleValue\":-22222.22,"
                     "\"stringValue\":\"string_value\","
                     "\"bytesValue\":\"bytes_value\","
                     "\"enumValue\":\"ENUM_VALUE0\""
                     "}"
                     "}"
                     "}";
    TestMessageMember2 rootMessage;
    ASSERT_TRUE(ProtoJsonizer::fromJsonString(jsonStr, &rootMessage));
    const TestSimpleMessage &message = rootMessage.testmessagemember1().simplemessages();
    checkTestSimpleMessage(message);
}

TEST_F(ProtoJsonizerTest, testFromJsonWithRepeatedMessage) {
    string jsonStr = "{"
                     "\"repeatedMessages\": ["
                     "{"
                     "\"boolValue\":[true, false, true],"
                     "\"int32Value\":[-10],"
                     "\"uint32Value\":[10],"
                     "\"int64Value\":[-22222, -11111],"
                     "\"uint64Value\":[22222, 11111],"
                     "\"floatValue\":[22222.22, 11111.11],"
                     "\"doubleValue\":[-22222.22, -11111.11],"
                     "\"stringValue\":[\"string_value\", \"string_value2\"],"
                     "\"bytesValue\":[\"bytes_value\", \"bytes_value2\"],"
                     "\"enumValue\":[\"ENUM_VALUE0\", \"ENUM_VALUE1\"]"
                     "}"
                     "]"
                     "}";
    TestRepeatedMessageMember rootMessage;
    ASSERT_TRUE(ProtoJsonizer::fromJsonString(jsonStr, &rootMessage));
    const TestRepeatedMessage &message = rootMessage.repeatedmessages(0);
    checkTestRepeatedMessage(message);
}

TEST_F(ProtoJsonizerTest, testFromJsonInvalidString) {
    string jsonStr = "{"
                     "\"repeatedMessages\": ["
                     "{"
                     "\"boolValue\":[true, false, true],"
                     "}";
    TestRepeatedMessageMember rootMessage;
    ASSERT_TRUE(!ProtoJsonizer::fromJsonString(jsonStr, &rootMessage));    
}

TEST_F(ProtoJsonizerTest, testFromJsonInvalidType) {
    string jsonStr = "{"
                     "\"boolValue\":\"true\","
                     "}";
    TestSimpleMessage message;
    ASSERT_TRUE(!ProtoJsonizer::fromJsonString(jsonStr, &message));
}

void ProtoJsonizerTest::checkTestSimpleMessage(const TestSimpleMessage &message) {
    EXPECT_EQ(true, message.boolvalue());
    EXPECT_EQ(int32_t(-10), message.int32value());
    EXPECT_EQ(uint32_t(10), message.uint32value());
    EXPECT_EQ(int64_t(-22222), message.int64value());
    EXPECT_EQ(uint64_t(22222), message.uint64value());
    EXPECT_EQ(float(22222.22), message.floatvalue());
    EXPECT_EQ(double(-22222.22), message.doublevalue());
    EXPECT_EQ(string("string_value"), message.stringvalue());
    EXPECT_EQ(string("bytes_value"), message.bytesvalue());
    EXPECT_EQ(ENUM_VALUE0, message.enumvalue());
}

void ProtoJsonizerTest::checkTestRepeatedMessage(const TestRepeatedMessage &message) {
    EXPECT_EQ(int(3), message.boolvalue_size());
    EXPECT_EQ(true, message.boolvalue(0));
    EXPECT_EQ(false, message.boolvalue(1));
    EXPECT_EQ(true, message.boolvalue(2));

    EXPECT_EQ(int(1), message.int32value_size());
    EXPECT_EQ(int32_t(-10), message.int32value(0));

    EXPECT_EQ(int(1), message.uint32value_size());
    EXPECT_EQ(uint32_t(10), message.uint32value(0));

    EXPECT_EQ(int(2), message.int64value_size());
    EXPECT_EQ(int64_t(-22222), message.int64value(0));
    EXPECT_EQ(int64_t(-11111), message.int64value(1));

    EXPECT_EQ(int(2), message.uint64value_size());
    EXPECT_EQ(uint64_t(22222), message.uint64value(0));
    EXPECT_EQ(uint64_t(11111), message.uint64value(1));

    EXPECT_EQ(int(2), message.floatvalue_size());
    EXPECT_EQ(float(22222.22), message.floatvalue(0));
    EXPECT_EQ(float(11111.11), message.floatvalue(1));

    EXPECT_EQ(int(2), message.doublevalue_size());
    EXPECT_EQ(double(-22222.22), message.doublevalue(0));
    EXPECT_EQ(double(-11111.11), message.doublevalue(1));

    EXPECT_EQ(int(2), message.stringvalue_size());
    EXPECT_EQ(string("string_value"), message.stringvalue(0));
    EXPECT_EQ(string("string_value2"), message.stringvalue(1));

    EXPECT_EQ(int(2), message.bytesvalue_size());
    EXPECT_EQ(string("bytes_value"), message.bytesvalue(0));
    EXPECT_EQ(string("bytes_value2"), message.bytesvalue(1));

    EXPECT_EQ(int(2), message.enumvalue_size());
    EXPECT_EQ(ENUM_VALUE0, message.enumvalue(0));
    EXPECT_EQ(ENUM_VALUE1, message.enumvalue(1));
}


}
}
