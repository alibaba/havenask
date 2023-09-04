#include "suez/drc/SwiftSource.h"

#include <map>

#include "suez/drc/SwiftReaderCreator.h"
#include "swift/testlib/MockSwiftReader.h"
#include "unittest/unittest.h"

namespace suez {

class SwiftSourceTest : public TESTBASE {};

TEST_F(SwiftSourceTest, testConfigConvert) {
    std::map<std::string, std::string> params = {
        {"topic_name", "t1"},
        {"swift_root", "zfs://swift_root/path"},
        {"need_field_filter", "true"},
        {"from", "0"},
        {"to", "32767"},
        {"reader_config", "a=b;c=d;"},
    };
    SwiftSourceConfig config;
    ASSERT_TRUE(config.init(params));
    ASSERT_EQ("t1", config.topicName);
    ASSERT_EQ("zfs://swift_root/path", config.swiftRoot);
    ASSERT_TRUE(config.needFieldFilter);
    ASSERT_EQ(0, config.from);
    ASSERT_EQ(32767, config.to);
    ASSERT_EQ("", config.clientConfig);
    ASSERT_EQ("a=b;c=d;", config.readerConfig);
    ASSERT_EQ("topicName=t1;from=0;to=32767;a=b;c=d;", config.constructReadConfigStr());
}

TEST_F(SwiftSourceTest, testTopicStreamMode) {
    std::map<std::string, std::string> params = {
        {"topic_stream_mode", "true"},
        {"topic_list", "t1:1:3|t2:2:-1"},
        {"swift_root", "zfs://swift_root/path"},
        {"need_field_filter", "true"},
        {"from", "0"},
        {"to", "32767"},
        {"reader_config", "a=b;c=d;"},
    };
    SwiftSourceConfig config;
    ASSERT_TRUE(config.init(params));
    ASSERT_EQ("zfs://swift_root/path", config.swiftRoot);
    ASSERT_TRUE(config.needFieldFilter);
    ASSERT_EQ(0, config.from);
    ASSERT_EQ(32767, config.to);
    ASSERT_EQ("", config.clientConfig);
    ASSERT_EQ("a=b;c=d;", config.readerConfig);
    ASSERT_EQ("from=0;to=32767;a=b;c=d;", config.constructReadConfigStr());
}

TEST_F(SwiftSourceTest, testSeekAndRead) {
    auto reader = std::make_unique<swift::testlib::NiceMockSwiftReader>();
    auto mockReader = reader.get();
    SwiftSource source(std::move(reader));
    EXPECT_CALL(*mockReader, seekByTimestamp(100, true)).WillOnce(Return(swift::protocol::ERROR_CLIENT_ARPC_ERROR));
    ASSERT_FALSE(source.seek(100));
    EXPECT_CALL(*mockReader, seekByTimestamp(100, true)).WillOnce(Return(swift::protocol::ERROR_NONE));
    ASSERT_TRUE(source.seek(100));

    swift::protocol::Message msg;
    msg.set_timestamp(101);
    *msg.mutable_data() = "hello, world";
    EXPECT_CALL(*mockReader, read(_, _, _)).WillOnce(DoAll(SetArgReferee<1>(msg), Return(swift::protocol::ERROR_NONE)));
    std::string data;
    int64_t logId;
    ASSERT_TRUE(source.read(data, logId));
    ASSERT_EQ(101, logId);
    ASSERT_EQ("hello, world", data);
}

} // namespace suez
