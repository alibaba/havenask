#include "swift/client/SwiftClientConfig.h"

#include <iosfwd>
#include <stdint.h>
#include <string>

#include "unittest/unittest.h"

using namespace std;
using namespace testing;
namespace swift {
namespace client {

class SwiftClientConfigTest : public TESTBASE {};

TEST_F(SwiftClientConfigTest, testFromJson) {
    string configStr = R"raw(
        {"zkPath":"abc","retryTime":22,"retryInterval":33,"logConfigFile":"file",
         "useFollowerAdmin":false,"tracingMsgLevel":1,"maxWriteBufferSizeMb":123,"bufferBlockSizeKb":124,
        "mergeMessageThreadNum":22,"tempWriteBufferPercent":0.23,"refreshTime":122,"ioThreadNum":3,
        "rpcTimeout":13,"rpcSendQueueSize":23, "fbWriterRecycleSizeKb":100, "clientIdentity":"test", 
        "latelyErrorTimeMaxIntervalUs":1000, "latelyErrorTimeMinIntervalUs":100, "username":"username1",
        "passwd":"passwd1"}
)raw";
    SwiftClientConfig config;
    ASSERT_TRUE(config.parseFromString(configStr));
    EXPECT_EQ(uint32_t(22), config.retryTimes);
    EXPECT_EQ(int64_t(33), config.retryTimeInterval);
    EXPECT_EQ(string("abc"), config.zkPath);
    EXPECT_EQ(string("file"), config.logConfigFile);
    EXPECT_EQ(string("test"), config.clientIdentity);
    EXPECT_EQ(int64_t(123), config.maxWriteBufferSizeMb);
    EXPECT_EQ(int64_t(124), config.bufferBlockSizeKb);
    EXPECT_EQ(int64_t(122), config.refreshTime);
    EXPECT_EQ(int64_t(100), config.fbWriterRecycleSizeKb);
    EXPECT_EQ(uint32_t(22), config.mergeMessageThreadNum);
    EXPECT_EQ(uint32_t(3), config.ioThreadNum);
    EXPECT_EQ(uint32_t(13), config.rpcTimeout);
    EXPECT_EQ(uint32_t(23), config.rpcSendQueueSize);
    EXPECT_DOUBLE_EQ(double(0.23), config.tempWriteBufferPercent);
    ASSERT_TRUE(!config.useFollowerAdmin);
    ASSERT_EQ(1, config.tracingMsgLevel);
    ASSERT_EQ(1000, config.latelyErrorTimeMaxIntervalUs);
    ASSERT_EQ(100, config.latelyErrorTimeMinIntervalUs);
    EXPECT_EQ(string("username1"), config.username);
    EXPECT_EQ(string("passwd1"), config.passwd);
}

TEST_F(SwiftClientConfigTest, testSimpleProcess) {
    string configStr =
        "zkPath=abc;retryTime=22;retryInterval=33;logConfigFile=file;useFollowerAdmin=false;maxWriteBufferSizeMb=123;"
        "bufferBlockSizeKb=124;mergeMessageThreadNum=22;tempWriteBufferPercent=0.23;refreshTime=122;ioThreadNum=3;"
        "rpcTimeout=13;rpcSendQueueSize=23;fbWriterRecycleSizeKb=100;tracingMsgLevel=1;clientIdentity=test;"
        "latelyErrorTimeMinIntervalUs=100;latelyErrorTimeMaxIntervalUs=1000;username=test_user1;passwd=passwd1_test";
    SwiftClientConfig config;
    ASSERT_TRUE(config.parseFromString(configStr));
    EXPECT_EQ(uint32_t(22), config.retryTimes);
    EXPECT_EQ(int64_t(33), config.retryTimeInterval);
    EXPECT_EQ(string("abc"), config.zkPath);
    EXPECT_EQ(string("file"), config.logConfigFile);
    EXPECT_EQ(string("test"), config.clientIdentity);
    EXPECT_EQ(int64_t(123), config.maxWriteBufferSizeMb);
    EXPECT_EQ(int64_t(124), config.bufferBlockSizeKb);
    EXPECT_EQ(int64_t(122), config.refreshTime);
    EXPECT_EQ(int64_t(100), config.fbWriterRecycleSizeKb);
    EXPECT_EQ(uint32_t(22), config.mergeMessageThreadNum);
    EXPECT_EQ(uint32_t(3), config.ioThreadNum);
    EXPECT_EQ(uint32_t(13), config.rpcTimeout);
    EXPECT_EQ(uint32_t(23), config.rpcSendQueueSize);
    EXPECT_DOUBLE_EQ(double(0.23), config.tempWriteBufferPercent);
    ASSERT_TRUE(!config.useFollowerAdmin);
    ASSERT_EQ(1, config.tracingMsgLevel);
    ASSERT_EQ(1000, config.latelyErrorTimeMaxIntervalUs);
    ASSERT_EQ(100, config.latelyErrorTimeMinIntervalUs);
    ASSERT_EQ("test_user1", config.username);
    ASSERT_EQ("passwd1_test", config.passwd);
}

TEST_F(SwiftClientConfigTest, testReaderConfigDefaultValue) {
    string configStr = "";
    SwiftClientConfig config;
    EXPECT_TRUE(config.zkPath.empty());
    ASSERT_FALSE(config.parseFromString(configStr));
    configStr = "zkPath=abc";
    ASSERT_TRUE(config.parseFromString(configStr));
    EXPECT_EQ(SwiftClientConfig::DEFAULT_TRY_TIMES, config.retryTimes);
    EXPECT_EQ(SwiftClientConfig::DEFAULT_RETRY_TIME_INTERVAL, config.retryTimeInterval);
    EXPECT_EQ(string("abc"), config.zkPath);
    EXPECT_EQ(string(""), config.clientIdentity);
    EXPECT_TRUE(config.logConfigFile.empty());
    EXPECT_EQ(SwiftClientConfig::DEFAULT_MAX_WRITE_BUFFER_SIZE_MB, config.maxWriteBufferSizeMb);
    EXPECT_EQ(SwiftClientConfig::DEFAULT_BUFFER_BLOCK_SIZE_KB, config.bufferBlockSizeKb);
    EXPECT_EQ(SwiftClientConfig::DEFAULT_MERGE_MESSAGE_THREAD_NUM, config.mergeMessageThreadNum);
    EXPECT_DOUBLE_EQ(SwiftClientConfig::DEFAULT_TEMP_WRITE_BUFFER_PERCENT, config.tempWriteBufferPercent);
    EXPECT_EQ(SwiftClientConfig::DEFAULT_ADMIN_REFRESH_TIME, config.refreshTime);
    EXPECT_EQ(SwiftClientConfig::DEFAULT_IO_THREAD_NUM, config.ioThreadNum);
    EXPECT_EQ(SwiftClientConfig::DEFAULT_RPC_TIMEOUT, config.rpcTimeout);
    EXPECT_EQ(SwiftClientConfig::DEFAULT_RPC_SEND_QUEUE_SIZE, config.rpcSendQueueSize);
    EXPECT_EQ(SwiftClientConfig::DEFAULT_FB_WRITER_RECYCLE_SIZE_KB, config.fbWriterRecycleSizeKb);
    EXPECT_EQ(SwiftClientConfig::DEFAULT_LATELY_ERROR_TIME_MIN_INTERVAL_US, config.latelyErrorTimeMinIntervalUs);
    EXPECT_EQ(SwiftClientConfig::DEFAULT_LATELY_ERROR_TIME_MAX_INTERVAL_US, config.latelyErrorTimeMaxIntervalUs);
    ASSERT_EQ(0, config.tracingMsgLevel);
    ASSERT_TRUE(config.useFollowerAdmin);
    ASSERT_EQ("", config.username);
    ASSERT_EQ("", config.passwd);
}

TEST_F(SwiftClientConfigTest, testReaderConfigDefaultValueJson) {
    string configStr = R"({"zkPath":"abc"})";
    SwiftClientConfig config;
    ASSERT_TRUE(config.parseFromString(configStr));
    EXPECT_EQ(SwiftClientConfig::DEFAULT_TRY_TIMES, config.retryTimes);
    EXPECT_EQ(SwiftClientConfig::DEFAULT_RETRY_TIME_INTERVAL, config.retryTimeInterval);
    EXPECT_EQ(string("abc"), config.zkPath);
    EXPECT_EQ(string(""), config.clientIdentity);
    EXPECT_EQ(SwiftClientConfig::DEFAULT_MAX_WRITE_BUFFER_SIZE_MB, config.maxWriteBufferSizeMb);
    EXPECT_EQ(SwiftClientConfig::DEFAULT_BUFFER_BLOCK_SIZE_KB, config.bufferBlockSizeKb);
    EXPECT_TRUE(config.logConfigFile.empty());
    EXPECT_EQ(SwiftClientConfig::DEFAULT_MERGE_MESSAGE_THREAD_NUM, config.mergeMessageThreadNum);
    EXPECT_EQ(SwiftClientConfig::DEFAULT_IO_THREAD_NUM, config.ioThreadNum);
    EXPECT_EQ(SwiftClientConfig::DEFAULT_RPC_TIMEOUT, config.rpcTimeout);
    EXPECT_EQ(SwiftClientConfig::DEFAULT_RPC_SEND_QUEUE_SIZE, config.rpcSendQueueSize);
    EXPECT_DOUBLE_EQ(SwiftClientConfig::DEFAULT_TEMP_WRITE_BUFFER_PERCENT, config.tempWriteBufferPercent);
    EXPECT_EQ(SwiftClientConfig::DEFAULT_FB_WRITER_RECYCLE_SIZE_KB, config.fbWriterRecycleSizeKb);
    ASSERT_TRUE(config.useFollowerAdmin);
    ASSERT_EQ(0, config.tracingMsgLevel);
    ASSERT_EQ("", config.username);
    ASSERT_EQ("", config.passwd);
}

} // namespace client
} // namespace swift
