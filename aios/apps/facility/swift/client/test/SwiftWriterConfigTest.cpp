#include "swift/client/SwiftWriterConfig.h"

#include <iosfwd>
#include <stdint.h>
#include <string>

#include "swift/protocol/Common.pb.h"
#include "unittest/unittest.h"

using namespace std;

namespace swift {
namespace client {

class SwiftWriterConfigTest : public TESTBASE {};

TEST_F(SwiftWriterConfigTest, testParseFromJson) {
    {
        string configStr = "{}";
        SwiftWriterConfig config;
        EXPECT_TRUE(config.parseFromString(configStr));
        EXPECT_EQ(string(""), config.topicName);
        EXPECT_FALSE(config.isSynchronous);
        EXPECT_TRUE(config.safeMode);
        EXPECT_EQ(SwiftWriterConfig::DEFAULT_RETRY_TIMES, config.retryTimes);
        EXPECT_EQ(SwiftWriterConfig::DEFAULT_RETRY_TIME_INTERVAL, config.retryTimeInterval);
        EXPECT_EQ(SwiftWriterConfig::DEFAULT_REQUEST_SEND_BYTE, config.oneRequestSendByteCount);
        EXPECT_EQ(SwiftWriterConfig::DEFAULT_MAX_BUFFER_BYTE, config.maxBufferSize);
        EXPECT_EQ(SwiftWriterConfig::DEFAULT_MAX_KEEP_IN_BUFFER_TIME, config.maxKeepInBufferTime);
        EXPECT_EQ(SwiftWriterConfig::SEND_REQUEST_LOOP_INTERVAL, config.sendRequestLoopInterval);
        EXPECT_EQ(SwiftWriterConfig::DEFAULT_WAIT_LOOP_INTERVAL, config.waitLoopInterval);
        EXPECT_EQ(SwiftWriterConfig::BROKER_BUSY_WAIT_INTERVAL_MIN, config.brokerBusyWaitIntervalMin);
        EXPECT_EQ(SwiftWriterConfig::BROKER_BUSY_WAIT_INTERVAL_MAX, config.brokerBusyWaitIntervalMax);
        EXPECT_EQ(SwiftWriterConfig::DEFAULT_WAIT_FINISHED_WRITER_TIME, config.waitFinishedWriterTime);
        EXPECT_EQ(SwiftWriterConfig::DEFAULT_BROKER_COMMIT_PROGRESS_DETECTION_INTERVAL, config.commitDetectionInterval);
        EXPECT_EQ(string(""), config.functionChain);
        EXPECT_EQ(string(""), config.zkPath);
        EXPECT_FALSE(config.compress);
        EXPECT_FALSE(config.compressMsg);
        EXPECT_FALSE(config.compressMsgInBroker);
        EXPECT_FALSE(config.mergeMsg);
        EXPECT_FALSE(config.needTimestamp);
        EXPECT_EQ(SwiftWriterConfig::DEFAULT_MERGE_THRESHOLD_IN_COUNT, config.mergeThresholdInCount);
        EXPECT_EQ(SwiftWriterConfig::DEFAULT_MERGE_THRESHOLD_IN_SIZE, config.mergeThresholdInSize);
        EXPECT_FALSE(config.mergeMsg);
        EXPECT_EQ(SwiftWriterConfig::DEFAULT_COMPRESS_THRESHOLD_IN_BYTES, config.compressThresholdInBytes);
        bool ret = config.isValidate();
        EXPECT_EQ(uint64_t(3000), config.syncSendTimeout);
        EXPECT_EQ(uint64_t(30000), config.asyncSendTimeout);
        EXPECT_EQ(SwiftWriterConfig::DEFAULT_MESSAGE_FORMAT, config.messageFormat);
        EXPECT_EQ(SwiftWriterConfig::DEFAULT_SCHEMA_VERSION, config.schemaVersion);
        EXPECT_TRUE(config.writerName.empty());
        EXPECT_EQ(0, config.majorVersion);
        EXPECT_EQ(0, config.minorVersion);
        EXPECT_TRUE(!ret);
    }
    {
        string configStr = R"raw(
            {"topicName":"abcd","mode":"sync","retryTimes":10,"retryTimeInterval":1000,
            "oneRequestSendByteCount":10009,"maxKeepInBufferTime":33333,"functionChain":
            "hashId2partId,HASH","sendRequestLoopInterval":44444,"maxBufferSize":22222,
            "brokerBusyWaitIntervalMin":3,"brokerBusyWaitIntervalMax":4,"waitFinishedWriterTime":
            123,"compress":true,"compressMsg":true,"compressMsgInBroker":true,
            "commitDetectionInterval":1233,"asyncSendTimeout":32000,"syncSendTimeout":31000, 
            "messageFormat":0,"zkPath":"abc","waitLoopInterval":12345,"mergeMessage":true,
            "needTimestamp":true,"mergeThresholdInCount":100,"mergeThresholdInSize":50,"schemaVersion":10, 
            "writerIdentity":"a1","writerName":"processor_1_100","majorVersion":100,"minorVersion":200,
            "accessId":"abc", "accessKey":"xxa123"}
             )raw";
        SwiftWriterConfig config;
        EXPECT_TRUE(config.parseFromString(configStr));
        EXPECT_EQ(string("abcd"), config.topicName);
        EXPECT_EQ(string("abc"), config.zkPath);
        EXPECT_TRUE(config.isSynchronous);
        EXPECT_FALSE(config.safeMode);
        EXPECT_EQ(uint32_t(10), config.retryTimes);
        EXPECT_EQ(uint64_t(1000), config.retryTimeInterval);
        EXPECT_EQ(uint64_t(10009), config.oneRequestSendByteCount);
        EXPECT_EQ(uint64_t(22222), config.maxBufferSize);
        EXPECT_EQ(uint64_t(33333), config.maxKeepInBufferTime);
        EXPECT_EQ(uint64_t(44444), config.sendRequestLoopInterval);
        EXPECT_EQ(uint64_t(12345), config.waitLoopInterval);
        EXPECT_EQ(uint64_t(3), config.brokerBusyWaitIntervalMin);
        EXPECT_EQ(uint64_t(4), config.brokerBusyWaitIntervalMax);
        EXPECT_EQ(uint64_t(123), config.waitFinishedWriterTime);
        EXPECT_EQ(uint64_t(1233), config.commitDetectionInterval);
        EXPECT_EQ(string("hashId2partId,HASH"), config.functionChain);
        EXPECT_TRUE(config.compress);
        EXPECT_TRUE(config.compressMsg);
        EXPECT_TRUE(config.compressMsgInBroker);
        EXPECT_TRUE(config.mergeMsg);
        EXPECT_TRUE(config.needTimestamp);
        EXPECT_EQ(100, config.mergeThresholdInCount);
        EXPECT_EQ(50, config.mergeThresholdInSize);
        EXPECT_EQ((uint64_t)0, config.compressThresholdInBytes);
        EXPECT_EQ(uint64_t(31000), config.syncSendTimeout);
        EXPECT_EQ(uint64_t(32000), config.asyncSendTimeout);
        EXPECT_EQ(uint32_t(0), config.messageFormat);
        EXPECT_EQ(uint32_t(10), config.schemaVersion);
        EXPECT_EQ("a1", config.writerIdentity);
        EXPECT_EQ("processor_1_100", config.writerName);
        EXPECT_EQ(100, config.majorVersion);
        EXPECT_EQ(200, config.minorVersion);
        bool ret = config.isValidate();
        EXPECT_TRUE(ret);
    }
}

TEST_F(SwiftWriterConfigTest, testSimpleProcess) {

    string configStr = "topicName=abcd;topicName1=abcd;mode=sync;retryTimes = 10; retryTimeInterval = 1000; "
                       "oneRequestSendByteCount=10009;"
                       "maxKeepInBufferTime=33333;functionChain=hashId2partId,HASH;sendRequestLoopInterval=44444;"
                       "maxBufferSize=22222;brokerBusyWaitIntervalMin=3; brokerBusyWaitIntervalMax=4;zkPath=abc;"
                       "waitFinishedWriterTime=123;commitDetectionInterval=1233;compress=true;compressMsg=true;"
                       "compressMsgInBroker=true;asyncSendTimeout=32000;syncSendTimeout=31000;messageFormat=0;"
                       "waitLoopInterval=12345;mergeMessage=true;mergeThresholdInCount=12345;"
                       "mergeThresholdInSize=50;needTimestamp=true;schemaVersion=10;writerIdentity=a1;"
                       "writerName=processor_1_100;majorVersion=100;minorVersion=200;"
                       "accessId=abc;accessKey=xxa123";

    SwiftWriterConfig config;
    EXPECT_TRUE(config.parseFromString(configStr));
    EXPECT_EQ(string("abcd"), config.topicName);
    EXPECT_TRUE(config.isSynchronous);
    EXPECT_FALSE(config.safeMode);
    EXPECT_EQ(uint32_t(10), config.retryTimes);
    EXPECT_EQ(uint64_t(1000), config.retryTimeInterval);
    EXPECT_EQ(uint64_t(10009), config.oneRequestSendByteCount);
    EXPECT_EQ(uint64_t(22222), config.maxBufferSize);
    EXPECT_EQ(uint64_t(33333), config.maxKeepInBufferTime);
    EXPECT_EQ(uint64_t(44444), config.sendRequestLoopInterval);
    EXPECT_EQ(uint64_t(12345), config.waitLoopInterval);
    EXPECT_EQ(uint64_t(3), config.brokerBusyWaitIntervalMin);
    EXPECT_EQ(uint64_t(4), config.brokerBusyWaitIntervalMax);
    EXPECT_EQ(uint64_t(123), config.waitFinishedWriterTime);
    EXPECT_EQ(uint64_t(1233), config.commitDetectionInterval);
    EXPECT_EQ(string("hashId2partId,HASH"), config.functionChain);
    EXPECT_EQ(string("abc"), config.zkPath);
    EXPECT_TRUE(config.compress);
    EXPECT_TRUE(config.compressMsg);
    EXPECT_TRUE(config.compressMsgInBroker);
    EXPECT_TRUE(config.mergeMsg);
    EXPECT_TRUE(config.needTimestamp);
    EXPECT_EQ(1023, config.mergeThresholdInCount);
    EXPECT_EQ(50, config.mergeThresholdInSize);
    EXPECT_EQ((uint64_t)0, config.compressThresholdInBytes);
    EXPECT_EQ(uint64_t(31000), config.syncSendTimeout);
    EXPECT_EQ(uint64_t(32000), config.asyncSendTimeout);
    EXPECT_EQ(uint32_t(0), config.messageFormat);
    EXPECT_EQ("a1", config.writerIdentity);
    EXPECT_EQ(string("abc"), config.accessId);
    EXPECT_EQ(string("xxa123"), config.accessKey);
    auto authInfo = config.getAccessAuthInfo();
    EXPECT_EQ(string("abc"), authInfo.accessid());
    EXPECT_EQ(string("xxa123"), authInfo.accesskey());

    EXPECT_EQ("processor_1_100", config.writerName);
    EXPECT_EQ(100, config.majorVersion);
    EXPECT_EQ(200, config.minorVersion);

    bool ret = config.isValidate();
    EXPECT_TRUE(ret);
    config.brokerBusyWaitIntervalMin = 5;
    ret = config.isValidate();
    EXPECT_TRUE(!ret);

    configStr = "mode=sync|safe;retryTimes = 10; retryTimeInterval = 1000; oneRequestSendByteCount=10009;"
                "maxKeepInBufferTime=33333;functionChain=hashId2partId,HASH;sendRequestLoopInterval=44444;"
                "maxBufferSize=22222; brokerBusyWaitIntervalMin=3; brokerBusyWaitIntervalMax=4;"
                "waitFinishedWriterTime=123;compress=true;compressMsgInBroker=true;";
    SwiftWriterConfig config2;
    EXPECT_TRUE(config2.parseFromString(configStr));
    EXPECT_EQ(SwiftWriterConfig::DEFAULT_COMPRESS_THRESHOLD_IN_BYTES, config2.compressThresholdInBytes);
    EXPECT_FALSE(config2.safeMode);
    EXPECT_TRUE(config2.isSynchronous);
    EXPECT_FALSE(config2.needTimestamp);

    configStr = "mode=sync;retryTimes = 10; retryTimeInterval = 1000; oneRequestSendByteCount=10009;"
                "maxKeepInBufferTime=33333;functionChain=hashId2partId,HASH;sendRequestLoopInterval=44444;"
                "maxBufferSize=22222; brokerBusyWaitIntervalMin=3; brokerBusyWaitIntervalMax=4;"
                "waitFinishedWriterTime=123;compress=true;compressThresholdInBytes=1024;mode=async|safe";
    SwiftWriterConfig config3;
    EXPECT_TRUE(config3.parseFromString(configStr));
    EXPECT_EQ(uint64_t(1024), config3.compressThresholdInBytes);
    EXPECT_TRUE(config3.safeMode);
    EXPECT_FALSE(config3.isSynchronous);
    EXPECT_EQ(uint32_t(10), config.schemaVersion);
}

TEST_F(SwiftWriterConfigTest, testParseMode) {
    SwiftWriterConfig config;

    EXPECT_TRUE(config.parseMode("sync"));
    EXPECT_TRUE(config.isSynchronous);
    EXPECT_TRUE(!config.safeMode);

    EXPECT_TRUE(config.parseMode("sync|"));
    EXPECT_TRUE(config.isSynchronous);
    EXPECT_TRUE(!config.safeMode);

    EXPECT_TRUE(config.parseMode("sync|safe"));
    EXPECT_TRUE(config.isSynchronous);
    EXPECT_TRUE(!config.safeMode);

    EXPECT_TRUE(config.parseMode("async|safe"));
    EXPECT_TRUE(!config.isSynchronous);
    EXPECT_TRUE(config.safeMode);

    EXPECT_TRUE(config.parseMode("async"));
    EXPECT_TRUE(!config.isSynchronous);
    EXPECT_TRUE(config.safeMode);

    EXPECT_TRUE(config.parseMode("async|unsafe"));
    EXPECT_TRUE(!config.isSynchronous);
    EXPECT_TRUE(!config.safeMode);

    EXPECT_TRUE(config.parseMode("sync|unsafe"));
    EXPECT_TRUE(config.isSynchronous);
    EXPECT_TRUE(!config.safeMode);

    EXPECT_TRUE(!config.parseMode("abc"));
    EXPECT_TRUE(!config.parseMode("safe"));
}

} // namespace client
} // namespace swift
