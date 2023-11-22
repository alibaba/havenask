#include "swift/client/SwiftReaderConfig.h"

#include <cstddef>
#include <stdint.h>
#include <string>
#include <vector>

#include "swift/protocol/Common.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "unittest/unittest.h"

using namespace std;

namespace swift {
namespace client {

class SwiftReaderConfigTest : public TESTBASE {};

TEST_F(SwiftReaderConfigTest, testFromJson) {
    string configStr = R"raw(
                       {"topicName":"abcd","fatalErrorTimeLimit":111,"fatalErrorReportInterval":222,
                       "consumptionRequestBytesLimit":444,"oneRequestReadCount":333,"readBufferSize":666,
                       "retryGetMsgInterval":8888,"from":10,"to":20,"uint8FilterMask":30,
                       "uint8MaskResult":40,"compress":true,"partitionStatusRefreshInterval":100,"decompress":false,
                       "partitions":[1,2,3],"fieldFilterDesc":"fieldA IN a|b|c","requiredFields":["ab","cd"],
                       "messageFormat":0,"batchReadCount":50,"zkPath":"abc","readAll":true,
                       "mergeMessage":22, "readerIdentity":"reader", "multiReaderOrder":"seq",
                       "checkpointMode":"refresh", "checkpointRefreshTimestampOffset":10000,
                       "accessId":"abc", "accessKey":"xxa123"}
                       )raw";
    SwiftReaderConfig config;
    EXPECT_TRUE(config.parseFromString(configStr));
    EXPECT_EQ(string("abcd"), config.topicName);
    EXPECT_EQ(uint64_t(111), config.fatalErrorTimeLimit);
    EXPECT_EQ(uint64_t(222), config.fatalErrorReportInterval);
    EXPECT_EQ(uint64_t(444), config.consumptionRequestBytesLimit);
    EXPECT_EQ(uint64_t(333), config.oneRequestReadCount);
    EXPECT_EQ(uint64_t(666), config.readBufferSize);
    EXPECT_EQ(uint64_t(8888), config.retryGetMsgInterval);
    EXPECT_EQ(uint32_t(10), config.swiftFilter.from());
    EXPECT_EQ(uint32_t(20), config.swiftFilter.to());
    EXPECT_TRUE(config.readAll);
    EXPECT_EQ(uint32_t(30), config.swiftFilter.uint8filtermask());
    EXPECT_EQ(uint32_t(40), config.swiftFilter.uint8maskresult());
    EXPECT_EQ(true, config.needCompress);
    EXPECT_EQ(false, config.canDecompress);
    EXPECT_EQ(uint64_t(100), config.partitionStatusRefreshInterval);
    EXPECT_EQ(size_t(3), config.readPartVec.size());
    EXPECT_EQ(uint32_t(1), config.readPartVec[0]);
    EXPECT_EQ(uint32_t(2), config.readPartVec[1]);
    EXPECT_EQ(uint32_t(3), config.readPartVec[2]);
    EXPECT_EQ(size_t(2), config.requiredFields.size());
    EXPECT_EQ(string("ab"), config.requiredFields[0]);
    EXPECT_EQ(string("cd"), config.requiredFields[1]);
    EXPECT_EQ(string("fieldA IN a|b|c"), config.fieldFilterDesc);
    EXPECT_EQ(uint32_t(0), config.messageFormat);
    EXPECT_EQ(uint32_t(50), config.batchReadCount);
    EXPECT_EQ(string("abc"), config.zkPath);
    EXPECT_EQ(string("reader"), config.readerIdentity);
    EXPECT_EQ(string("abc"), config.accessId);
    EXPECT_EQ(string("xxa123"), config.accessKey);
    auto authInfo = config.getAccessAuthInfo();
    EXPECT_EQ(string("abc"), authInfo.accessid());
    EXPECT_EQ(string("xxa123"), authInfo.accesskey());
    EXPECT_EQ(uint32_t(22), config.mergeMessage);
    EXPECT_EQ("seq", config.multiReaderOrder);
    EXPECT_EQ(READER_CHECKPOINT_REFRESH_MODE, config.checkpointMode);
    EXPECT_EQ(10000, config.checkpointRefreshTimestampOffset);
}

TEST_F(SwiftReaderConfigTest, testSimpleProcess) {
    string configStr = "topicName=abcd;fatalErrorTimeLimit= 111;fatalErrorReportInterval=222;"
                       "consumptionRequestBytesLimit=444;oneRequestReadCount=333;"
                       "readBufferSize=666;retryGetMsgInterval=8888;from=10;to=20;"
                       "uint8FilterMask=30;uint8MaskResult=40;compress=true;"
                       "partitionStatusRefreshInterval=100;decompress=false;partitions=1,2,3;"
                       "requiredFields=ab,cd;fieldFilterDesc=fieldA IN a|b|c;messageFormat=0;"
                       "batchReadCount=50;zkPath=abc;readAll=true;mergeMessage=22;"
                       "readerIdentity=reader;multiReaderOrder=sequence;"
                       "checkpointMode=refresh;checkpointRefreshTimestampOffset=10000;"
                       "accessId=abc;accessKey=xxa123";
    SwiftReaderConfig config;
    EXPECT_TRUE(config.parseFromString(configStr));
    EXPECT_EQ(string("abcd"), config.topicName);
    EXPECT_EQ(uint64_t(111), config.fatalErrorTimeLimit);
    EXPECT_EQ(uint64_t(222), config.fatalErrorReportInterval);
    EXPECT_EQ(uint64_t(444), config.consumptionRequestBytesLimit);
    EXPECT_EQ(uint64_t(333), config.oneRequestReadCount);
    EXPECT_EQ(uint64_t(666), config.readBufferSize);
    EXPECT_EQ(uint64_t(8888), config.retryGetMsgInterval);
    EXPECT_EQ(uint32_t(10), config.swiftFilter.from());
    EXPECT_EQ(uint32_t(20), config.swiftFilter.to());
    EXPECT_TRUE(config.readAll);
    EXPECT_EQ(uint32_t(30), config.swiftFilter.uint8filtermask());
    EXPECT_EQ(uint32_t(40), config.swiftFilter.uint8maskresult());
    EXPECT_EQ(true, config.needCompress);
    EXPECT_EQ(false, config.canDecompress);
    EXPECT_EQ(uint64_t(100), config.partitionStatusRefreshInterval);
    EXPECT_EQ(size_t(3), config.readPartVec.size());
    EXPECT_EQ(uint32_t(1), config.readPartVec[0]);
    EXPECT_EQ(uint32_t(2), config.readPartVec[1]);
    EXPECT_EQ(uint32_t(3), config.readPartVec[2]);
    EXPECT_EQ(size_t(2), config.requiredFields.size());
    EXPECT_EQ(string("ab"), config.requiredFields[0]);
    EXPECT_EQ(string("cd"), config.requiredFields[1]);
    EXPECT_EQ(string("fieldA IN a|b|c"), config.fieldFilterDesc);
    EXPECT_EQ(uint32_t(0), config.messageFormat);
    EXPECT_EQ(uint32_t(50), config.batchReadCount);
    EXPECT_EQ(string("abc"), config.zkPath);
    EXPECT_EQ(uint32_t(22), config.mergeMessage);
    EXPECT_EQ(string("reader"), config.readerIdentity);
    EXPECT_EQ(string("abc"), config.accessId);
    EXPECT_EQ(string("xxa123"), config.accessKey);
    EXPECT_EQ("sequence", config.multiReaderOrder);
    EXPECT_EQ(READER_CHECKPOINT_REFRESH_MODE, config.checkpointMode);
    EXPECT_EQ(10000, config.checkpointRefreshTimestampOffset);
}

TEST_F(SwiftReaderConfigTest, testReaderConfigDefaultValue) {
    string configStr = "";
    SwiftReaderConfig config;
    EXPECT_TRUE(config.parseFromString(configStr));

    EXPECT_EQ(SwiftReaderConfig::DEFAULT_FATAL_ERROR_TIME_LIMIT, config.fatalErrorTimeLimit);
    EXPECT_EQ(SwiftReaderConfig::DEFAULT_FATAL_ERROR_REPORT_INTERVAL, config.fatalErrorReportInterval);
    EXPECT_EQ(SwiftReaderConfig::DEFAULT_CONSUMPTIONREQUEST_BYTES_LIMIT, config.consumptionRequestBytesLimit);
    EXPECT_EQ(SwiftReaderConfig::ONE_REQUEST_READ_COUNT, config.oneRequestReadCount);
    EXPECT_EQ(SwiftReaderConfig::READ_BUFFER_SIZE, config.readBufferSize);
    EXPECT_EQ(SwiftReaderConfig::RETRY_GET_MESSAGE_INTERVAL, config.retryGetMsgInterval);
    EXPECT_EQ(uint32_t(0), config.swiftFilter.from());
    EXPECT_EQ(uint32_t(65535), config.swiftFilter.to());
    EXPECT_TRUE(!config.readAll);
    EXPECT_EQ(uint32_t(0), config.swiftFilter.uint8filtermask());
    EXPECT_EQ(uint32_t(0), config.swiftFilter.uint8maskresult());
    EXPECT_EQ(false, config.needCompress);
    EXPECT_EQ(true, config.canDecompress);
    EXPECT_EQ(SwiftReaderConfig::PARTITION_STATUS_REFRESH_INTERVAL, config.partitionStatusRefreshInterval);
    EXPECT_EQ(size_t(0), config.requiredFields.size());
    EXPECT_EQ(string(""), config.fieldFilterDesc);
    EXPECT_EQ(SwiftReaderConfig::DEFAULT_MESSAGE_FORMAT, config.messageFormat);
    EXPECT_EQ(uint32_t(256), config.batchReadCount);
    EXPECT_EQ(uint32_t(1), config.mergeMessage);
    EXPECT_TRUE(config.multiReaderOrder.empty());
    EXPECT_TRUE(config.checkpointMode.empty());
    EXPECT_EQ(SwiftReaderConfig::DEFAULT_CHECKPOINT_REFRESH_TIMESTAMP_OFFSET, config.checkpointRefreshTimestampOffset);
}

} // namespace client
} // namespace swift
