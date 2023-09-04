#include "swift/config/BrokerConfig.h"

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>

#include "linux/sysinfo.h"
#include "swift/config/AuthorizerInfo.h"
#include "swift/config/BrokerConfigParser.h"
#include "swift/config/ConfigDefine.h"
#include "swift/config/ConfigReader.h"
#include "swift/config/PartitionConfig.h"
#include "sys/sysinfo.h"
#include "unittest/unittest.h"

using namespace std;
namespace swift {
namespace config {

class BrokerConfigParserTest : public TESTBASE {
protected:
    size_t getPhyMemSize();
};

TEST_F(BrokerConfigParserTest, testSimpleProcess) {
    BrokerConfigParser configParser;
    std::unique_ptr<BrokerConfig> brokerConfig(configParser.parse(GET_TEST_DATA_PATH() + "config/swift_norm.conf"));
    EXPECT_TRUE(brokerConfig.get());

    EXPECT_EQ(std::string("pangu://localcluster:10240/home"), brokerConfig->dfsRoot);
    EXPECT_EQ(std::string("pangu://localcluster:10240/ip"), brokerConfig->ipMapFile);
    EXPECT_EQ(int(2), brokerConfig->getThreadNum());
    EXPECT_EQ(int(3), brokerConfig->getReportMetricThreadNum());
    EXPECT_EQ(uint32_t(30), brokerConfig->getLogSampleCount());
    EXPECT_EQ(size_t(1024), brokerConfig->getMaxGetMessageSizeKb());
    EXPECT_EQ(size_t(1000), brokerConfig->getHoldNoDataRequestTimeMs());
    EXPECT_EQ(size_t(200), brokerConfig->getNoDataRequestNotfiyIntervalMs());

    EXPECT_FALSE(brokerConfig->getCloseForceLog());
    EXPECT_EQ(int(2), brokerConfig->maxReadThreadNum);
    EXPECT_EQ(int(2), brokerConfig->maxWriteThreadNum);
    // it's in AdminConfig
    // EXPECT_EQ(int(1000), brokerConfig->queueSize);

    EXPECT_EQ(int64_t(512 * 1024 * 1024), brokerConfig->getPartitionMinBufferSize());
    EXPECT_EQ(int64_t(1024 * 1024 * 1024), brokerConfig->getPartitionMaxBufferSize());
    EXPECT_EQ(int64_t(2 * 1024 * 1024), brokerConfig->getBufferBlockSize());
    EXPECT_DOUBLE_EQ(double(0.2), brokerConfig->fileBufferUseRatio);
    EXPECT_EQ(int64_t(1000 * 1024 * 1024), brokerConfig->getTotalBufferSize());
    EXPECT_EQ(int64_t(100 * 1024 * 1024), brokerConfig->getBrokerFileBufferSize());
    EXPECT_EQ(int64_t(100 * 1024 * 1024), brokerConfig->getBrokerFileMetaBufferSize());
    EXPECT_EQ(int64_t(800 * 1024 * 1024), brokerConfig->getBrokerMessageBufferSize());
    EXPECT_EQ(int64_t(11 * 1024 * 1024), brokerConfig->getBufferMinReserveSize());

    EXPECT_EQ(int64_t(32 * 1024 * 1024), brokerConfig->getDfsCommitBlockSize());
    EXPECT_EQ(int(10), brokerConfig->dfsCommitInterval);
    EXPECT_EQ(int(110), brokerConfig->dfsCommitIntervalForMemoryPreferTopic);
    EXPECT_EQ(int(120), brokerConfig->dfsCommitIntervalWhenDelay);
    EXPECT_EQ(int(5), brokerConfig->dfsCommitThreadNum);
    EXPECT_EQ(int(100), brokerConfig->dfsCommitQueueSize);
    EXPECT_EQ(int(6), brokerConfig->loadThreadNum);
    EXPECT_EQ(int(101), brokerConfig->loadQueueSize);
    EXPECT_EQ(int(7), brokerConfig->unloadThreadNum);
    EXPECT_EQ(int(102), brokerConfig->unloadQueueSize);
    EXPECT_EQ(int(3), brokerConfig->httpThreadNum);
    EXPECT_EQ(int(4), brokerConfig->httpQueueSize);

    EXPECT_EQ(int64_t(1024 * 1024 * 1024), brokerConfig->getDfsFileSize());
    EXPECT_EQ(int64_t(700 * 1024 * 1024), brokerConfig->getDfsFileMinSize());
    EXPECT_EQ(int64_t(1800), brokerConfig->dfsFileSplitTime);
    EXPECT_EQ(int64_t(1700), brokerConfig->closeNoWriteFileTime);

    EXPECT_EQ((int64_t)3600 * 1000 * 1000, brokerConfig->getObsoleteFileTimeInterval());
    EXPECT_EQ((int32_t)3, brokerConfig->reservedFileCount);
    EXPECT_EQ((int64_t)100 * 1000 * 1000, brokerConfig->getDelObsoleteFileInterval());
    EXPECT_FALSE(brokerConfig->randomDelObsoleteFileInterval);
    EXPECT_EQ((int64_t)50 * 1000 * 1000, brokerConfig->getCandidateObsoleteFileInterval());
    EXPECT_EQ((int64_t)5 * 1000 * 1000, brokerConfig->getRequestTimeout());
    EXPECT_EQ(int64_t(1024), brokerConfig->maxMessageCountInOneFile);
    EXPECT_EQ(uint32_t(3), brokerConfig->cacheMetaCount);
    EXPECT_EQ(int64_t(1000), brokerConfig->maxPermissionWaitTime);
    EXPECT_EQ(uint32_t(10), brokerConfig->concurrentReadFileLimit);
    EXPECT_EQ(int32_t(20), brokerConfig->leaderLeaseTime);
    EXPECT_EQ(int32_t(5), brokerConfig->leaderLoopInterval);
    EXPECT_DOUBLE_EQ(101, brokerConfig->maxCommitTimeAsError);
    EXPECT_DOUBLE_EQ(23, brokerConfig->minChangeFileForDfsErrorTime);
    EXPECT_EQ(int32_t(5), brokerConfig->leaderLoopInterval);
    EXPECT_DOUBLE_EQ(0.05, brokerConfig->recyclePercent);
    EXPECT_FALSE(brokerConfig->supportMergeMsg);
    EXPECT_FALSE(brokerConfig->supportFb);
    EXPECT_FALSE(brokerConfig->checkFieldFilterMsg);
    EXPECT_EQ(10, brokerConfig->getOneFileFdNum());
    EXPECT_EQ(20, brokerConfig->getCacheFileReserveTime());
    EXPECT_EQ(11, brokerConfig->getCacheBlockReserveTime());
    EXPECT_EQ(3000000, brokerConfig->getObsoleteReaderInterval());
    EXPECT_EQ(5000000, brokerConfig->getObsoleteReaderMetricInterval());
    EXPECT_EQ(100, brokerConfig->getCommitThreadLoopIntervalMs());
    EXPECT_EQ(10, brokerConfig->readQueueSize);
    EXPECT_EQ(20, brokerConfig->statusReportIntervalSec);
    EXPECT_EQ(100, brokerConfig->getMaxReadSizeSec());
    EXPECT_TRUE(brokerConfig->enableFastRecover);
    EXPECT_FALSE(brokerConfig->useRecommendPort);
    EXPECT_EQ("zfs://xxx-aa-bb", brokerConfig->mirrorZkRoot);
    EXPECT_EQ(true, brokerConfig->authConf._enable);
    std::map<std::string, std::string> kvMap1 = {{"user1", "passwd1"}, {"user2", "passwd2"}};
    std::map<std::string, std::string> kvMap2 = {{"user3", "passwd3"}, {"user4", "passwd4"}};
    ASSERT_EQ(kvMap1, brokerConfig->authConf._sysUsers);
    ASSERT_EQ(kvMap2, brokerConfig->authConf._normalUsers);
    EXPECT_FALSE(brokerConfig->readNotCommittedMsg);
    EXPECT_EQ(1000, brokerConfig->getTimestampOffsetInUs());
    EXPECT_EQ(10000000, brokerConfig->maxTopicAclSyncIntervalUs);

    PartitionConfig partitionConfig;
    brokerConfig->getDefaultPartitionConfig(partitionConfig);
    EXPECT_EQ(int64_t(512 * 1024 * 1024), partitionConfig.getPartitionMinBufferSize());
    EXPECT_EQ(int64_t(1024 * 1024 * 1024), partitionConfig.getPartitionMaxBufferSize());
    EXPECT_EQ(int64_t(2 * 1024 * 1024), partitionConfig.getBlockSize());

    EXPECT_EQ(int64_t(1024), partitionConfig.getMaxMessageCountInOneFile());

    EXPECT_EQ(std::string("pangu://localcluster:10240/home"), partitionConfig.getDataRoot());

    EXPECT_EQ(int64_t(32 * 1024 * 1024), partitionConfig.getMaxCommitSize());
    EXPECT_EQ(int64_t(10000000), partitionConfig.getMaxCommitInterval());
    EXPECT_EQ(int64_t(110000000), partitionConfig.getMaxCommitIntervalForMemoryPreferTopic());
    EXPECT_EQ(std::max(int64_t(MIN_MAX_FILE_SIZE), int64_t(1024 * 1024 * 1024ll)), partitionConfig.getMaxFileSize());
    EXPECT_EQ(int64_t(700 * 1024 * 1024ll), partitionConfig.getMinFileSize());
    EXPECT_EQ(int64_t(1800 * 1000000ll), partitionConfig.getMaxFileSplitInterval());
    EXPECT_EQ(int64_t(1700 * 1000000ll), partitionConfig.getCloseNoWriteFileInterval());

    EXPECT_EQ((int64_t)3600 * 1000 * 1000, partitionConfig.getObsoleteFileTimeInterval());
    EXPECT_EQ((int32_t)5, partitionConfig.getReservedFileCount());
    EXPECT_EQ((int64_t)100 * 1000 * 1000, partitionConfig.getDelObsoleteFileInterval());
    EXPECT_EQ((int64_t)50 * 1000 * 1000, partitionConfig.getCandidateObsoleteFileInterval());
    EXPECT_EQ(uint32_t(3), partitionConfig.getCacheMetaCount());
    EXPECT_EQ(int64_t(1000000), partitionConfig.getMaxCommitTimeAsError());
    EXPECT_EQ(int64_t(60000000), partitionConfig.getMinChangeFileForDfsErrorTime());
    EXPECT_DOUBLE_EQ(0.05, partitionConfig.getRecyclePercent());
    EXPECT_FALSE(partitionConfig.checkFieldFilterMsg());
    EXPECT_EQ(3000000, partitionConfig.getObsoleteReaderInterval());
    EXPECT_EQ(5000000, partitionConfig.getObsoleteReaderMetricInterval());
    EXPECT_EQ(104857600, partitionConfig.getMaxReadSizeSec());
    EXPECT_FALSE(partitionConfig.readNotCommittedMsg());
    EXPECT_EQ(1000, partitionConfig.getTimestampOffsetInUs());
}

TEST_F(BrokerConfigParserTest, testPartitionConfigUnlimited) {
    BrokerConfigParser configParser;
    std::unique_ptr<BrokerConfig> brokerConfig(
        configParser.parse(GET_TEST_DATA_PATH() + "config/swift.unlimited.conf"));
    EXPECT_TRUE(brokerConfig.get());

    EXPECT_EQ(int64_t(0.06 * 1024 * 1024), brokerConfig->getPartitionMinBufferSize());
    EXPECT_EQ(int64_t(0.07 * 1024 * 1024), brokerConfig->getPartitionMaxBufferSize());

    EXPECT_EQ(int64_t(0.03 * 1024 * 1024), brokerConfig->getDfsCommitBlockSize());
    EXPECT_EQ(int64_t(0.1 * 1024 * 1024), brokerConfig->getDfsFileSize());

    EXPECT_EQ((int64_t)10 * 1000 * 1000, brokerConfig->getObsoleteFileTimeInterval());
    EXPECT_EQ((int32_t)2, brokerConfig->reservedFileCount);
    EXPECT_EQ((int64_t)300 * 1000 * 1000, brokerConfig->getDelObsoleteFileInterval());
    EXPECT_TRUE(brokerConfig->randomDelObsoleteFileInterval);
    EXPECT_EQ((int64_t)3600 * 1000 * 1000, brokerConfig->getCandidateObsoleteFileInterval());
    EXPECT_EQ((int64_t)(getPhyMemSize() * 0.1), brokerConfig->getTotalBufferSize());
    EXPECT_EQ(MAX_BUFFER_BLOCK_SIZE, brokerConfig->getBufferBlockSize());

    EXPECT_EQ(int64_t(1), brokerConfig->closeNoWriteFileTime);
    EXPECT_EQ(60000000, brokerConfig->getObsoleteReaderInterval());
    EXPECT_EQ(60000000, brokerConfig->getObsoleteReaderMetricInterval());
    EXPECT_EQ(128, brokerConfig->getMaxReadSizeSec());
    EXPECT_TRUE(brokerConfig->mirrorZkRoot.empty());

    PartitionConfig partitionConfig;
    brokerConfig->getDefaultPartitionConfig(partitionConfig);
    EXPECT_EQ(int64_t(0.06 * 1024 * 1024), partitionConfig.getPartitionMinBufferSize());
    EXPECT_EQ(int64_t(0.07 * 1024 * 1024), partitionConfig.getPartitionMaxBufferSize());

    EXPECT_EQ(int64_t(MAX_BUFFER_BLOCK_SIZE), partitionConfig.getBlockSize());

    EXPECT_EQ(int64_t(0.03 * 1024 * 1024), partitionConfig.getMaxCommitSize());
    EXPECT_EQ(int64_t(0.1 * 1024 * 1024), partitionConfig.getMaxFileSize());
    EXPECT_EQ((int64_t)10 * 1000 * 1000, partitionConfig.getObsoleteFileTimeInterval());
    EXPECT_EQ((int32_t)2, partitionConfig.getReservedFileCount());
    EXPECT_EQ((int64_t)300 * 1000 * 1000, partitionConfig.getDelObsoleteFileInterval());
    EXPECT_EQ((int64_t)3600 * 1000 * 1000, partitionConfig.getCandidateObsoleteFileInterval());
    EXPECT_EQ(int64_t(1 * 1000000ll), partitionConfig.getCloseNoWriteFileInterval());
    EXPECT_EQ(60000000, partitionConfig.getObsoleteReaderInterval());
    EXPECT_EQ(60000000, partitionConfig.getObsoleteReaderMetricInterval());
    EXPECT_EQ(134217728, partitionConfig.getMaxReadSizeSec());
}

TEST_F(BrokerConfigParserTest, testNecessaryOptions) {
    ConfigReader reader;
    BrokerConfigParser configParser;

    reader._sec2KeyValMap[SECTION_BROKER][DFS_ROOT_PATH] = "/x/x/x/x/";
    std::unique_ptr<BrokerConfig> brokerConfig(configParser.parse(reader));
    EXPECT_TRUE(!brokerConfig.get());

    reader._sec2KeyValMap[SECTION_COMMON][USER_NAME] = "swift";
    reader._sec2KeyValMap[SECTION_COMMON][SERVICE_NAME] = "123";
    reader._sec2KeyValMap[SECTION_COMMON][ZOOKEEPER_ROOT_PATH] = "zfs://";
    reader._sec2KeyValMap[SECTION_BROKER][MAX_READ_THREAD_NUM] = "12";
    reader._sec2KeyValMap[SECTION_BROKER][MAX_WRITE_THREAD_NUM] = "13";
    reader._sec2KeyValMap[SECTION_BROKER][PARTITION_MIN_BUFFER_SIZE_MB] = "128";
    reader._sec2KeyValMap[SECTION_BROKER][PARTITION_MAX_BUFFER_SIZE_MB] = "1024";
    brokerConfig.reset(configParser.parse(reader));
    EXPECT_TRUE(brokerConfig.get());

    EXPECT_EQ(std::string("/x/x/x/x/"), brokerConfig->dfsRoot);
    EXPECT_EQ(int(16), brokerConfig->getThreadNum());
    EXPECT_EQ(int(12), brokerConfig->maxReadThreadNum);
    EXPECT_EQ(int(13), brokerConfig->maxWriteThreadNum);
    EXPECT_EQ(int(200), brokerConfig->queueSize);
    EXPECT_EQ(int64_t(128 * 1024 * 1024), brokerConfig->getPartitionMinBufferSize());
    EXPECT_EQ(int64_t(1024 * 1024 * 1024), brokerConfig->getPartitionMaxBufferSize());

    EXPECT_EQ(int64_t(DEFAULT_DFS_COMMIT_BLOCK_SIZE), brokerConfig->getDfsCommitBlockSize());
    EXPECT_DOUBLE_EQ(DEFAULT_DFS_COMMIT_INTERVAL, brokerConfig->dfsCommitInterval);
    EXPECT_EQ(int64_t(config::DEFAULT_DFS_FILE_SIZE), brokerConfig->getDfsFileSize());
    EXPECT_EQ(int64_t(1800), brokerConfig->dfsFileSplitTime);
    EXPECT_EQ(config::DEFAULT_OBSOLETE_FILE_TIME_INTERVAL, brokerConfig->getObsoleteFileTimeInterval());
    EXPECT_EQ(config::DEFAULT_RESERVED_FILE_COUNT, brokerConfig->reservedFileCount);
}

TEST_F(BrokerConfigParserTest, testWithDefaultValue) {
    BrokerConfigParser configParser;
    std::unique_ptr<BrokerConfig> brokerConfig(
        configParser.parse(GET_TEST_DATA_PATH() + "config/swift_default_value.conf"));
    EXPECT_TRUE(brokerConfig.get());
    EXPECT_EQ(int(0), brokerConfig->getReportMetricThreadNum());
    EXPECT_EQ(DEFAULT_LOG_SAMPLE_COUNT, brokerConfig->getLogSampleCount());
    ASSERT_TRUE(brokerConfig->getCloseForceLog());
    EXPECT_EQ(size_t(0), brokerConfig->getMaxGetMessageSizeKb());
    EXPECT_EQ(size_t(0), brokerConfig->getHoldNoDataRequestTimeMs());
    EXPECT_EQ(DEFAULT_NO_DATA_REQUEST_NOTFIY_INTERVAL_MS, brokerConfig->getNoDataRequestNotfiyIntervalMs());
    EXPECT_EQ(int64_t(DEFAULT_PARTITION_MIN_BUFFER_SIZE), brokerConfig->getPartitionMinBufferSize());
    EXPECT_EQ(int64_t(DEFAULT_PARTITION_MAX_BUFFER_SIZE), brokerConfig->getPartitionMaxBufferSize());

    EXPECT_EQ(int64_t(DEFAULT_BUFFER_BLOCK_SIZE), brokerConfig->getBufferBlockSize());
    EXPECT_DOUBLE_EQ(DEFAULT_FILE_BUFFER_USE_RATIO, brokerConfig->fileBufferUseRatio);

    EXPECT_EQ(int64_t(DEFAULT_DFS_COMMIT_BLOCK_SIZE), brokerConfig->getDfsCommitBlockSize());
    EXPECT_EQ(int(DEFAULT_DFS_COMMIT_INTERVAL), brokerConfig->dfsCommitInterval);
    EXPECT_EQ(int(DEFAULT_DFS_COMMIT_INTERVAL_FOR_MEMORY_PREFER_TOPIC),
              brokerConfig->dfsCommitIntervalForMemoryPreferTopic);
    EXPECT_EQ(int(DEFAULT_DFS_COMMIT_INTERVAL_WHEN_DELAY), brokerConfig->dfsCommitIntervalWhenDelay);
    EXPECT_EQ(int(DEFAULT_DFS_COMMIT_THREAD_NUM), brokerConfig->dfsCommitThreadNum);
    EXPECT_EQ(int(DEFAULT_DFS_COMMIT_QUEUE_SIZE), brokerConfig->dfsCommitQueueSize);
    EXPECT_EQ(int64_t(DEFAULT_DFS_FILE_SIZE), brokerConfig->getDfsFileSize());
    EXPECT_EQ(int64_t(DEFAULT_DFS_FILE_MIN_SIZE), brokerConfig->getDfsFileMinSize());

    EXPECT_EQ(int64_t(DEFAULT_DFS_FILE_SPLIT_TIME), brokerConfig->dfsFileSplitTime);
    EXPECT_EQ(int64_t(DEFAULT_CLOSE_NO_WRITE_FILE_TIME), brokerConfig->closeNoWriteFileTime);
    EXPECT_EQ(int64_t(DEFAULT_OBSOLETE_FILE_TIME_INTERVAL), brokerConfig->getObsoleteFileTimeInterval());
    EXPECT_EQ(int32_t(DEFAULT_RESERVED_FILE_COUNT), brokerConfig->reservedFileCount);
    EXPECT_EQ(int64_t(DEFAULT_DEL_OBSOLETE_FILE_INTERVAL), brokerConfig->getDelObsoleteFileInterval());
    EXPECT_TRUE(brokerConfig->randomDelObsoleteFileInterval);
    EXPECT_EQ(int64_t(DEFAULT_CANDIDATE_OBSOLETE_FILE_INTERVAL), brokerConfig->getCandidateObsoleteFileInterval());
    EXPECT_EQ(int64_t(DEFAULT_REQUEST_TIMEOUT), brokerConfig->getRequestTimeout());
    EXPECT_EQ(int64_t(1024 * 1024), brokerConfig->maxMessageCountInOneFile);
    EXPECT_EQ(uint32_t(5), brokerConfig->cacheMetaCount);
    EXPECT_EQ(int64_t(10 * 1000 * 1000), brokerConfig->maxPermissionWaitTime);
    EXPECT_EQ(uint32_t(5), brokerConfig->concurrentReadFileLimit);
    EXPECT_EQ(int32_t(DEFAULT_LEADER_LEASE_TIME), brokerConfig->leaderLeaseTime);
    EXPECT_EQ(int32_t(DEFAULT_LEADER_LOOP_INTERVAL), brokerConfig->leaderLoopInterval);

    EXPECT_EQ(int(DEFAULT_LOAD_THREAD_NUM), brokerConfig->loadThreadNum);
    EXPECT_EQ(int(DEFAULT_LOAD_QUEUE_SIZE), brokerConfig->loadQueueSize);
    EXPECT_EQ(int(DEFAULT_UNLOAD_THREAD_NUM), brokerConfig->unloadThreadNum);
    EXPECT_EQ(int(DEFAULT_UNLOAD_QUEUE_SIZE), brokerConfig->unloadQueueSize);
    EXPECT_EQ(int(1), brokerConfig->httpThreadNum);
    EXPECT_EQ(int(100), brokerConfig->httpQueueSize);
    EXPECT_DOUBLE_EQ(DEFAULT_FILE_CHANGE_FOR_DFS_ERROR, brokerConfig->minChangeFileForDfsErrorTime);
    EXPECT_DOUBLE_EQ(DEFAULT_COMMIT_INTERVAL_AS_ERROR, brokerConfig->maxCommitTimeAsError);
    EXPECT_DOUBLE_EQ(DEFAULT_RECYCLE_PERCENT, brokerConfig->recyclePercent);
    EXPECT_EQ(DEFAULT_ONE_FILE_FD_NUM, brokerConfig->getOneFileFdNum());
    EXPECT_EQ(DEFAULT_CACHE_FILE_RESERVE_TIME, brokerConfig->getCacheFileReserveTime());
    EXPECT_EQ(DEFAULT_CACHE_BLOCK_RESERVE_TIME, brokerConfig->getCacheBlockReserveTime());
    EXPECT_EQ(DEFAULT_READ_QUEUE_SIZE, brokerConfig->readQueueSize);
    EXPECT_EQ(10, brokerConfig->statusReportIntervalSec);
    EXPECT_FALSE(brokerConfig->enableFastRecover);
    EXPECT_TRUE(brokerConfig->useRecommendPort);
    EXPECT_TRUE(brokerConfig->readNotCommittedMsg);
    EXPECT_EQ(0, brokerConfig->getTimestampOffsetInUs());

    PartitionConfig partitionConfig;
    brokerConfig->getDefaultPartitionConfig(partitionConfig);

    EXPECT_EQ(DEFAULT_DFS_FILE_SIZE, partitionConfig.getMaxFileSize());
    EXPECT_EQ(DEFAULT_DFS_FILE_MIN_SIZE, partitionConfig.getMinFileSize());

    EXPECT_EQ(int64_t(1024 * 1024), partitionConfig.getMaxMessageCountInOneFile());
    EXPECT_EQ(uint32_t(5), partitionConfig.getCacheMetaCount());
    EXPECT_EQ(int64_t(getPhyMemSize() * 0.5), brokerConfig->getTotalBufferSize());
    EXPECT_EQ(int64_t(getPhyMemSize() * 0.5 * DEFAULT_FILE_BUFFER_USE_RATIO * (1 - DEFAULT_FILE_META_BUFFER_USE_RATIO)),
              brokerConfig->getBrokerFileBufferSize());
    EXPECT_EQ(int64_t(getPhyMemSize() * 0.5 * DEFAULT_FILE_BUFFER_USE_RATIO * DEFAULT_FILE_META_BUFFER_USE_RATIO),
              brokerConfig->getBrokerFileMetaBufferSize());
    EXPECT_EQ(int64_t(getPhyMemSize() * 0.5 * (1 - DEFAULT_FILE_BUFFER_USE_RATIO)),
              brokerConfig->getBrokerMessageBufferSize());
    EXPECT_EQ(int64_t(DEFAULT_BUFFER_MIN_RESERVE_RATIO * getPhyMemSize() * 0.5 * (1 - DEFAULT_FILE_BUFFER_USE_RATIO)),
              brokerConfig->getBufferMinReserveSize());
    EXPECT_TRUE(brokerConfig->supportMergeMsg);
    EXPECT_TRUE(brokerConfig->supportFb);
    EXPECT_TRUE(brokerConfig->checkFieldFilterMsg);
    EXPECT_EQ(DEFAULT_COMMIT_THREAD_LOOP_INTERVAL_MS, brokerConfig->commitThreadLoopIntervalMs);

    EXPECT_EQ(int64_t(DEFAULT_FILE_CHANGE_FOR_DFS_ERROR * 1000 * 1000),
              partitionConfig.getMinChangeFileForDfsErrorTime());
    EXPECT_EQ(int64_t(DEFAULT_COMMIT_INTERVAL_AS_ERROR * 1000), partitionConfig.getMaxCommitTimeAsError());
    EXPECT_DOUBLE_EQ(DEFAULT_DFS_COMMIT_INTERVAL * 1000 * 1000, partitionConfig.getMaxCommitInterval());
    EXPECT_DOUBLE_EQ(DEFAULT_DFS_COMMIT_INTERVAL_FOR_MEMORY_PREFER_TOPIC * 1000 * 1000,
                     partitionConfig.getMaxCommitIntervalForMemoryPreferTopic());
    EXPECT_DOUBLE_EQ(DEFAULT_DFS_COMMIT_INTERVAL_WHEN_DELAY * 1000 * 1000,
                     partitionConfig.getMaxCommitIntervalWhenDelay());
    EXPECT_DOUBLE_EQ(DEFAULT_RECYCLE_PERCENT, partitionConfig.getRecyclePercent());
    EXPECT_TRUE(partitionConfig.checkFieldFilterMsg());
    EXPECT_TRUE(partitionConfig.readNotCommittedMsg());
    EXPECT_EQ(0, partitionConfig.getTimestampOffsetInUs());
}

TEST_F(BrokerConfigParserTest, testWithInvalidValue) {
    BrokerConfigParser configParser;
    std::unique_ptr<BrokerConfig> brokerConfig(configParser.parse(GET_TEST_DATA_PATH() + "config/invalid_swift.conf"));
    EXPECT_TRUE(brokerConfig.get());
    EXPECT_EQ(int64_t(-1), brokerConfig->maxMessageCountInOneFile);

    PartitionConfig partitionConfig;
    brokerConfig->getDefaultPartitionConfig(partitionConfig);
    EXPECT_EQ(int64_t(-1), partitionConfig.getMaxMessageCountInOneFile());
}

TEST_F(BrokerConfigParserTest, testValidate) {
    BrokerConfig config;
    EXPECT_TRUE(config.validate());

    config.maxReadThreadNum = 10;
    config.concurrentReadFileLimit = 9;
    EXPECT_TRUE(config.validate());

    config.maxReadThreadNum = 10;
    config.concurrentReadFileLimit = 10;
    EXPECT_FALSE(config.validate());

    config.maxReadThreadNum = 9;
    config.concurrentReadFileLimit = 10;
    EXPECT_FALSE(config.validate());

    config.maxReadThreadNum = 10;
    config.concurrentReadFileLimit = 9;
    config.statusReportIntervalSec = 0;
    EXPECT_FALSE(config.validate());
    config.statusReportIntervalSec = 150;
    EXPECT_TRUE(config.validate());
    config.statusReportIntervalSec = 151;
    EXPECT_FALSE(config.validate());
}

size_t BrokerConfigParserTest::getPhyMemSize() {
    struct sysinfo s_info;
    sysinfo(&s_info);
    return s_info.totalram;
}

} // namespace config
} // namespace swift
