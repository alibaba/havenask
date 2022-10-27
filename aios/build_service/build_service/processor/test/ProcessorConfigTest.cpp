#include "build_service/test/unittest.h"
#include "build_service/processor/ProcessorConfig.h"

using namespace std;

namespace build_service {
namespace processor {

class ProcessorConfigTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void ProcessorConfigTest::setUp() {
}

void ProcessorConfigTest::tearDown() {
}

TEST_F(ProcessorConfigTest, testFullConfigJson) {
    string content;
    string filePath = TEST_DATA_PATH"processor_config_test/full_processor_config.json";
    ASSERT_TRUE(util::FileUtil::readFile(filePath, content));
    ProcessorConfig processorConfig;
    ASSERT_NO_THROW(FromJsonString(processorConfig, content));
    EXPECT_EQ(uint32_t(16), processorConfig.processorThreadNum);
    EXPECT_EQ(uint32_t(10000), processorConfig.processorQueueSize);
    EXPECT_EQ("normal", processorConfig.processorStrategyStr);
}

TEST_F(ProcessorConfigTest, testEmptyConfigJson) {
    string content = "{}";
    ProcessorConfig processorConfig;
    ASSERT_NO_THROW(FromJsonString(processorConfig, content));
    EXPECT_EQ(ProcessorConfig::DEFAULT_PROCESSOR_THREAD_NUM, processorConfig.processorThreadNum);
    EXPECT_EQ(ProcessorConfig::DEFAULT_PROCESSOR_QUEUE_SIZE, processorConfig.processorQueueSize);
}

TEST_F(ProcessorConfigTest, testJsonize) {
    ProcessorConfig processorConfig;
    processorConfig.processorThreadNum = 20;
    processorConfig.processorQueueSize = 20000;
    string jsonStr = ToJsonString(processorConfig);
    ProcessorConfig otherProcessorConfig;
    ASSERT_NO_THROW(FromJsonString(otherProcessorConfig, jsonStr));
    EXPECT_EQ(processorConfig.processorThreadNum, otherProcessorConfig.processorThreadNum);
    EXPECT_EQ(processorConfig.processorQueueSize, otherProcessorConfig.processorQueueSize);
}

TEST_F(ProcessorConfigTest, testBatchProcessorStrategy) {
    string content;
    string filePath = TEST_DATA_PATH"processor_config_test/batch_processor_config.json";
    ASSERT_TRUE(util::FileUtil::readFile(filePath, content));
    ProcessorConfig processorConfig;
    ASSERT_NO_THROW(FromJsonString(processorConfig, content));
    EXPECT_EQ(uint32_t(16), processorConfig.processorThreadNum);
    EXPECT_EQ(uint32_t(10000), processorConfig.processorQueueSize);
    EXPECT_EQ("batch", processorConfig.processorStrategyStr);
    EXPECT_TRUE(!processorConfig.processorStrategyParameter.empty());

    EXPECT_TRUE(processorConfig.validate());
    processorConfig.processorStrategyStr = "unknown";
    EXPECT_FALSE(processorConfig.validate());

    {
        content = "{ \"processor_strategy\" : \"batch\" }";
        ProcessorConfig processorConfig;
        ASSERT_NO_THROW(FromJsonString(processorConfig, content));
        EXPECT_EQ(100, processorConfig.processorQueueSize);
    }
}

}
}
