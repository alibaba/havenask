#include "indexlib/config/test/merge_io_config_unittest.h"

using namespace std;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, MergeIOConfigTest);

MergeIOConfigTest::MergeIOConfigTest() {}

MergeIOConfigTest::~MergeIOConfigTest() {}

void MergeIOConfigTest::CaseSetUp() {}

void MergeIOConfigTest::CaseTearDown() {}

void MergeIOConfigTest::TestJsonize()
{
    MergeIOConfig mergeIOConfig;
    ASSERT_EQ(false, mergeIOConfig.enableAsyncRead);
    ASSERT_EQ(false, mergeIOConfig.enableAsyncWrite);
    ASSERT_EQ(MergeIOConfig::DEFAULT_READ_BUFFER_SIZE, mergeIOConfig.readBufferSize);
    ASSERT_EQ(MergeIOConfig::DEFAULT_WRITE_BUFFER_SIZE, mergeIOConfig.writeBufferSize);
    ASSERT_EQ(MergeIOConfig::DEFAULT_READ_THREAD_NUM, mergeIOConfig.readThreadNum);
    ASSERT_EQ(MergeIOConfig::DEFAULT_READ_QUEUE_SIZE, mergeIOConfig.readQueueSize);
    ASSERT_EQ(MergeIOConfig::DEFAULT_WRITE_THREAD_NUM, mergeIOConfig.writeThreadNum);
    ASSERT_EQ(MergeIOConfig::DEFAULT_WRITE_QUEUE_SIZE, mergeIOConfig.writeQueueSize);

    string jsonStr = "{"
                     "\"enable_async_read\" : true,"
                     "\"enable_async_write\" : false,"
                     "\"read_buffer_size\" : 1,"
                     "\"write_buffer_size\" : 10,"
                     "\"read_thread_num\" : 20,"
                     "\"read_queue_size\" : 100,"
                     "\"write_thread_num\" : 50,"
                     "\"write_queue_size\" : 20"
                     "}";

    FromJsonString(mergeIOConfig, jsonStr);
    ASSERT_EQ(true, mergeIOConfig.enableAsyncRead);
    ASSERT_EQ(false, mergeIOConfig.enableAsyncWrite);
    ASSERT_EQ((uint32_t)1, mergeIOConfig.readBufferSize);
    ASSERT_EQ((uint32_t)10, mergeIOConfig.writeBufferSize);
    ASSERT_EQ((uint32_t)20, mergeIOConfig.readThreadNum);
    ASSERT_EQ((uint32_t)100, mergeIOConfig.readQueueSize);
    ASSERT_EQ((uint32_t)50, mergeIOConfig.writeThreadNum);
    ASSERT_EQ((uint32_t)20, mergeIOConfig.writeQueueSize);

    jsonStr = ToJsonString(mergeIOConfig);
    MergeIOConfig mergeIOConfigNew;
    FromJsonString(mergeIOConfigNew, jsonStr);
    ASSERT_EQ(true, mergeIOConfigNew.enableAsyncRead);
    ASSERT_EQ(false, mergeIOConfigNew.enableAsyncWrite);
    ASSERT_EQ((uint32_t)1, mergeIOConfigNew.readBufferSize);
    ASSERT_EQ((uint32_t)10, mergeIOConfigNew.writeBufferSize);
    ASSERT_EQ((uint32_t)20, mergeIOConfigNew.readThreadNum);
    ASSERT_EQ((uint32_t)100, mergeIOConfigNew.readQueueSize);
    ASSERT_EQ((uint32_t)50, mergeIOConfigNew.writeThreadNum);
    ASSERT_EQ((uint32_t)20, mergeIOConfigNew.writeQueueSize);
}
}} // namespace indexlib::config
