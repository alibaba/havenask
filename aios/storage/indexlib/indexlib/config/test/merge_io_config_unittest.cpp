#include "indexlib/config/test/merge_io_config_unittest.h"

using namespace std;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, MergeIOConfigTest);

MergeIOConfigTest::MergeIOConfigTest() {}

MergeIOConfigTest::~MergeIOConfigTest() {}

void MergeIOConfigTest::CaseSetUp() {}

void MergeIOConfigTest::CaseTearDown() {}

void MergeIOConfigTest::TestJsonize()
{
    MergeIOConfig mergeIOConfig;
    INDEXLIB_TEST_EQUAL(false, mergeIOConfig.enableAsyncRead);
    INDEXLIB_TEST_EQUAL(false, mergeIOConfig.enableAsyncWrite);
    INDEXLIB_TEST_EQUAL(MergeIOConfig::DEFAULT_READ_BUFFER_SIZE, mergeIOConfig.readBufferSize);
    INDEXLIB_TEST_EQUAL(MergeIOConfig::DEFAULT_WRITE_BUFFER_SIZE, mergeIOConfig.writeBufferSize);
    INDEXLIB_TEST_EQUAL(MergeIOConfig::DEFAULT_READ_THREAD_NUM, mergeIOConfig.readThreadNum);
    INDEXLIB_TEST_EQUAL(MergeIOConfig::DEFAULT_READ_QUEUE_SIZE, mergeIOConfig.readQueueSize);
    INDEXLIB_TEST_EQUAL(MergeIOConfig::DEFAULT_WRITE_THREAD_NUM, mergeIOConfig.writeThreadNum);
    INDEXLIB_TEST_EQUAL(MergeIOConfig::DEFAULT_WRITE_QUEUE_SIZE, mergeIOConfig.writeQueueSize);

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
    INDEXLIB_TEST_EQUAL(true, mergeIOConfig.enableAsyncRead);
    INDEXLIB_TEST_EQUAL(false, mergeIOConfig.enableAsyncWrite);
    INDEXLIB_TEST_EQUAL((uint32_t)1, mergeIOConfig.readBufferSize);
    INDEXLIB_TEST_EQUAL((uint32_t)10, mergeIOConfig.writeBufferSize);
    INDEXLIB_TEST_EQUAL((uint32_t)20, mergeIOConfig.readThreadNum);
    INDEXLIB_TEST_EQUAL((uint32_t)100, mergeIOConfig.readQueueSize);
    INDEXLIB_TEST_EQUAL((uint32_t)50, mergeIOConfig.writeThreadNum);
    INDEXLIB_TEST_EQUAL((uint32_t)20, mergeIOConfig.writeQueueSize);

    jsonStr = ToJsonString(mergeIOConfig);
    MergeIOConfig mergeIOConfigNew;
    FromJsonString(mergeIOConfigNew, jsonStr);
    INDEXLIB_TEST_EQUAL(true, mergeIOConfigNew.enableAsyncRead);
    INDEXLIB_TEST_EQUAL(false, mergeIOConfigNew.enableAsyncWrite);
    INDEXLIB_TEST_EQUAL((uint32_t)1, mergeIOConfigNew.readBufferSize);
    INDEXLIB_TEST_EQUAL((uint32_t)10, mergeIOConfigNew.writeBufferSize);
    INDEXLIB_TEST_EQUAL((uint32_t)20, mergeIOConfigNew.readThreadNum);
    INDEXLIB_TEST_EQUAL((uint32_t)100, mergeIOConfigNew.readQueueSize);
    INDEXLIB_TEST_EQUAL((uint32_t)50, mergeIOConfigNew.writeThreadNum);
    INDEXLIB_TEST_EQUAL((uint32_t)20, mergeIOConfigNew.writeQueueSize);
}
}} // namespace indexlib::config
