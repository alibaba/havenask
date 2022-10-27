#include "indexlib/misc/test/indexlib_metric_control_unittest.h"
#include <stdlib.h>

using namespace std;
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(misc);
IE_LOG_SETUP(misc, IndexlibMetricControlTest);

IndexlibMetricControlTest::IndexlibMetricControlTest()
{
}

IndexlibMetricControlTest::~IndexlibMetricControlTest()
{
}

void IndexlibMetricControlTest::CaseSetUp()
{
}

void IndexlibMetricControlTest::CaseTearDown()
{
}

void IndexlibMetricControlTest::TestSimpleProcess()
{
    string patternStr = "pattern=.*Latency.*,id=gather1,prohibit=true;"
                        "pattern=.*PartitionMemoryUse.*,id=gather2;"
                        "pattern=.*MemoryUse.*,id=gather3;"
                        "pattern=.*file_system/.*,prohibit=true";

    string jsonPatternStr = "[                             \
    {                                                      \
        \"pattern\" : \".*Latency.*\",                     \
        \"id\" : \"gather1\",                              \
        \"prohibit\" : true                                \
    },                                                     \
    {                                                      \
        \"pattern\" : \".*PartitionMemoryUse.*\",          \
        \"id\" : \"gather2\"                               \
    },                                                     \
    {                                                      \
        \"pattern\" : \".*MemoryUse.*\",                   \
        \"id\" : \"gather3\"                               \
    },                                                     \
    {                                                      \
        \"pattern\" : \".*file_system/.*\",                \
        \"prohibit\" : true                                \
    }                                                      \
    ]";

    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
    string fileName = "metric.json";
    rootDir->Store(fileName, jsonPatternStr);

    // test read env with flat pattern
    setenv("INDEXLIB_METRIC_PARAM", patternStr.c_str(), 1);
    Check();
    unsetenv("INDEXLIB_METRIC_PARAM");

    // check read json file
    IndexlibMetricControl::GetInstance()->TEST_RESET();
    string filePath = GET_TEST_DATA_PATH() + "/" + fileName;
    setenv("INDEXLIB_METRIC_PARAM_FILE_PATH", filePath.c_str(), 1);
    IndexlibMetricControl::GetInstance()->InitFromEnv();
    Check();
}

void IndexlibMetricControlTest::Check()
{
    IndexlibMetricControl::Status status;
    status = IndexlibMetricControl::GetInstance()->Get("reopenLatency");
    ASSERT_EQ(string("gather1"), status.gatherId);
    ASSERT_TRUE(status.prohibit);

    status = IndexlibMetricControl::GetInstance()->Get("basic/IndexPartitionMemoryUse");
    ASSERT_EQ(string("gather2"), status.gatherId);
    ASSERT_FALSE(status.prohibit);    

    status = IndexlibMetricControl::GetInstance()->Get("basic/MemoryUse");
    ASSERT_EQ(string("gather3"), status.gatherId);
    ASSERT_FALSE(status.prohibit);    

    status = IndexlibMetricControl::GetInstance()->Get("file_system/fileLen");
    ASSERT_EQ(string(""), status.gatherId);
    ASSERT_TRUE(status.prohibit);    

    status = IndexlibMetricControl::GetInstance()->Get("no_match_metric");
    ASSERT_EQ(string(""), status.gatherId);
    ASSERT_FALSE(status.prohibit);
}

IE_NAMESPACE_END(misc);

