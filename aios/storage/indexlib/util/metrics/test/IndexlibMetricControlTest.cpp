#include "indexlib/util/metrics/IndexlibMetricControl.h"

#include <stdlib.h>

#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace util {
class IndexlibMetricControlTest : public INDEXLIB_TESTBASE
{
public:
    IndexlibMetricControlTest();
    ~IndexlibMetricControlTest();

    DECLARE_CLASS_NAME(IndexlibMetricControlTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void Check();

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.util, IndexlibMetricControlTest);

INDEXLIB_UNIT_TEST_CASE(IndexlibMetricControlTest, TestSimpleProcess);

IndexlibMetricControlTest::IndexlibMetricControlTest() {}

IndexlibMetricControlTest::~IndexlibMetricControlTest() {}

void IndexlibMetricControlTest::CaseSetUp() {}

void IndexlibMetricControlTest::CaseTearDown() {}

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

    string filePath = GET_TEMP_DATA_PATH() + "/metric.json";
    fslib::fs::FilePtr file(fslib::fs::FileSystem::openFile(filePath, fslib::WRITE));
    ASSERT_TRUE(file);
    ASSERT_EQ(jsonPatternStr.size(), file->write(jsonPatternStr.data(), jsonPatternStr.size()));
    ASSERT_EQ(fslib::EC_OK, file->close());

    // test read env with flat pattern
    setenv("INDEXLIB_METRIC_PARAM", patternStr.c_str(), 1);
    Check();
    unsetenv("INDEXLIB_METRIC_PARAM");

    // check read json file
    IndexlibMetricControl::GetInstance()->TEST_RESET();
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
}} // namespace indexlib::util
