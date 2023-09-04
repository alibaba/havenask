#include "indexlib/util/memory_control/BuildResourceMetrics.h"

#include "autil/Log.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
namespace indexlib { namespace util {

class BuildResourceMetricsTest : public INDEXLIB_TESTBASE
{
public:
    BuildResourceMetricsTest();
    ~BuildResourceMetricsTest();

    DECLARE_CLASS_NAME(BuildResourceMetricsTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BuildResourceMetricsTest, TestSimpleProcess);

AUTIL_LOG_SETUP(indexlib.util, BuildResourceMetricsTest);

BuildResourceMetricsTest::BuildResourceMetricsTest() {}

BuildResourceMetricsTest::~BuildResourceMetricsTest() {}

void BuildResourceMetricsTest::CaseSetUp() {}

void BuildResourceMetricsTest::CaseTearDown() {}

void BuildResourceMetricsTest::TestSimpleProcess()
{
    BuildResourceMetrics buildResourceMetrics;
    buildResourceMetrics.Init();

    ASSERT_FALSE(buildResourceMetrics.RegisterMetrics(BMT_CURRENT_MEMORY_USE, BST_ACCUMULATE_SAMPLE, "CurrentMemUse"));
    ASSERT_FALSE(buildResourceMetrics.RegisterMetrics(BMT_DUMP_TEMP_MEMORY_SIZE, BST_MAX_SAMPLE, "DumpTempMemUse"));

    BuildResourceMetricsNode* node1 = buildResourceMetrics.AllocateNode();
    BuildResourceMetricsNode* node2 = buildResourceMetrics.AllocateNode();
    BuildResourceMetricsNode* node3 = buildResourceMetrics.AllocateNode();

    node1->Update(BMT_CURRENT_MEMORY_USE, 10);
    node2->Update(BMT_CURRENT_MEMORY_USE, 20);
    node3->Update(BMT_CURRENT_MEMORY_USE, 30);

    EXPECT_EQ(60, buildResourceMetrics.GetValue(BMT_CURRENT_MEMORY_USE));
    node1->Update(BMT_CURRENT_MEMORY_USE, 20);
    EXPECT_EQ(70, buildResourceMetrics.GetValue(BMT_CURRENT_MEMORY_USE));

    node1->Update(BMT_DUMP_TEMP_MEMORY_SIZE, 10);
    node2->Update(BMT_DUMP_TEMP_MEMORY_SIZE, 20);
    node3->Update(BMT_DUMP_TEMP_MEMORY_SIZE, 30);

    EXPECT_EQ(30, buildResourceMetrics.GetValue(BMT_DUMP_TEMP_MEMORY_SIZE));
    node2->Update(BMT_DUMP_TEMP_MEMORY_SIZE, 50);
    EXPECT_EQ(50, buildResourceMetrics.GetValue(BMT_DUMP_TEMP_MEMORY_SIZE));
}
}} // namespace indexlib::util
