#include "indexlib/util/memory_control/test/build_resource_metrics_unittest.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, BuildResourceMetricsTest);

BuildResourceMetricsTest::BuildResourceMetricsTest()
{
}

BuildResourceMetricsTest::~BuildResourceMetricsTest()
{
}

void BuildResourceMetricsTest::CaseSetUp()
{
}

void BuildResourceMetricsTest::CaseTearDown()
{
}

void BuildResourceMetricsTest::TestSimpleProcess()
{
    BuildResourceMetrics buildResourceMetrics;
    buildResourceMetrics.Init();

    ASSERT_FALSE(buildResourceMetrics.RegisterMetrics(
                    BMT_CURRENT_MEMORY_USE, BST_ACCUMULATE_SAMPLE, "CurrentMemUse"));
    ASSERT_FALSE(buildResourceMetrics.RegisterMetrics(
                    BMT_DUMP_TEMP_MEMORY_SIZE, BST_MAX_SAMPLE, "DumpTempMemUse"));

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

IE_NAMESPACE_END(util);

