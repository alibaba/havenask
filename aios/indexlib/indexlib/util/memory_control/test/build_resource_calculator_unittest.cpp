#include "indexlib/util/memory_control/test/build_resource_calculator_unittest.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, BuildResourceCalculatorTest);

BuildResourceCalculatorTest::BuildResourceCalculatorTest()
{
}

BuildResourceCalculatorTest::~BuildResourceCalculatorTest()
{
}

void BuildResourceCalculatorTest::CaseSetUp()
{
}

void BuildResourceCalculatorTest::CaseTearDown()
{
}

void BuildResourceCalculatorTest::TestSimpleProcess()
{
    BuildResourceMetricsPtr metrics(new BuildResourceMetrics);
    metrics->Init();
    
    BuildResourceMetricsNode* node1 = metrics->AllocateNode();
    BuildResourceMetricsNode* node2 = metrics->AllocateNode();
    BuildResourceMetricsNode* node3 = metrics->AllocateNode();
    node1->Update(BMT_CURRENT_MEMORY_USE, 10);
    node2->Update(BMT_CURRENT_MEMORY_USE, 20);
    node3->Update(BMT_CURRENT_MEMORY_USE, 30);
    node1->Update(BMT_DUMP_TEMP_MEMORY_SIZE, 10);
    node2->Update(BMT_DUMP_TEMP_MEMORY_SIZE, 10);
    node3->Update(BMT_DUMP_TEMP_MEMORY_SIZE, 20);
    node1->Update(BMT_DUMP_EXPAND_MEMORY_SIZE, 20);
    node2->Update(BMT_DUMP_EXPAND_MEMORY_SIZE, 30);
    node3->Update(BMT_DUMP_EXPAND_MEMORY_SIZE, 40);
    node1->Update(BMT_DUMP_FILE_SIZE, 30);
    node2->Update(BMT_DUMP_FILE_SIZE, 30);
    node3->Update(BMT_DUMP_FILE_SIZE, 40);

    typedef BuildResourceCalculator calculator;
    int dumpThreadCount = 2;
    
    EXPECT_EQ(60, calculator::GetCurrentTotalMemoryUse(metrics));
    EXPECT_EQ(40, calculator::EstimateDumpTempMemoryUse(metrics, dumpThreadCount));
    EXPECT_EQ(90, calculator::EstimateDumpExpandMemoryUse(metrics));
    EXPECT_EQ(100, calculator::EstimateDumpFileSize(metrics)); 
}

IE_NAMESPACE_END(util);

