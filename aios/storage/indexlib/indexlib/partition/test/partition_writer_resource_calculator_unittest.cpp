#include "indexlib/partition/test/partition_writer_resource_calculator_unittest.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionWriterResourceCalculatorTest);

PartitionWriterResourceCalculatorTest::PartitionWriterResourceCalculatorTest() {}

PartitionWriterResourceCalculatorTest::~PartitionWriterResourceCalculatorTest() {}

void PartitionWriterResourceCalculatorTest::CaseSetUp() {}

void PartitionWriterResourceCalculatorTest::CaseTearDown() {}

void PartitionWriterResourceCalculatorTest::TestSimpleProcess()
{
    BuildResourceMetricsPtr writerMetrics = CreateBuildResourceMetrics(100, 10, 20, 50);
    BuildResourceMetricsPtr modifierMetrics = CreateBuildResourceMetrics(200, 30, 40, 80);
    BuildResourceMetricsPtr opWriterMetrics = CreateBuildResourceMetrics(150, 40, 60, 55);

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().dumpThreadCount = 3;

    PartitionWriterResourceCalculator calculator(options);
    calculator.mSegWriterMetrics = writerMetrics;
    calculator.mModifierMetrics = modifierMetrics;
    calculator.mOperationWriterMetrics = opWriterMetrics;

    // when index node less than dump thread count, * indexCount, not dump thread count
    size_t expectMemUseOfCurrentSegment = 100 + 200 + 150 + 20 + 40 + 60 + 50 + 80 + 55 + max(max(10, 40) * 2, 30);
    ASSERT_EQ(expectMemUseOfCurrentSegment, calculator.EstimateMaxMemoryUseOfCurrentSegment());

    size_t expectDumpMemoryUse = 20 + 40 + 60 + max(max(10, 40) * 2, 30);
    ASSERT_EQ(expectDumpMemoryUse, calculator.EstimateDumpMemoryUse());

    size_t expectDumpFileSize = 50 + 80 + 55;
    ASSERT_EQ(expectDumpFileSize, calculator.EstimateDumpFileSize());
}

BuildResourceMetricsPtr PartitionWriterResourceCalculatorTest::CreateBuildResourceMetrics(size_t totalMemUse,
                                                                                          size_t dumpTempMemUse,
                                                                                          size_t dumpExpandMemUse,
                                                                                          size_t dumpFileSize)
{
    BuildResourceMetricsPtr metrics(new BuildResourceMetrics);
    metrics->Init();

    BuildResourceMetricsNode* node = metrics->AllocateNode();
    node->Update(BMT_CURRENT_MEMORY_USE, totalMemUse);
    node->Update(BMT_DUMP_TEMP_MEMORY_SIZE, dumpTempMemUse);
    node->Update(BMT_DUMP_EXPAND_MEMORY_SIZE, dumpExpandMemUse);
    node->Update(BMT_DUMP_FILE_SIZE, dumpFileSize);
    return metrics;
}
}} // namespace indexlib::partition
