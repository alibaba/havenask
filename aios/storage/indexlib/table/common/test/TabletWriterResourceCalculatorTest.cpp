#include "indexlib/table/common/TabletWriterResourceCalculator.h"

#include "indexlib/util/memory_control/BuildResourceMetrics.h"
#include "unittest/unittest.h"

using namespace indexlib::util;

namespace indexlib::table {
class TabletWriterResourceCalculatorTest : public TESTBASE
{
public:
    std::shared_ptr<indexlib::util::BuildResourceMetrics>
    CreateBuildResourceMetrics(size_t totalMemUse, size_t dumpTempMemUse, size_t dumpExpandMemUse, size_t dumpFileSize);
};

TEST_F(TabletWriterResourceCalculatorTest, TestSimpleProcess)
{
    std::shared_ptr<indexlib::util::BuildResourceMetrics> writerMetrics = CreateBuildResourceMetrics(100, 10, 20, 50);

    TabletWriterResourceCalculator calculator(writerMetrics, false, 3);

    // when index node less than dump thread count, * indexCount, not dump thread count
    size_t expectMemUseOfCurrentSegment = 100 + 20 + 50 + 10;
    ASSERT_EQ(expectMemUseOfCurrentSegment, calculator.EstimateMaxMemoryUseOfCurrentSegment());

    size_t expectDumpMemoryUse = 20 + 10;
    ASSERT_EQ(expectDumpMemoryUse, calculator.EstimateDumpMemoryUse());

    size_t expectDumpFileSize = 50;
    ASSERT_EQ(expectDumpFileSize, calculator.EstimateDumpFileSize());
}

std::shared_ptr<indexlib::util::BuildResourceMetrics>
TabletWriterResourceCalculatorTest::CreateBuildResourceMetrics(size_t totalMemUse, size_t dumpTempMemUse,
                                                               size_t dumpExpandMemUse, size_t dumpFileSize)
{
    std::shared_ptr<indexlib::util::BuildResourceMetrics> metrics(new BuildResourceMetrics);
    metrics->Init();

    BuildResourceMetricsNode* node = metrics->AllocateNode();
    node->Update(BMT_CURRENT_MEMORY_USE, totalMemUse);
    node->Update(BMT_DUMP_TEMP_MEMORY_SIZE, dumpTempMemUse);
    node->Update(BMT_DUMP_EXPAND_MEMORY_SIZE, dumpExpandMemUse);
    node->Update(BMT_DUMP_FILE_SIZE, dumpFileSize);
    return metrics;
}
} // namespace indexlib::table
