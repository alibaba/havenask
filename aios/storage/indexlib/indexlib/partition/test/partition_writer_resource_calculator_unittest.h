#pragma once

#include "indexlib/common_define.h"
#include "indexlib/partition/partition_writer_resource_calculator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class PartitionWriterResourceCalculatorTest : public INDEXLIB_TESTBASE
{
public:
    PartitionWriterResourceCalculatorTest();
    ~PartitionWriterResourceCalculatorTest();

    DECLARE_CLASS_NAME(PartitionWriterResourceCalculatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    util::BuildResourceMetricsPtr CreateBuildResourceMetrics(size_t totalMemUse, size_t dumpTempMemUse,
                                                             size_t dumpExpandMemUse, size_t dumpFileSize);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PartitionWriterResourceCalculatorTest, TestSimpleProcess);
}} // namespace indexlib::partition
