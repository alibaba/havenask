#pragma once

#include "indexlib/common_define.h"
#include "indexlib/merger/index_partition_merger_metrics.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace merger {

class IndexPartitionMergerMetricsTest : public INDEXLIB_TESTBASE
{
public:
    IndexPartitionMergerMetricsTest();
    ~IndexPartitionMergerMetricsTest();

    DECLARE_CLASS_NAME(IndexPartitionMergerMetricsTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestProgress();

private:
    void CheckMetricsValue(const std::string& name, const std::string& unit, double value);

private:
    util::MetricProviderPtr mMetricProvider;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerMetricsTest, TestProgress);
}} // namespace indexlib::merger
