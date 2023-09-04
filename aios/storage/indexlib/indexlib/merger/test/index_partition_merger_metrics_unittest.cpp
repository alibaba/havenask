#include "indexlib/merger/test/index_partition_merger_metrics_unittest.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, IndexPartitionMergerMetricsTest);

IndexPartitionMergerMetricsTest::IndexPartitionMergerMetricsTest() {}

IndexPartitionMergerMetricsTest::~IndexPartitionMergerMetricsTest() {}

void IndexPartitionMergerMetricsTest::CaseSetUp() { mMetricProvider.reset(new util::MetricProvider()); }

void IndexPartitionMergerMetricsTest::CaseTearDown() {}

void IndexPartitionMergerMetricsTest::CheckMetricsValue(const string& name, const string& unit, double value)
{
    // NOTE: only support STATUS type
    util::MetricPtr metric = mMetricProvider->DeclareMetric(name, kmonitor::STATUS);
    ASSERT_TRUE(metric != nullptr);
    ASSERT_DOUBLE_EQ(value, metric->_value) << name;
}

void IndexPartitionMergerMetricsTest::TestProgress()
{
    IndexPartitionMergerMetrics metrics(mMetricProvider);
    ProgressMetricsPtr itemMetrics1 = metrics.RegisterMergeItem(40);
    ProgressMetricsPtr itemMetrics2 = metrics.RegisterMergeItem(60);

    metrics.StartReport();

    CheckMetricsValue("indexlib/merge/progress", "%", 0.0);
    CheckMetricsValue("indexlib/merge/leftItemCount", "count", 2);

    itemMetrics1->UpdateCurrentRatio(0.5);
    metrics.ReportMetrics();

    CheckMetricsValue("indexlib/merge/progress", "%", 20.0);
    CheckMetricsValue("indexlib/merge/leftItemCount", "count", 2);

    itemMetrics2->UpdateCurrentRatio(0.5);
    metrics.ReportMetrics();

    CheckMetricsValue("indexlib/merge/progress", "%", 50.0);
    CheckMetricsValue("indexlib/merge/leftItemCount", "count", 2);

    itemMetrics1->SetFinish();
    metrics.ReportMetrics();

    CheckMetricsValue("indexlib/merge/progress", "%", 70.0);
    CheckMetricsValue("indexlib/merge/leftItemCount", "count", 1);

    itemMetrics2->SetFinish();
    metrics.ReportMetrics();

    CheckMetricsValue("indexlib/merge/progress", "%", 100.0);
    CheckMetricsValue("indexlib/merge/leftItemCount", "count", 0);
}
}} // namespace indexlib::merger
