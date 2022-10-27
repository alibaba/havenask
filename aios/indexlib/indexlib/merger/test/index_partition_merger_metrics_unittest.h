#ifndef __INDEXLIB_INDEXPARTITIONMERGERMETRICSTEST_H
#define __INDEXLIB_INDEXPARTITIONMERGERMETRICSTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/merger/index_partition_merger_metrics.h"
#include "indexlib/misc/metric_provider.h"

IE_NAMESPACE_BEGIN(merger);

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
    void CheckMetricsValue(const std::string& name, const std::string& unit, 
                           double value);
private:
    misc::MetricProviderPtr mMetricProvider;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerMetricsTest, TestProgress);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_INDEXPARTITIONMERGERMETRICSTEST_H
