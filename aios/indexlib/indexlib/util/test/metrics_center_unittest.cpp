#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/metrics_center.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);

class FakeProxyMetrics : public ProxyMetrics
{
public:
    FakeProxyMetrics(const string &metricsName)
        : ProxyMetrics(metricsName)
    {
    }

public:
    int64_t GetValue() const 
    {
        return 0;
    }
};

// only test ProxyMetrics, because other types of Metrics is tested in ProfilingTest
class MetricsCenterTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(MetricsCenterTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForRegisterProxyMetrics()
    {
        MetricsCenter *metricsCenter = MetricsCenter::GetInstance();
        string metricsName("proxy_metrics1");
        ProxyMetricsPtr proxyMetrics(new FakeProxyMetrics(metricsName));
        INDEXLIB_TEST_TRUE(metricsCenter->RegisterProxyMetrics(proxyMetrics));
        INDEXLIB_TEST_TRUE(!metricsCenter->RegisterProxyMetrics(proxyMetrics));

        INDEXLIB_TEST_TRUE(metricsCenter->UnRegisterProxyMetrics(metricsName));
        INDEXLIB_TEST_TRUE(!metricsCenter->UnRegisterProxyMetrics(metricsName));
    }

    void TestCaseForTryGetProxyMetrics()
    {
        MetricsCenter *metricsCenter = MetricsCenter::GetInstance();
        INDEXLIB_TEST_TRUE(NULL == metricsCenter->TryGetProxyMetrics("not_exist_metrics"));
    
        string metricsName("proxy_metrics2");
        ProxyMetricsPtr proxyMetrics(new FakeProxyMetrics(metricsName));
        INDEXLIB_TEST_TRUE(metricsCenter->RegisterProxyMetrics(proxyMetrics));
        INDEXLIB_TEST_EQUAL(proxyMetrics.get(), 
                            metricsCenter->TryGetProxyMetrics(metricsName));

        INDEXLIB_TEST_TRUE(metricsCenter->UnRegisterProxyMetrics(metricsName));
        INDEXLIB_TEST_TRUE(metricsCenter->TryGetProxyMetrics(metricsName) == NULL);
    }

private:
};

INDEXLIB_UNIT_TEST_CASE(MetricsCenterTest, TestCaseForRegisterProxyMetrics);
INDEXLIB_UNIT_TEST_CASE(MetricsCenterTest, TestCaseForTryGetProxyMetrics);

IE_NAMESPACE_END(util);
