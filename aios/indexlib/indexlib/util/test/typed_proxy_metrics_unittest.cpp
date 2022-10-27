#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/typed_proxy_metrics.h"

using namespace std;
using namespace std::tr1;

IE_NAMESPACE_BEGIN(util);

class FakeProxy 
{
public:
    FakeProxy(int64_t initValue)
        : mValue(initValue)
    {}

    int64_t GetValue() const { return mValue++; }
private:
    mutable int64_t mValue;
};

class TypedProxyMetricsTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(TypedProxyMetricsTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForGetValue()
    {
        int64_t initValue = 999;
	tr1::shared_ptr<FakeProxy> proxy(new FakeProxy(initValue));
        TypedProxyMetrics<FakeProxy> metrics("proxy_metrics", proxy);
        
        INDEXLIB_TEST_EQUAL(initValue, metrics.GetValue());
        INDEXLIB_TEST_EQUAL(initValue + 1, metrics.GetValue());
    }

private:
};

INDEXLIB_UNIT_TEST_CASE(TypedProxyMetricsTest, TestCaseForGetValue);

IE_NAMESPACE_END(util);
