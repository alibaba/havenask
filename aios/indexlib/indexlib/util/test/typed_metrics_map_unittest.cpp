#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/typed_metrics_map.h"
#include "indexlib/util/process_metrics_group.h"
#include "indexlib/util/metrics.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);

class TypedMetricsMapTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(TypedMetricsMapTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForGetMetrics()
    {
        typedef TypedMetricsMap<ProcessMetricsGroup<Metrics> > ProcessMetricsMap;
        
        ProcessMetricsMap map;
        Metrics& m1 = map.GetMetrics("testMetrics1");
        INDEXLIB_TEST_EQUAL((int64_t)0, m1.GetValue());
        Metrics& m2 = map.GetMetrics("testMetrics2");
        INDEXLIB_TEST_EQUAL((int64_t)0, m2.GetValue());

        Metrics& m3 = map.GetMetrics("testMetrics1");
        INDEXLIB_TEST_TRUE(&m1 == &m3);

        INDEXLIB_TEST_EQUAL((uint32_t)1, map.GetMetricsCount("testMetrics1"));
        INDEXLIB_TEST_EQUAL((uint32_t)1, map.GetMetricsCount("testMetrics2"));
        INDEXLIB_TEST_EQUAL((uint32_t)0, map.GetMetricsCount("testMetrics3"));

        m1.AddValue(2);
        INDEXLIB_TEST_EQUAL((int64_t)1, map.GetTotalCountByMetricsName(
                        "testMetrics1"));
        INDEXLIB_TEST_EQUAL((int64_t)2, map.GetTotalValueByMetricsName(
                        "testMetrics1"));

        INDEXLIB_TEST_EQUAL((int64_t)0, map.GetTotalCountByMetricsName(
                        "testMetrics2"));
        INDEXLIB_TEST_EQUAL((int64_t)0, map.GetTotalValueByMetricsName(
                        "testMetrics2"));
        
        map.ResetAll();
        INDEXLIB_TEST_EQUAL((int64_t)0, map.GetTotalCountByMetricsName(
                        "testMetrics1"));
        INDEXLIB_TEST_EQUAL((int64_t)0, map.GetTotalValueByMetricsName(
                        "testMetrics1"));        
    }

    void TestCaseForExtractMetricsNames()
    {
        typedef TypedMetricsMap<ProcessMetricsGroup<Metrics> > ProcessMetricsMap;
        
        ProcessMetricsMap map;
        Metrics& m1 = map.GetMetrics("testMetrics1");
        INDEXLIB_TEST_EQUAL((int64_t)0, m1.GetValue());
        Metrics& m2 = map.GetMetrics("testMetrics2");
        INDEXLIB_TEST_EQUAL((int64_t)0, m2.GetValue());
        
        std::vector<std::string> names;
        map.ExtractMetricsNames(names);

        INDEXLIB_TEST_EQUAL((size_t)2, names.size());
        INDEXLIB_TEST_EQUAL(string("testMetrics1"), names[0]);
        INDEXLIB_TEST_EQUAL(string("testMetrics2"), names[1]);
    }

private:
};

INDEXLIB_UNIT_TEST_CASE(TypedMetricsMapTest, TestCaseForGetMetrics);
INDEXLIB_UNIT_TEST_CASE(TypedMetricsMapTest, TestCaseForExtractMetricsNames);

IE_NAMESPACE_END(util);
