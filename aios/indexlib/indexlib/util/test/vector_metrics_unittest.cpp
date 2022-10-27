#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/vector_metrics.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);

class VectorMetricsTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(VectorMetricsTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForPushBack()
    {
        const static size_t VALUE_COUNT = 100;
        
        StrVectorMetrics vm("test_metrics");
        for (size_t i = 0; i < VALUE_COUNT; ++i)
        {
            stringstream ss;
            ss << "testvalue" << i;
            vm.PushBack(ss.str());
        }
        
        INDEXLIB_TEST_EQUAL(VALUE_COUNT, vm.Size());

        for (size_t j = 0; j < VALUE_COUNT; ++j)
        {
            stringstream ss;
            ss << "testvalue" << j;
            INDEXLIB_TEST_EQUAL(ss.str(), vm.GetValue(j));
        }

        vm.Reset();
        INDEXLIB_TEST_EQUAL((size_t)0, vm.Size());
    }

    void TestCaseForPairedMetricsPushBack()
    {
        const static size_t VALUE_COUNT = 100;
        
        PairedVectorMetrics vm("test_metrics");
        for (size_t i = 0; i < VALUE_COUNT; ++i)
        {
            stringstream ss;
            ss << "testvalue" << i;
            vm.PushBack(make_pair(ss.str(), i));
        }
        
        INDEXLIB_TEST_EQUAL(VALUE_COUNT, vm.Size());

        for (size_t j = 0; j < VALUE_COUNT; ++j)
        {
            stringstream ss;
            ss << "testvalue" << j;
            INDEXLIB_TEST_EQUAL(ss.str(), vm.GetValue(j).first);
            INDEXLIB_TEST_EQUAL(j, (size_t)vm.GetValue(j).second);
        }

        vm.Reset();
        INDEXLIB_TEST_EQUAL((size_t)0, vm.Size());
    }


private:
};

INDEXLIB_UNIT_TEST_CASE(VectorMetricsTest, TestCaseForPushBack);
INDEXLIB_UNIT_TEST_CASE(VectorMetricsTest, TestCaseForPairedMetricsPushBack);

IE_NAMESPACE_END(util);
