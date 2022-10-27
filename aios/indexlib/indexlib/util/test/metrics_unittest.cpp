#define INDEXLIB_PROFILE_TRACE

#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/metrics_center.h"
#include "indexlib/util/profiling.h"
#include <set>
#include <autil/Thread.h>

using namespace std;
using namespace autil::legacy;
using namespace autil;

IE_NAMESPACE_BEGIN(util);

bool MySortPredicate(const pair<string, int64_t>& m1, const pair<string, int64_t>& m2)
{
    return m1.second < m2.second;
}


class MetricsTest : public INDEXLIB_TESTBASE
{
public:
    
    DECLARE_CLASS_NAME(MetricsTest);
    void CaseSetUp() override
    {
        mAddValuePerThread = 10;
        mAddCountPerThread = 20000;
    }
    void CaseTearDown() override
    {
    }

    int32_t FuncGetMetricsWithResetThread()
    {
        for (int64_t i = 0; i < mAddCountPerThread; i++)
        {
            THREAD_PROFILE_COUNT(test, TestMetrics, mAddValuePerThread);
        }
                
        assert(THREAD_PROFILE_METRICS(test, TestMetrics).GetValue()
               == mAddCountPerThread * mAddValuePerThread);
        if (THREAD_PROFILE_METRICS(test, TestMetrics).GetValue()  
            != mAddCountPerThread * mAddValuePerThread)
        {
            throw;
        }

        assert(THREAD_PROFILE_METRICS(test, TestMetrics).GetCount()
               == mAddCountPerThread);
        if (THREAD_PROFILE_METRICS(test, TestMetrics).GetCount()
            != mAddCountPerThread)
        {
            throw;
        }
        
        //To avoid threadId reuse(such as thread pool of apsara),
        //you need to call Reset function before thread finish.
        MetricsCenter::GetInstance()->ResetCurrentThread();
        return 0;
    }

    int32_t UnsafePairedVectorMetricsWithResetFunc()
    {
        for (int64_t i = 0; i < mAddCountPerThread; i++)
        {
            stringstream ss;
            ss << "test_value" << i;
            THREAD_PROFILE_PAIR_VALUE(test, TestMetrics, ss.str(), i);
        }

        UnsafePairedVectorMetrics& vm = THREAD_PROFILE_PAIRED_METRICS(test, TestMetrics);
        assert((size_t)mAddCountPerThread == vm.Size());
        if ((size_t)mAddCountPerThread != vm.Size())
        {
            throw;
        }

        for (size_t j = 0; j < vm.Size(); j++)
        {
            stringstream ss;
            ss << "test_value" << j;
            assert(ss.str() == vm.GetValue(j).first);
            if (ss.str() != vm.GetValue(j).first)
            {
                throw;
            }
            assert(j == (size_t)vm.GetValue(j).second);
            if (j != (size_t)vm.GetValue(j).second)
            {
                throw;
            }
        }
        
        //To avoid threadId reuse(such as thread pool of apsara),
        //you need to call Reset function before thread finish.
        MetricsCenter::GetInstance()->ResetCurrentThread();
        return 0;
    }

    int32_t FuncGetMetricsWithoutResetThreadUsingThreadProfiling()
    {
        for (int64_t i = 0; i < mAddCountPerThread; i++)
        {
            THREAD_PROFILE_COUNT(test, TestThreadMetrics, mAddValuePerThread);
        }
        return 0;
    }

    int32_t FuncGetMetricsWithoutResetThreadUsingProcessProfiling()
    {
        for (int64_t i = 0; i < mAddCountPerThread; i++)
        {
            PROCESS_PROFILE_COUNT(test, TestProcessMetrics, mAddValuePerThread);
        }
        return 0;
    }

    void TestCaseForUnsafeMetrics()
    {
        std::vector<autil::ThreadPtr> pids;
        int threadCount = 10;

        for (int i = 0; i < threadCount; i++)
        {
            autil::ThreadPtr p = autil::Thread::createThread(
                    std::tr1::bind(&MetricsTest::FuncGetMetricsWithResetThread, this));
            pids.push_back(p);
        }

        for (size_t i = 0; i < pids.size(); i++)
        {
            pids[i]->join();
        }

    }

    void TestCaseForUnsafePairedVectorMetrics()
    {
        std::vector<autil::ThreadPtr> pids;
        int threadCount = 10;

        for (int i = 0; i < threadCount; i++)
        {
            autil::ThreadPtr p = autil::Thread::createThread(
                    std::tr1::bind(&MetricsTest::UnsafePairedVectorMetricsWithResetFunc, this));
            pids.push_back(p);
        }

        for (uint32_t i = 0; i < pids.size(); i++)
        {
            pids[i]->join();
        }
    }

    int32_t PairedVectorMetricsWithResetFunc()
    {
        for (int64_t i = 0; i < mAddCountPerThread; i++)
        {
            stringstream ss;
            ss << "test_value" << i;
            PROCESS_PROFILE_PAIR_VALUE(test, TestMetrics, ss.str(), i);
        }
        return 0;
    }

    void TestCaseForPairedVectorMetricsUsingProcessProfiling()
    {
        std::vector<autil::ThreadPtr> pids;
        size_t threadCount = 5;

        for (size_t i = 0; i < threadCount; i++)
        {
            autil::ThreadPtr p = autil::Thread::createThread(
                    std::tr1::bind(&MetricsTest::PairedVectorMetricsWithResetFunc, this));
            pids.push_back(p);
        }

        for (size_t i = 0; i < pids.size(); i++)
        {
            pids[i]->join();
        }

        PairedVectorMetrics& pairedMetrics = PROCESS_PROFILE_PAIRED_METRICS(test, TestMetrics);
        PairedVectorMetrics::ValueVector v = pairedMetrics.GetVector();

        INDEXLIB_TEST_EQUAL((size_t)threadCount * mAddCountPerThread, v.size());

        std::sort(v.begin(), v.end(), MySortPredicate);

        size_t i = 0;
        while (i < v.size())
        {
            for (int64_t k = 0; k < mAddCountPerThread; k++)
            {
                for (size_t j = 0; j < threadCount; ++j)
                {
                    stringstream ss;
                    ss << "test_value" << k;
                    INDEXLIB_TEST_EQUAL(ss.str(), v[i].first);
                    INDEXLIB_TEST_EQUAL(k, v[i].second);
                    ++i;
                }
            }
        }
    }

    void TestCaseForGetTotalDataUsingThreadProfiling()
    {
        std::vector<autil::ThreadPtr> pids;
        size_t threadCount = 10;
        
        for (size_t i = 0; i < threadCount; i++)
        {
            autil::ThreadPtr p = autil::Thread::createThread(
                    std::tr1::bind(&MetricsTest::FuncGetMetricsWithoutResetThreadUsingThreadProfiling, this));
            pids.push_back(p);
        }

        for (size_t i = 0; i < pids.size(); i++)
        {
            pids[i]->join();
        }
        int64_t totalCount = MetricsCenter::GetInstance()
                             ->GetTotalCountByMetricsName("test/TestThreadMetrics");
        int64_t totalValue = MetricsCenter::GetInstance()
                             ->GetTotalValueByMetricsName("test/TestThreadMetrics");

        INDEXLIB_TEST_EQUAL(totalCount, (int64_t)threadCount * mAddCountPerThread);
        INDEXLIB_TEST_EQUAL(totalValue, (int64_t)threadCount * mAddCountPerThread * mAddValuePerThread);
    }

    void TestCaseForGetTotalDataUsingProcessProfiling()
    {
        std::vector<autil::ThreadPtr> pids;
        size_t threadCount = 10;
        
        for (size_t i = 0; i < threadCount; i++)
        {
            autil::ThreadPtr p = autil::Thread::createThread(
                    std::tr1::bind(&MetricsTest::FuncGetMetricsWithoutResetThreadUsingProcessProfiling, this));
            pids.push_back(p);
        }

        for (size_t i = 0; i < pids.size(); i++)
        {
            pids[i]->join();
        }
        int64_t totalCount = MetricsCenter::GetInstance()
                             ->GetTotalCountByMetricsName("test/TestProcessMetrics");
        int64_t totalValue = MetricsCenter::GetInstance()
                             ->GetTotalValueByMetricsName("test/TestProcessMetrics");

        INDEXLIB_TEST_EQUAL(totalCount, (int64_t)threadCount * mAddCountPerThread);
        INDEXLIB_TEST_EQUAL(totalValue, (int64_t)threadCount * mAddCountPerThread * mAddValuePerThread);
    }
    

private:
    int64_t mAddValuePerThread;
    int64_t mAddCountPerThread;
};

INDEXLIB_UNIT_TEST_CASE(MetricsTest, TestCaseForUnsafeMetrics);
INDEXLIB_UNIT_TEST_CASE(MetricsTest, TestCaseForUnsafePairedVectorMetrics);
INDEXLIB_UNIT_TEST_CASE(MetricsTest, TestCaseForPairedVectorMetricsUsingProcessProfiling);
INDEXLIB_UNIT_TEST_CASE(MetricsTest, TestCaseForGetTotalDataUsingProcessProfiling);
INDEXLIB_UNIT_TEST_CASE(MetricsTest, TestCaseForGetTotalDataUsingThreadProfiling);

IE_NAMESPACE_END(util);

#undef INDEXLIB_PROFILE_TRACE
