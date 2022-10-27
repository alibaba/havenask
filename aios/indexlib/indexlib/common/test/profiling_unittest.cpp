#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/profiling.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(common);

class ProfilingTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(ProfilingTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForScopedThreadProfileTime()
    {
        {
            SCOPED_THREAD_PROFILE_TIME(ProfilingTest, ScopedThreadProfileTime);
            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 100000;
            nanosleep(&ts, NULL);
        }
        
        util::UnsafeMetrics& m1 = THREAD_PROFILE_METRICS(ProfilingTest,
                ScopedThreadProfileTime);
#if defined(INDEXLIB_PROFILE_BASIC) || defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_TRUE(m1.GetValue() > 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() > 0);
#else
        INDEXLIB_TEST_TRUE(m1.GetValue() == 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() == 0);
#endif
    }

    void TestCaseForScopedProcessProfileTime()
    {
        {
            SCOPED_PROCESS_PROFILE_TIME(ProfilingTest, ScopedProcessProfileTime);
            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 100000;
            nanosleep(&ts, NULL);
        }
        
        util::Metrics& m1 = PROCESS_PROFILE_METRICS(ProfilingTest,
                ScopedProcessProfileTime);
#if defined(INDEXLIB_PROFILE_BASIC) || defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_TRUE(m1.GetValue() > 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() > 0);
#else
        INDEXLIB_TEST_TRUE(m1.GetValue() == 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() == 0);
#endif
    }

    void TestCaseForEnterThreadProfileTime()
    {
        {
            ENTER_THREAD_PROFILE_TIME(ProfilingTest, EnterThreadProfileTime);
            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 100000;
            nanosleep(&ts, NULL);
            LEAVE_THREAD_PROFILE_TIME(ProfilingTest, EnterThreadProfileTime);
        }
        
        util::UnsafeMetrics& m1 = THREAD_PROFILE_METRICS(ProfilingTest,
                EnterThreadProfileTime);
#if defined(INDEXLIB_PROFILE_BASIC) || defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_TRUE(m1.GetValue() > 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() > 0);
#else
        INDEXLIB_TEST_TRUE(m1.GetValue() == 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() == 0);
#endif
    }

    void TestCaseForEnterProcessProfileTime()
    {
        {
            ENTER_PROCESS_PROFILE_TIME(ProfilingTest, EnterProcessProfileTime);
            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 100000;
            nanosleep(&ts, NULL);
            LEAVE_PROCESS_PROFILE_TIME(ProfilingTest, EnterProcessProfileTime);
        }
        
        util::Metrics& m1 = PROCESS_PROFILE_METRICS(ProfilingTest,
                EnterProcessProfileTime);
#if defined(INDEXLIB_PROFILE_BASIC) || defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_TRUE(m1.GetValue() > 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() > 0);
#else
        INDEXLIB_TEST_TRUE(m1.GetValue() == 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() == 0);
#endif
    }

    void TestCaseForThreadProfileCount()
    {
        THREAD_PROFILE_COUNT(ProfilingTest, ThreadProfileCount, 100);

        util::UnsafeMetrics& m1 = THREAD_PROFILE_METRICS(ProfilingTest,
                ThreadProfileCount);
#if defined(INDEXLIB_PROFILE_BASIC) || defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((int64_t)100, m1.GetValue());
        INDEXLIB_TEST_EQUAL((int64_t)1, m1.GetCount() > 0);
#else
        INDEXLIB_TEST_TRUE(m1.GetValue() == 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() == 0);
#endif  
    }

    void TestCaseForProcessProfileCount()
    {
        PROCESS_PROFILE_COUNT(ProfilingTest, ProcessProfileCount, 100);

        util::Metrics& m1 = PROCESS_PROFILE_METRICS(ProfilingTest,
                ProcessProfileCount);
#if defined(INDEXLIB_PROFILE_BASIC) || defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((int64_t)100, m1.GetValue());
        INDEXLIB_TEST_EQUAL((int64_t)1, m1.GetCount() > 0);
#else
        INDEXLIB_TEST_TRUE(m1.GetValue() == 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() == 0);
#endif  
    }

    void TestCaseForThreadProfilePairValue()
    {
        THREAD_PROFILE_PAIR_VALUE(ProfilingTest, ThreadProfilePairValue, "key", 100);

        util::UnsafePairedVectorMetrics& m1 = THREAD_PROFILE_PAIRED_METRICS(ProfilingTest,
                ThreadProfilePairValue);
#if defined(INDEXLIB_PROFILE_BASIC) || defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((size_t)1, m1.Size());

        const pair<string, int64_t>& value = m1.GetValue(0);
        INDEXLIB_TEST_EQUAL(string("key"), value.first);
        INDEXLIB_TEST_EQUAL((int64_t)(100), value.second);
#else
        INDEXLIB_TEST_EQUAL((size_t)0, m1.Size());
#endif  
    }

    void TestCaseForProcessProfilePairValue()
    {
        PROCESS_PROFILE_PAIR_VALUE(ProfilingTest, ProcessProfilePairValue, "key", 100);

        util::PairedVectorMetrics& m1 = PROCESS_PROFILE_PAIRED_METRICS(ProfilingTest,
                ProcessProfilePairValue);
#if defined(INDEXLIB_PROFILE_BASIC) || defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((size_t)1, m1.Size());

        const std::pair<std::string, int64_t>& value = m1.GetValue(0);
        INDEXLIB_TEST_EQUAL(string("key"), value.first);
        INDEXLIB_TEST_EQUAL((int64_t)(100), value.second);
#else
        INDEXLIB_TEST_EQUAL((size_t)0, m1.Size());
#endif  
    }

    void TestCaseForThreadProfileInt32Value()
    {
        THREAD_PROFILE_INT32_VALUE(ProfilingTest, ThreadProfileInt32Value, 100);

        util::UnsafeInt32VectorMetrics& m1 = THREAD_PROFILE_INT32_METRICS(ProfilingTest,
                ThreadProfileInt32Value);
#if defined(INDEXLIB_PROFILE_BASIC) || defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((size_t)1, m1.Size());

        int32_t value = m1.GetValue(0);
        INDEXLIB_TEST_EQUAL((int32_t)(100), value);
#else
        INDEXLIB_TEST_EQUAL((size_t)0, m1.Size());
#endif  
    }

    void TestCaseForProcessProfileInt32Value()
    {
        PROCESS_PROFILE_INT32_VALUE(ProfilingTest, ProcessProfileInt32Value, 100);

        util::Int32VectorMetrics& m1 = PROCESS_PROFILE_INT32_METRICS(ProfilingTest,
                ProcessProfileInt32Value);
#if defined(INDEXLIB_PROFILE_BASIC) || defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((size_t)1, m1.Size());

        int32_t value = m1.GetValue(0);
        INDEXLIB_TEST_EQUAL((int32_t)(100), value);
#else
        INDEXLIB_TEST_EQUAL((size_t)0, m1.Size());
#endif  
    }

    void TestCaseForThreadProfileInt64Value()
    {
        THREAD_PROFILE_INT64_VALUE(ProfilingTest, ThreadProfileInt64Value, 100);

        util::UnsafeInt64VectorMetrics& m1 = THREAD_PROFILE_INT64_METRICS(ProfilingTest,
                ThreadProfileInt64Value);
#if defined(INDEXLIB_PROFILE_BASIC) || defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((size_t)1, m1.Size());

        int64_t value = m1.GetValue(0);
        INDEXLIB_TEST_EQUAL((int64_t)(100), value);
#else
        INDEXLIB_TEST_EQUAL((size_t)0, m1.Size());
#endif  
    }

    void TestCaseForProcessProfileInt64Value()
    {
        PROCESS_PROFILE_INT64_VALUE(ProfilingTest, ProcessProfileInt64Value, 100);

        util::Int64VectorMetrics& m1 = PROCESS_PROFILE_INT64_METRICS(ProfilingTest,
                ProcessProfileInt64Value);
#if defined(INDEXLIB_PROFILE_BASIC) || defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((size_t)1, m1.Size());

        int64_t value = m1.GetValue(0);
        INDEXLIB_TEST_EQUAL((int64_t)(100), value);
#else
        INDEXLIB_TEST_EQUAL((size_t)0, m1.Size());
#endif  
    }

    void TestCaseForThreadProfileStringValue()
    {
        THREAD_PROFILE_STRING_VALUE(ProfilingTest, ThreadProfileStringValue, "str_value");

        util::UnsafeStrVectorMetrics& m1 = THREAD_PROFILE_STRING_METRICS(ProfilingTest,
                ThreadProfileStringValue);
#if defined(INDEXLIB_PROFILE_BASIC) || defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((size_t)1, m1.Size());

        const string& value = m1.GetValue(0);
        INDEXLIB_TEST_EQUAL(string("str_value"), value);
#else
        INDEXLIB_TEST_EQUAL((size_t)0, m1.Size());
#endif  
    }

    void TestCaseForProcessProfileStringValue()
    {
        PROCESS_PROFILE_STRING_VALUE(ProfilingTest, ProcessProfileStringValue, "str_value");

        util::StrVectorMetrics& m1 = PROCESS_PROFILE_STRING_METRICS(ProfilingTest,
                ProcessProfileStringValue);
#if defined(INDEXLIB_PROFILE_BASIC) || defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((size_t)1, m1.Size());

        const string& value = m1.GetValue(0);
        INDEXLIB_TEST_EQUAL(string("str_value"), value);
#else
        INDEXLIB_TEST_EQUAL((size_t)0, m1.Size());
#endif  
    }

    void TestCaseForScopedThreadProfileTimeTrace()
    {
        {
            SCOPED_THREAD_PROFILE_TIME_TRACE(ProfilingTest, ScopedThreadProfileTimeTrace);
            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 100000;
            nanosleep(&ts, NULL);
        }
        
        util::UnsafeMetrics& m1 = THREAD_PROFILE_METRICS(ProfilingTest,
                ScopedThreadProfileTimeTrace);
#if defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_TRUE(m1.GetValue() > 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() > 0);
#else
        INDEXLIB_TEST_TRUE(m1.GetValue() == 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() == 0);
#endif
    }

    void TestCaseForScopedProcessProfileTimeTrace()
    {
        {
            SCOPED_PROCESS_PROFILE_TIME_TRACE(ProfilingTest, ScopedProcessProfileTimeTrace);
            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 100000;
            nanosleep(&ts, NULL);
        }
        
        util::Metrics& m1 = PROCESS_PROFILE_METRICS(ProfilingTest,
                ScopedProcessProfileTimeTrace);
#if defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_TRUE(m1.GetValue() > 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() > 0);
#else
        INDEXLIB_TEST_TRUE(m1.GetValue() == 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() == 0);
#endif
    }

    void TestCaseForEnterThreadProfileTimeTrace()
    {
        {
            ENTER_THREAD_PROFILE_TIME_TRACE(ProfilingTest, EnterThreadProfileTimeTrace);
            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 100000;
            nanosleep(&ts, NULL);
            LEAVE_THREAD_PROFILE_TIME_TRACE(ProfilingTest, EnterThreadProfileTimeTrace);
        }
        
        util::UnsafeMetrics& m1 = THREAD_PROFILE_METRICS(ProfilingTest,
                EnterThreadProfileTimeTrace);
#if defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_TRUE(m1.GetValue() > 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() > 0);
#else
        INDEXLIB_TEST_TRUE(m1.GetValue() == 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() == 0);
#endif
    }

    void TestCaseForEnterProcessProfileTimeTrace()
    {
        {
            ENTER_PROCESS_PROFILE_TIME_TRACE(ProfilingTest, EnterProcessProfileTimeTrace);
            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 100000;
            nanosleep(&ts, NULL);
            LEAVE_PROCESS_PROFILE_TIME_TRACE(ProfilingTest, EnterProcessProfileTimeTrace);
        }
        
        util::Metrics& m1 = PROCESS_PROFILE_METRICS(ProfilingTest,
                EnterProcessProfileTimeTrace);
#if defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_TRUE(m1.GetValue() > 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() > 0);
#else
        INDEXLIB_TEST_TRUE(m1.GetValue() == 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() == 0);
#endif
    }

    void TestCaseForThreadProfileCountTrace()
    {
        THREAD_PROFILE_COUNT_TRACE(ProfilingTest, ThreadProfileCountTrace, 100);

        util::UnsafeMetrics& m1 = THREAD_PROFILE_METRICS(ProfilingTest,
                ThreadProfileCountTrace);
#if defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((int64_t)100, m1.GetValue());
        INDEXLIB_TEST_EQUAL((int64_t)1, m1.GetCount() > 0);
#else
        INDEXLIB_TEST_TRUE(m1.GetValue() == 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() == 0);
#endif  
    }

    void TestCaseForProcessProfileCountTrace()
    {
        PROCESS_PROFILE_COUNT_TRACE(ProfilingTest, ProcessProfileCountTrace, 100);

        util::Metrics& m1 = PROCESS_PROFILE_METRICS(ProfilingTest,
                ProcessProfileCountTrace);
#if defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((int64_t)100, m1.GetValue());
        INDEXLIB_TEST_EQUAL((int64_t)1, m1.GetCount() > 0);
#else
        INDEXLIB_TEST_TRUE(m1.GetValue() == 0);
        INDEXLIB_TEST_TRUE(m1.GetCount() == 0);
#endif  
    }

    void TestCaseForThreadProfilePairValueTrace()
    {
        THREAD_PROFILE_PAIR_VALUE_TRACE(ProfilingTest, ThreadProfilePairValueTrace, "key", 100);

        util::UnsafePairedVectorMetrics& m1 = THREAD_PROFILE_PAIRED_METRICS(ProfilingTest,
                ThreadProfilePairValueTrace);
#if defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((size_t)1, m1.Size());

        const pair<string, int64_t>& value = m1.GetValue(0);
        INDEXLIB_TEST_EQUAL(string("key"), value.first);
        INDEXLIB_TEST_EQUAL((int64_t)(100), value.second);
#else
        INDEXLIB_TEST_EQUAL((size_t)0, m1.Size());
#endif  
    }

    void TestCaseForProcessProfilePairValueTrace()
    {
        PROCESS_PROFILE_PAIR_VALUE_TRACE(ProfilingTest, ProcessProfilePairValueTrace, "key", 100);

        util::PairedVectorMetrics& m1 = PROCESS_PROFILE_PAIRED_METRICS(ProfilingTest,
                ProcessProfilePairValueTrace);
#if defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((size_t)1, m1.Size());

        const std::pair<std::string, int64_t>& value = m1.GetValue(0);
        INDEXLIB_TEST_EQUAL(string("key"), value.first);
        INDEXLIB_TEST_EQUAL((int64_t)(100), value.second);
#else
        INDEXLIB_TEST_EQUAL((size_t)0, m1.Size());
#endif  
    }

    void TestCaseForThreadProfileInt32ValueTrace()
    {
        THREAD_PROFILE_INT32_VALUE_TRACE(ProfilingTest, ThreadProfileInt32ValueTrace, 100);

        util::UnsafeInt32VectorMetrics& m1 = THREAD_PROFILE_INT32_METRICS(ProfilingTest,
                ThreadProfileInt32ValueTrace);
#if defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((size_t)1, m1.Size());

        int32_t value = m1.GetValue(0);
        INDEXLIB_TEST_EQUAL((int32_t)(100), value);
#else
        INDEXLIB_TEST_EQUAL((size_t)0, m1.Size());
#endif  
    }

    void TestCaseForProcessProfileInt32ValueTrace()
    {
        PROCESS_PROFILE_INT32_VALUE_TRACE(ProfilingTest, ProcessProfileInt32ValueTrace, 100);

        util::Int32VectorMetrics& m1 = PROCESS_PROFILE_INT32_METRICS(ProfilingTest,
                ProcessProfileInt32ValueTrace);
#if defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((size_t)1, m1.Size());

        int32_t value = m1.GetValue(0);
        INDEXLIB_TEST_EQUAL((int32_t)(100), value);
#else
        INDEXLIB_TEST_EQUAL((size_t)0, m1.Size());
#endif  
    }

    void TestCaseForThreadProfileInt64ValueTrace()
    {
        THREAD_PROFILE_INT64_VALUE_TRACE(ProfilingTest, ThreadProfileInt64ValueTrace, 100);

        util::UnsafeInt64VectorMetrics& m1 = THREAD_PROFILE_INT64_METRICS(ProfilingTest,
                ThreadProfileInt64ValueTrace);
#if defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((size_t)1, m1.Size());

        int64_t value = m1.GetValue(0);
        INDEXLIB_TEST_EQUAL((int64_t)(100), value);
#else
        INDEXLIB_TEST_EQUAL((size_t)0, m1.Size());
#endif  
    }

    void TestCaseForProcessProfileInt64ValueTrace()
    {
        PROCESS_PROFILE_INT64_VALUE_TRACE(ProfilingTest, ProcessProfileInt64ValueTrace, 100);

        util::Int64VectorMetrics& m1 = PROCESS_PROFILE_INT64_METRICS(ProfilingTest,
                ProcessProfileInt64ValueTrace);
#if defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((size_t)1, m1.Size());

        int64_t value = m1.GetValue(0);
        INDEXLIB_TEST_EQUAL((int64_t)(100), value);
#else
        INDEXLIB_TEST_EQUAL((size_t)0, m1.Size());
#endif  
    }

    void TestCaseForThreadProfileStringValueTrace()
    {
        THREAD_PROFILE_STRING_VALUE_TRACE(ProfilingTest, ThreadProfileStringValueTrace, "str_value");

        util::UnsafeStrVectorMetrics& m1 = THREAD_PROFILE_STRING_METRICS(ProfilingTest,
                ThreadProfileStringValueTrace);
#if defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((size_t)1, m1.Size());

        const string& value = m1.GetValue(0);
        INDEXLIB_TEST_EQUAL(string("str_value"), value);
#else
        INDEXLIB_TEST_EQUAL((size_t)0, m1.Size());
#endif  
    }

    void TestCaseForProcessProfileStringValueTrace()
    {
        PROCESS_PROFILE_STRING_VALUE_TRACE(ProfilingTest, ProcessProfileStringValueTrace, "str_value");

        util::StrVectorMetrics& m1 = PROCESS_PROFILE_STRING_METRICS(ProfilingTest,
                ProcessProfileStringValueTrace);
#if defined(INDEXLIB_PROFILE_TRACE)
        INDEXLIB_TEST_EQUAL((size_t)1, m1.Size());

        const string& value = m1.GetValue(0);
        INDEXLIB_TEST_EQUAL(string("str_value"), value);
#else
        INDEXLIB_TEST_EQUAL((size_t)0, m1.Size());
#endif  
    }
    
private:
};

INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForScopedThreadProfileTime);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForScopedProcessProfileTime);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForEnterThreadProfileTime);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForEnterProcessProfileTime);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForThreadProfileCount);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForProcessProfileCount);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForThreadProfilePairValue);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForProcessProfilePairValue);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForThreadProfileInt32Value);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForProcessProfileInt32Value);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForThreadProfileInt64Value);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForProcessProfileInt64Value);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForThreadProfileStringValue);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForProcessProfileStringValue);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForScopedThreadProfileTimeTrace);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForScopedProcessProfileTimeTrace);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForEnterThreadProfileTimeTrace);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForEnterProcessProfileTimeTrace);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForThreadProfileCountTrace);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForProcessProfileCountTrace);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForThreadProfilePairValueTrace);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForProcessProfilePairValueTrace);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForThreadProfileInt32ValueTrace);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForProcessProfileInt32ValueTrace);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForThreadProfileInt64ValueTrace);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForProcessProfileInt64ValueTrace);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForThreadProfileStringValueTrace);
INDEXLIB_UNIT_TEST_CASE(ProfilingTest, TestCaseForProcessProfileStringValueTrace);

IE_NAMESPACE_END(common);
