#ifndef __INDEXLIB_PROFILING_H
#define __INDEXLIB_PROFILING_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "autil/TimeUtility.h"
#include "indexlib/util/metrics_center.h"
#include "indexlib/util/timer.h"

IE_NAMESPACE_BEGIN(util);

#if defined(INDEXLIB_PROFILE_BASIC) || defined(INDEXLIB_PROFILE_TRACE)

template <typename T>
class ScopedProfiler
{
public:
    ScopedProfiler(T& metrics)
        : mMetrics(metrics)
    {
        mTimer.Start();
    }

    ~ScopedProfiler()
    {
        mMetrics.AddValue(mTimer.Stop());
    }

private:
    T& mMetrics;
    util::Timer mTimer;
};


////////////////////////////////////////////////////////////////
///Basic metrics

///time metrics for thread-local
#define SCOPED_THREAD_PROFILE_TIME(moduleName, metricsName)                  \
    IE_NAMESPACE(util)::UnsafeMetrics& moduleName##couterName = IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetThreadLocalMetrics<IE_NAMESPACE(util)::UnsafeMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName)); \
    IE_NAMESPACE(util)::ScopedProfiler<IE_NAMESPACE(util)::UnsafeMetrics> sp_##metricsName(moduleName##couterName)

#define ENTER_THREAD_PROFILE_TIME(moduleName, metricsName)              \
    IE_NAMESPACE(util)::UnsafeMetrics& moduleName##metricsName = IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetThreadLocalMetrics<IE_NAMESPACE(util)::UnsafeMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName)); \
    util::Timer timer_##metricsName;

#define LEAVE_THREAD_PROFILE_TIME(moduleName, metricsName)              \
    moduleName##metricsName.AddValue(timer_##metricsName.Stop());

///counter metrics for thread-local
#define THREAD_PROFILE_COUNT(moduleName, metricsName, value)            \
    IE_NAMESPACE(util)::UnsafeMetrics& moduleName##metricsName = IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetThreadLocalMetrics<IE_NAMESPACE(util)::UnsafeMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName)); \
    moduleName##metricsName.AddValue(value);

#define THREAD_PROFILE_METRICS(moduleName, metricsName)                 \
    IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetThreadLocalMetrics<IE_NAMESPACE(util)::UnsafeMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName))

///time metrics for process
#define SCOPED_PROCESS_PROFILE_TIME(moduleName, metricsName)                 \
    static IE_NAMESPACE(util)::Metrics& moduleName##metricsName = IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetProcessMetrics<IE_NAMESPACE(util)::Metrics>(std::string(#moduleName) + "/" + std::string(#metricsName)); \
    IE_NAMESPACE(util)::ScopedProfiler<IE_NAMESPACE(util)::Metrics> sp_##metricsName(moduleName##metricsName)

#define ENTER_PROCESS_PROFILE_TIME(moduleName, metricsName)                  \
    static IE_NAMESPACE(util)::Metrics& moduleName##metricsName = IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetProcessMetrics<IE_NAMESPACE(util)::Metrics>(std::string(#moduleName) + "/" + std::string(#metricsName)); \
    util::Timer timer_##metricsName;

#define LEAVE_PROCESS_PROFILE_TIME(moduleName, metricsName)             \
    moduleName##metricsName.AddValue(timer_##metricsName.Stop())

///counter metrics for process
#define PROCESS_PROFILE_COUNT(moduleName, metricsName, value)      \
    static IE_NAMESPACE(util)::Metrics& moduleName##metricsName = IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetProcessMetrics<IE_NAMESPACE(util)::Metrics>(std::string(#moduleName) + "/" + std::string(#metricsName)); \
    moduleName##metricsName.AddValue(value)

#define PROCESS_PROFILE_METRICS(moduleName, metricsName)                \
    IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetProcessMetrics<IE_NAMESPACE(util)::Metrics>(std::string(#moduleName) + "/" + std::string(#metricsName))

////////////////////////////////////////////////////////////////
///Paired value metrics

///Paired value metrics for thread-local
#define THREAD_PROFILE_PAIR_VALUE(moduleName, metricsName, key, value)   \
    IE_NAMESPACE(util)::UnsafePairedVectorMetrics& moduleName##metricsName = IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetThreadLocalMetrics<IE_NAMESPACE(util)::UnsafePairedVectorMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName)); \
    moduleName##metricsName.PushBack(make_pair(key, value));

#define THREAD_PROFILE_PAIRED_METRICS(moduleName, metricsName) \
    IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetThreadLocalMetrics<IE_NAMESPACE(util)::UnsafePairedVectorMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName))

///Paired value metrics for process
#define PROCESS_PROFILE_PAIR_VALUE(moduleName, metricsName, key, value) \
    static IE_NAMESPACE(util)::PairedVectorMetrics& moduleName##metricsName = IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetProcessMetrics<IE_NAMESPACE(util)::PairedVectorMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName)); \
    moduleName##metricsName.PushBack(make_pair(key, value));

#define PROCESS_PROFILE_PAIRED_METRICS(moduleName, metricsName) \
    IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetProcessMetrics<IE_NAMESPACE(util)::PairedVectorMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName))


////////////////////////////////////////////////////////////////
///Int32 value metrics

///Int32 value metrics for thread-local
#define THREAD_PROFILE_INT32_VALUE(moduleName, metricsName, value)   \
    IE_NAMESPACE(util)::UnsafeInt32VectorMetrics& moduleName##metricsName = IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetThreadLocalMetrics<IE_NAMESPACE(util)::UnsafeInt32VectorMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName)); \
    moduleName##metricsName.PushBack(value);

#define THREAD_PROFILE_INT32_METRICS(moduleName, metricsName) \
    IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetThreadLocalMetrics<IE_NAMESPACE(util)::UnsafeInt32VectorMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName))

///Int32 value metrics for process
#define PROCESS_PROFILE_INT32_VALUE(moduleName, metricsName, value) \
    static IE_NAMESPACE(util)::Int32VectorMetrics& moduleName##metricsName = IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetProcessMetrics<IE_NAMESPACE(util)::Int32VectorMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName)); \
    moduleName##metricsName.PushBack(value);

#define PROCESS_PROFILE_INT32_METRICS(moduleName, metricsName)    \
    IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetProcessMetrics<IE_NAMESPACE(util)::Int32VectorMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName))


////////////////////////////////////////////////////////////////
///Int64 value metrics

///Int64 value metrics for thread-local
#define THREAD_PROFILE_INT64_VALUE(moduleName, metricsName, value)   \
    IE_NAMESPACE(util)::UnsafeInt64VectorMetrics& moduleName##metricsName = IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetThreadLocalMetrics<IE_NAMESPACE(util)::UnsafeInt64VectorMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName)); \
    moduleName##metricsName.PushBack(value);

#define THREAD_PROFILE_INT64_METRICS(moduleName, metricsName) \
    IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetThreadLocalMetrics<IE_NAMESPACE(util)::UnsafeInt64VectorMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName))

///Int64 value metrics for process
#define PROCESS_PROFILE_INT64_VALUE(moduleName, metricsName, value) \
    static IE_NAMESPACE(util)::Int64VectorMetrics& moduleName##metricsName = IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetProcessMetrics<IE_NAMESPACE(util)::Int64VectorMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName)); \
    moduleName##metricsName.PushBack(value);

#define PROCESS_PROFILE_INT64_METRICS(moduleName, metricsName)    \
    IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetProcessMetrics<IE_NAMESPACE(util)::Int64VectorMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName))

////////////////////////////////////////////////////////////////
///String value metrics

///String value metrics for thread-local
#define THREAD_PROFILE_STRING_VALUE(moduleName, metricsName, value)   \
    IE_NAMESPACE(util)::UnsafeStrVectorMetrics& moduleName##metricsName = IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetThreadLocalMetrics<IE_NAMESPACE(util)::UnsafeStrVectorMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName)); \
    moduleName##metricsName.PushBack(value);

#define THREAD_PROFILE_STRING_METRICS(moduleName, metricsName) \
    IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetThreadLocalMetrics<IE_NAMESPACE(util)::UnsafeStrVectorMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName))


///String value metrics for process
#define PROCESS_PROFILE_STRING_VALUE(moduleName, metricsName, value) \
    static IE_NAMESPACE(util)::StrVectorMetrics& moduleName##metricsName = IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetProcessMetrics<IE_NAMESPACE(util)::StrVectorMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName)); \
    moduleName##metricsName.PushBack(value);

#define PROCESS_PROFILE_STRING_METRICS(moduleName, metricsName)   \
    IE_NAMESPACE(util)::MetricsCenter::GetInstance()->GetProcessMetrics<IE_NAMESPACE(util)::StrVectorMetrics>(std::string(#moduleName) + "/" + std::string(#metricsName))


#define BEGIN_PROFILE_SECTION(moduleName) {
#define END_PROFILE_SECTION(moduleName) }

#else // No profing

class NullMetrics
{
public:
    static util::Metrics& GetMetrics()
    {
        static util::Metrics EMPTY_METRICS("EMPTY_METRICS");
        return EMPTY_METRICS;
    }

    static util::UnsafeMetrics& GetUnsafeMetrics()
    {
        static util::UnsafeMetrics EMPTY_UNSAFE_METRICS("EMPTY_METRICS");
        return EMPTY_UNSAFE_METRICS;
    }

    static util::PairedVectorMetrics& GetPairedVectorMetrics()
    {
        static util::PairedVectorMetrics EMPTY_PAIRED_METRICS("EMPTY_PAIRED_METRICS");
        return EMPTY_PAIRED_METRICS;
    }

    static util::UnsafePairedVectorMetrics& GetUnsafePairedVectorMetrics()
    {
        static util::UnsafePairedVectorMetrics EMPTY_UNSAFE_PAIRED_METRICS("EMPTY_UNSAFE_PAIRED_METRICS");
        return  EMPTY_UNSAFE_PAIRED_METRICS;
    }

    static util::Int32VectorMetrics& GetInt32VectorMetrics()
    {
        static util::Int32VectorMetrics EMPTY_INT32_METRICS("EMPTY_INT32_METRICS");
        return EMPTY_INT32_METRICS;
    }

    static util::UnsafeInt32VectorMetrics& GetUnsafeInt32VectorMetrics()
    {
        static util::UnsafeInt32VectorMetrics EMPTY_UNSAFE_INT32_METRICS("EMPTY_UNSAFE_INT32_METRICS");
        return  EMPTY_UNSAFE_INT32_METRICS;
    }

    static util::Int64VectorMetrics& GetInt64VectorMetrics()
    {
        static util::Int64VectorMetrics EMPTY_INT64_METRICS("EMPTY_INT64_METRICS");
        return EMPTY_INT64_METRICS;
    }

    static util::UnsafeInt64VectorMetrics& GetUnsafeInt64VectorMetrics()
    {
        static util::UnsafeInt64VectorMetrics EMPTY_UNSAFE_INT64_METRICS("EMPTY_UNSAFE_INT64_METRICS");
        return  EMPTY_UNSAFE_INT64_METRICS;
    }

    static util::StrVectorMetrics& GetStrVectorMetrics()
    {
        static util::StrVectorMetrics EMPTY_STR_METRICS("EMPTY_STR_METRICS");
        return EMPTY_STR_METRICS;
    }

    static util::UnsafeStrVectorMetrics& GetUnsafeStrVectorMetrics()
    {
        static util::UnsafeStrVectorMetrics EMPTY_UNSAFE_STR_METRICS("EMPTY_UNSAFE_STR_METRICS");
        return  EMPTY_UNSAFE_STR_METRICS;
    }
};

////////////////////////////////////////////////////////////////
///Basic metrics

//time metrics for thread-local
#define SCOPED_THREAD_PROFILE_TIME(MetricsName, metricsName)
#define ENTER_THREAD_PROFILE_TIME(MetricsName, metricsName) 
#define LEAVE_THREAD_PROFILE_TIME(MetricsName, metricsName)

//counter metrics for thread-local
#define THREAD_PROFILE_COUNT(moduleName, metricsName, value)

#define THREAD_PROFILE_METRICS(moduleName, metricsName) IE_NAMESPACE(util)::NullMetrics::GetUnsafeMetrics()
#define PROCESS_PROFILE_METRICS(moduleName, metricsName) IE_NAMESPACE(util)::NullMetrics::GetMetrics()

//time metrics for process
#define SCOPED_PROCESS_PROFILE_TIME(MetricsName, metricsName)
#define ENTER_PROCESS_PROFILE_TIME(MetricsName, metricsName)
#define LEAVE_PROCESS_PROFILE_TIME(MetricsName, metricsName)

//counter metrics for process
#define PROCESS_PROFILE_COUNT(moduleName, metricsName, value)

////////////////////////////////////////////////////////////////
///Paired value metrics

///Paired value metrics for thread-local
#define THREAD_PROFILE_PAIR_VALUE(moduleName, metricsName, key, value)
#define THREAD_PROFILE_PAIRED_METRICS(moduleName, metricsName) IE_NAMESPACE(util)::NullMetrics::GetUnsafePairedVectorMetrics()

///Paired value metrics for process
#define PROCESS_PROFILE_PAIR_VALUE(moduleName, metricsName, key, value)
#define PROCESS_PROFILE_PAIRED_METRICS(moduleName, metricsName) IE_NAMESPACE(util)::NullMetrics::GetPairedVectorMetrics()

////////////////////////////////////////////////////////////////
///Int32 value metrics

//Int32 value metrics for thread-local
#define THREAD_PROFILE_INT32_VALUE(moduleName, metricsName, value)
#define THREAD_PROFILE_INT32_METRICS(moduleName, metricsName) IE_NAMESPACE(util)::NullMetrics::GetUnsafeInt32VectorMetrics()

//Int32 value metrics for process
#define PROCESS_PROFILE_INT32_VALUE(moduleName, metricsName, value) ;
#define PROCESS_PROFILE_INT32_METRICS(moduleName, metricsName) IE_NAMESPACE(util)::NullMetrics::GetInt32VectorMetrics()

////////////////////////////////////////////////////////////////
///Int64 value metrics

//Int64 value metrics for thread-local
#define THREAD_PROFILE_INT64_VALUE(moduleName, metricsName, value)
#define THREAD_PROFILE_INT64_METRICS(moduleName, metricsName) IE_NAMESPACE(util)::NullMetrics::GetUnsafeInt64VectorMetrics()

//Int64 value metrics for process
#define PROCESS_PROFILE_INT64_VALUE(moduleName, metricsName, value) ;
#define PROCESS_PROFILE_INT64_METRICS(moduleName, metricsName) IE_NAMESPACE(util)::NullMetrics::GetInt64VectorMetrics()

////////////////////////////////////////////////////////////////
///String value metrics

///String value metrics for thread-lcoal
#define THREAD_PROFILE_STRING_VALUE(moduleName, metricsName, value)
#define THREAD_PROFILE_STRING_METRICS(moduleName, metricsName) IE_NAMESPACE(util)::NullMetrics::GetUnsafeStrVectorMetrics()

///String value metrics for thread-lcoal
#define PROCESS_PROFILE_STRING_VALUE(moduleName, metricsName, value)
#define PROCESS_PROFILE_STRING_METRICS(moduleName, metricsName) IE_NAMESPACE(util)::NullMetrics::GetStrVectorMetrics()


#define BEGIN_PROFILE_SECTION(moduleName) while (0) {
#define END_PROFILE_SECTION(moduleName) }

#endif // defined(INDEXLIB_PROFILE_BASIC) || defined(INDEXLIB_PROFILE_TRACE)


#ifdef INDEXLIB_PROFILE_TRACE

///time metrics for thread-local
#define SCOPED_THREAD_PROFILE_TIME_TRACE(moduleName, metricsName) SCOPED_THREAD_PROFILE_TIME(moduleName, metricsName)
#define ENTER_THREAD_PROFILE_TIME_TRACE(moduleName, metricsName) ENTER_THREAD_PROFILE_TIME(moduleName, metricsName)
#define LEAVE_THREAD_PROFILE_TIME_TRACE(moduleName, metricsName) LEAVE_THREAD_PROFILE_TIME(moduleName, metricsName)

///counter metrics for thread-local
#define THREAD_PROFILE_COUNT_TRACE(moduleName, metricsName, value) THREAD_PROFILE_COUNT(moduleName, metricsName, value)

///time metrics for process
#define SCOPED_PROCESS_PROFILE_TIME_TRACE(moduleName, metricsName) SCOPED_PROCESS_PROFILE_TIME(moduleName, metricsName)
#define ENTER_PROCESS_PROFILE_TIME_TRACE(moduleName, metricsName) ENTER_PROCESS_PROFILE_TIME(moduleName, metricsName)
#define LEAVE_PROCESS_PROFILE_TIME_TRACE(moduleName, metricsName) LEAVE_PROCESS_PROFILE_TIME(moduleName, metricsName)

///counter metrics for process
#define PROCESS_PROFILE_COUNT_TRACE(moduleName, metricsName, value) PROCESS_PROFILE_COUNT(moduleName, metricsName, value)

////////////////////////////////////////////////////////////////
///Paired value metrics

///Paired value metrics for thread-local
#define THREAD_PROFILE_PAIR_VALUE_TRACE(moduleName, metricsName, key, value) THREAD_PROFILE_PAIR_VALUE(moduleName, metricsName, key, value)

///Paired value metrics for process
#define PROCESS_PROFILE_PAIR_VALUE_TRACE(moduleName, metricsName, key, value) PROCESS_PROFILE_PAIR_VALUE(moduleName, metricsName, key, value)

////////////////////////////////////////////////////////////////
///Int32 value metrics

///Int32 value metrics for thread-local
#define THREAD_PROFILE_INT32_VALUE_TRACE(moduleName, metricsName, value) THREAD_PROFILE_INT32_VALUE(moduleName, metricsName, value)

///Int32 value metrics for process
#define PROCESS_PROFILE_INT32_VALUE_TRACE(moduleName, metricsName, value) PROCESS_PROFILE_INT32_VALUE(moduleName, metricsName, value)

////////////////////////////////////////////////////////////////
///Int64 value metrics

///Int64 value metrics for thread-local
#define THREAD_PROFILE_INT64_VALUE_TRACE(moduleName, metricsName, value) THREAD_PROFILE_INT64_VALUE(moduleName, metricsName, value)

///Int64 value metrics for process
#define PROCESS_PROFILE_INT64_VALUE_TRACE(moduleName, metricsName, value) PROCESS_PROFILE_INT64_VALUE(moduleName, metricsName, value)

////////////////////////////////////////////////////////////////
///String value metrics

///String value metrics for thread-local
#define THREAD_PROFILE_STRING_VALUE_TRACE(moduleName, metricsName, value) THREAD_PROFILE_STRING_VALUE(moduleName, metricsName, value)

///String value metrics for process
#define PROCESS_PROFILE_STRING_VALUE_TRACE(moduleName, metricsName, value) PROCESS_PROFILE_STRING_VALUE(moduleName, metricsName, value)

#define BEGIN_PROFILE_SECTION_TRACE(moduleName) {
#define END_PROFILE_SECTION_TRACE(moduleName) }

#else // Not define INDEXLIB_PROFILE_TRACE

///time metrics for thread-local
#define SCOPED_THREAD_PROFILE_TIME_TRACE(moduleName, metricsName)
#define ENTER_THREAD_PROFILE_TIME_TRACE(moduleName, metricsName)
#define LEAVE_THREAD_PROFILE_TIME_TRACE(moduleName, metricsName)

///counter metrics for thread-local
#define THREAD_PROFILE_COUNT_TRACE(moduleName, metricsName, value)

///time metrics for process
#define SCOPED_PROCESS_PROFILE_TIME_TRACE(moduleName, metricsName)
#define ENTER_PROCESS_PROFILE_TIME_TRACE(moduleName, metricsName)
#define LEAVE_PROCESS_PROFILE_TIME_TRACE(moduleName, metricsName)

///counter metrics for process
#define PROCESS_PROFILE_COUNT_TRACE(moduleName, metricsName, value)

////////////////////////////////////////////////////////////////
///Paired value metrics

///Paired value metrics for thread-local
#define THREAD_PROFILE_PAIR_VALUE_TRACE(moduleName, metricsName, key, value)

///Paired value metrics for process
#define PROCESS_PROFILE_PAIR_VALUE_TRACE(moduleName, metricsName, key, value)

////////////////////////////////////////////////////////////////
///Int32 value metrics

///Int32 value metrics for thread-local
#define THREAD_PROFILE_INT32_VALUE_TRACE(moduleName, metricsName, value)

///Int32 value metrics for process
#define PROCESS_PROFILE_INT32_VALUE_TRACE(moduleName, metricsName, value)

////////////////////////////////////////////////////////////////
///Int64 value metrics

///Int64 value metrics for thread-local
#define THREAD_PROFILE_INT64_VALUE_TRACE(moduleName, metricsName, value)

///Int64 value metrics for process
#define PROCESS_PROFILE_INT64_VALUE_TRACE(moduleName, metricsName, value)

////////////////////////////////////////////////////////////////
///String value metrics

///String value metrics for thread-local
#define THREAD_PROFILE_STRING_VALUE_TRACE(moduleName, metricsName, value)

///String value metrics for process
#define PROCESS_PROFILE_STRING_VALUE_TRACE(moduleName, metricsName, value)

#define BEGIN_PROFILE_SECTION_TRACE(moduleName) while (0) {
#define END_PROFILE_SECTION_TRACE(moduleName) }

#endif // #ifdef INDEXLIB_PROFILE_TRACE

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_PROFILING_H
