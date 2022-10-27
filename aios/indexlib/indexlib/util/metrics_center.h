#ifndef __INDEXLIB_METRICS_CENTER_H
#define __INDEXLIB_METRICS_CENTER_H

#include <map>
#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/util/metrics.h"
#include "indexlib/util/unsafe_metrics.h"
#include "indexlib/util/vector_metrics.h"
#include "indexlib/util/process_metrics_group.h"
#include "indexlib/util/thread_metrics_group.h"
#include "indexlib/util/typed_metrics_map.h"
#include "indexlib/util/proxy_metrics.h"
#include "indexlib/misc/singleton.h"
#include "autil/Lock.h"

IE_NAMESPACE_BEGIN(util);

class MetricsCenter : public misc::Singleton<MetricsCenter>
{
public:
    typedef TypedMetricsMap<ProcessMetricsGroup<Metrics> > ProcessMetricsMap;
    typedef TypedMetricsMap<ThreadMetricsGroup<UnsafeMetrics> > ThreadLocalMetricsMap;

    typedef TypedMetricsMap<ProcessMetricsGroup<PairedVectorMetrics> > ProcessPVMetricsMap;
    typedef TypedMetricsMap<ThreadMetricsGroup<UnsafePairedVectorMetrics> > ThreadLocalPVMetricsMap;

    typedef TypedMetricsMap<ProcessMetricsGroup<Int32VectorMetrics> > ProcessInt32VectMetricsMap;
    typedef TypedMetricsMap<ThreadMetricsGroup<UnsafeInt32VectorMetrics> > ThreadLocalInt32VectMetricsMap;

    typedef TypedMetricsMap<ProcessMetricsGroup<Int64VectorMetrics> > ProcessInt64VectMetricsMap;
    typedef TypedMetricsMap<ThreadMetricsGroup<UnsafeInt64VectorMetrics> > ThreadLocalInt64VectMetricsMap;

    typedef TypedMetricsMap<ProcessMetricsGroup<StrVectorMetrics> > ProcessStrVectMetricsMap;
    typedef TypedMetricsMap<ThreadMetricsGroup<UnsafeStrVectorMetrics> > ThreadLocalStrVectMetricsMap;

    typedef std::map<std::string, ProxyMetricsPtr> ProcessProxyMetricsMap;

public:
    MetricsCenter();
    ~MetricsCenter();

public:
    /**
     * Get process metrics by name
     */
    template <typename T>
    T& GetProcessMetrics(const std::string& metricsName)
    {
        assert(false);
    }

    /**
     * try to get proxy process metrics by name
     */
    ProxyMetrics* TryGetProxyMetrics(const std::string& metricsName);

    /**
     * register proxy process metrics by name
     */
    bool RegisterProxyMetrics(const ProxyMetricsPtr& proxyMetrics);

    /**
     * unregister proxy process metrics by name
     */
    bool UnRegisterProxyMetrics(const std::string& metricsName);

    /**
     * Extract all process metrics names
     */
    template <typename T>
    void ExtractProcessMetricsNames(std::vector<std::string>& metricsNames)
    {
        assert(false);
    }

    /**
     * Get thread-local metrics of current thread by name 
     */
    template <typename T>
    T& GetThreadLocalMetrics(const std::string& metricsName)
    {
        assert(false);
    }

    /**
     * Extract all thread-local metrics names
     */
    template <typename T>
    void ExtractThreadMetricsNames(std::vector<std::string>& metricsNames)
    {
        assert(false);
    }

    int64_t GetTotalCountByMetricsName(const std::string& metricsName);
    int64_t GetTotalValueByMetricsName(const std::string& metricsName);

    //Clear all the metrics
    void ResetAll()
    {
        mProcessMetrics.ResetAll();
        mThreadLocalMetrics.ResetAll();

        mProcessPVMetrics.ResetAll();
        mThreadLocalPVMetrics.ResetAll();
    }

    void ResetCurrentThread()
    {
        mThreadLocalMetrics.ResetCurrentThread();
        mThreadLocalPVMetrics.ResetCurrentThread();
    }

    uint32_t GetMetricsCount(const std::string& metricsName)
    {
        return (mProcessMetrics.GetMetricsCount(metricsName) + 
                mThreadLocalMetrics.GetMetricsCount(metricsName) +
                mProcessPVMetrics.GetMetricsCount(metricsName) +
                mThreadLocalPVMetrics.GetMetricsCount(metricsName));
    }

private:
    ProcessMetricsMap mProcessMetrics;
    ThreadLocalMetricsMap mThreadLocalMetrics;

    ProcessPVMetricsMap mProcessPVMetrics;
    ThreadLocalPVMetricsMap mThreadLocalPVMetrics;

    ProcessInt32VectMetricsMap mProcessInt32VectMetrics;
    ThreadLocalInt32VectMetricsMap mThreadLocalInt32VectMetrics;

    ProcessInt64VectMetricsMap mProcessInt64VectMetrics;
    ThreadLocalInt64VectMetricsMap mThreadLocalInt64VectMetrics;

    ProcessStrVectMetricsMap mProcessStrVectMetrics;
    ThreadLocalStrVectMetricsMap mThreadLocalStrVectMetrics;

    mutable autil::ReadWriteLock mProxyMetricsLock;
    ProcessProxyMetricsMap mProxyMetrics;
};

///////////////////////////////////////////////////////////////////////////////
template <>
inline Metrics& 
MetricsCenter::GetProcessMetrics<Metrics>(const std::string& metricsName)
{
    return mProcessMetrics.GetMetrics(metricsName);
}

template <>
inline PairedVectorMetrics& 
MetricsCenter::GetProcessMetrics<PairedVectorMetrics>(const std::string& metricsName)
{
    return mProcessPVMetrics.GetMetrics(metricsName);
}

template <>
inline Int32VectorMetrics& 
MetricsCenter::GetProcessMetrics<Int32VectorMetrics>(const std::string& metricsName)
{
    return mProcessInt32VectMetrics.GetMetrics(metricsName);
}

template <>
inline Int64VectorMetrics& 
MetricsCenter::GetProcessMetrics<Int64VectorMetrics>(const std::string& metricsName)
{
    return mProcessInt64VectMetrics.GetMetrics(metricsName);
}

template <>
inline StrVectorMetrics& 
MetricsCenter::GetProcessMetrics<StrVectorMetrics>(const std::string& metricsName)
{
    return mProcessStrVectMetrics.GetMetrics(metricsName);
}

inline ProxyMetrics*
MetricsCenter::TryGetProxyMetrics(const std::string& metricsName)
{
    autil::ScopedReadWriteLock lock(mProxyMetricsLock, 'R');    
    ProcessProxyMetricsMap::const_iterator it = mProxyMetrics.find(metricsName);
    if (it !=  mProxyMetrics.end())
    {
        return it->second.get();
    }
    return NULL;
}

inline bool MetricsCenter::RegisterProxyMetrics(const ProxyMetricsPtr& proxyMetrics)
{
    autil::ScopedReadWriteLock lock(mProxyMetricsLock, 'W');    
    ProcessProxyMetricsMap::const_iterator it = mProxyMetrics.find(proxyMetrics->GetMetricsName());
    if (it == mProxyMetrics.end())
    {
        mProxyMetrics[proxyMetrics->GetMetricsName()] = proxyMetrics;
        return true;
    }

    return false;
}

inline bool MetricsCenter::UnRegisterProxyMetrics(const std::string& metricsName)
{
    autil::ScopedReadWriteLock lock(mProxyMetricsLock, 'W');    
    ProcessProxyMetricsMap::iterator it = mProxyMetrics.find(metricsName);
    if (it != mProxyMetrics.end())
    {
        mProxyMetrics.erase(it);
        return true;
    }

    return false;
}

template <>
inline void MetricsCenter::ExtractProcessMetricsNames<Metrics>(
        std::vector<std::string>& metricsNames)
{
    return mProcessMetrics.ExtractMetricsNames(metricsNames);
}

template <>
inline UnsafeMetrics& 
MetricsCenter::GetThreadLocalMetrics<UnsafeMetrics>(const std::string& metricsName)
{
    return mThreadLocalMetrics.GetMetrics(metricsName);
}

template <>
inline UnsafePairedVectorMetrics& 
MetricsCenter::GetThreadLocalMetrics<UnsafePairedVectorMetrics>(
        const std::string& metricsName)
{
    return mThreadLocalPVMetrics.GetMetrics(metricsName);
}

template <>
inline UnsafeInt32VectorMetrics& 
MetricsCenter::GetThreadLocalMetrics<UnsafeInt32VectorMetrics>(
        const std::string& metricsName)
{
    return mThreadLocalInt32VectMetrics.GetMetrics(metricsName);
}

template <>
inline UnsafeInt64VectorMetrics& 
MetricsCenter::GetThreadLocalMetrics<UnsafeInt64VectorMetrics>(
        const std::string& metricsName)
{
    return mThreadLocalInt64VectMetrics.GetMetrics(metricsName);
}

template <>
inline UnsafeStrVectorMetrics& 
MetricsCenter::GetThreadLocalMetrics<UnsafeStrVectorMetrics>(
        const std::string& metricsName)
{
    return mThreadLocalStrVectMetrics.GetMetrics(metricsName);
}

inline int64_t 
MetricsCenter::GetTotalCountByMetricsName(const std::string& metricsName)
{
    return (mProcessMetrics.GetTotalCountByMetricsName(metricsName) + 
            mThreadLocalMetrics.GetTotalCountByMetricsName(metricsName) +
            mProcessPVMetrics.GetTotalCountByMetricsName(metricsName) +
            mThreadLocalPVMetrics.GetTotalCountByMetricsName(metricsName));   
}

inline int64_t 
MetricsCenter::GetTotalValueByMetricsName(const std::string& metricsName)
{
    return (mProcessMetrics.GetTotalValueByMetricsName(metricsName) + 
            mThreadLocalMetrics.GetTotalValueByMetricsName(metricsName) +
            mProcessPVMetrics.GetTotalValueByMetricsName(metricsName) +
            mThreadLocalPVMetrics.GetTotalValueByMetricsName(metricsName));
}

typedef std::tr1::shared_ptr<MetricsCenter> MetricsCenterPtr;

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_METRICS_CENTER_H
