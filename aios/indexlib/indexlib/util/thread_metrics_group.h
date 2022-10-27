#ifndef __INDEXLIB_THREAD_METRICS_GROUP_H
#define __INDEXLIB_THREAD_METRICS_GROUP_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include <map>

IE_NAMESPACE_BEGIN(util);

template <typename T>
class ThreadMetricsGroup
{
public:
    typedef T MetricsType;
    typedef std::tr1::shared_ptr<MetricsType> MetricsPtrType;

    typedef std::map<int32_t, MetricsPtrType> MetricsMap;
    
public:
    ThreadMetricsGroup() {}
    ~ThreadMetricsGroup() {}
    
public:
    MetricsType& GetMetrics(const std::string& name)
    {
        int32_t threadId = pthread_self();
        MetricsPtrType metrics = TryGetMetrics(threadId);
        if (metrics)
        {
            return *metrics;
        }
        else
        {
            return GetOrInsertMetrics(threadId, name);
        }
    }

    int64_t GetTotalValue()
    {
        autil::ScopedReadWriteLock lock(mMapRWLock, 'R');

        int64_t ret = 0;
        typename MetricsMap::iterator iter = mMetricsMap.begin();
        while (iter != mMetricsMap.end())
        {
            ret += iter->second->GetValue();
            iter++;
        }
        return ret;
    }

    int64_t GetTotalCount()
    {
        autil::ScopedReadWriteLock lock(mMapRWLock, 'R');

        int64_t ret = 0;
        typename MetricsMap::iterator iter = mMetricsMap.begin();
        while (iter != mMetricsMap.end())
        {
            ret += iter->second->GetCount();
            iter++;
        }
        return ret;
    }

    void ResetCurrentThread()
    {
        autil::ScopedReadWriteLock lock(mMapRWLock, 'R');

        int32_t threadId = pthread_self();
        
        typename MetricsMap::iterator iter = mMetricsMap.find(threadId);
        if (iter != mMetricsMap.end())
        {
            iter->second->Reset();
        }
    }

    uint32_t GetMetricsCount()
    {
        autil::ScopedReadWriteLock lock(mMapRWLock, 'R');
        return mMetricsMap.size();
    }

protected:
    MetricsPtrType TryGetMetrics(int32_t threadId)
    {
        autil::ScopedReadWriteLock lock(mMapRWLock, 'R');

        typename MetricsMap::iterator iter = mMetricsMap.find(threadId);
        if (iter == mMetricsMap.end())
        {
            return MetricsPtrType();
        }
        else
        {
            return iter->second;
        }
    }

    MetricsType& GetOrInsertMetrics(int32_t threadId, const std::string& name)
    {
        autil::ScopedReadWriteLock lock(mMapRWLock, 'W');

        typename MetricsMap::iterator iter = mMetricsMap.find(threadId);
        if (iter == mMetricsMap.end())
        {
            MetricsPtrType metrics(new MetricsType(name));
            mMetricsMap[threadId] = metrics;
            return *metrics;
        }
        else
        {
            return *(iter->second);
        }
    }

private:
    MetricsMap mMetricsMap;
    autil::ReadWriteLock mMapRWLock;
};

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_THREAD_METRICS_GROUP_H
