#ifndef __INDEXLIB_TYPED_METRICS_MAP_H
#define __INDEXLIB_TYPED_METRICS_MAP_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>

IE_NAMESPACE_BEGIN(util);

template <typename T>
class TypedMetricsMap
{
public:
    TypedMetricsMap() {}
    ~TypedMetricsMap() {}

public:
    typedef T GroupType;
    typedef std::tr1::shared_ptr<GroupType> GroupPtrType;
    
    typedef typename GroupType::MetricsType MetricsType;
    typedef typename GroupType::MetricsPtrType MetricsPtrType;

    typedef std::map<std::string, GroupPtrType> GroupMap;
    typedef typename std::map<std::string, GroupPtrType>::const_iterator ConstIterator;
    
public:
    MetricsType& GetMetrics(const std::string& metricsName)
    {
        MetricsType* metrics = TryGetMetrics(metricsName);
        if (metrics)
        {
            return *metrics;
        }
        else
        {
            return GetOrInsertMetrics(metricsName);
        }
    }

    void ExtractMetricsNames(std::vector<std::string>& names) const;

    uint32_t GetMetricsCount(const std::string& metricsName);

    int64_t GetTotalCountByMetricsName(const std::string& metricsName);
    int64_t GetTotalValueByMetricsName(const std::string& metricsName);

    /**
     * Reset all metrics 
     */
    void ResetAll();
    
    /**
     * Reset metrics of current thread
     */
    void ResetCurrentThread();

private:
    MetricsType* TryGetMetrics(const std::string& metricsName)
    {
        autil::ScopedReadWriteLock lock(mMapRWLock, 'R');

        typename GroupMap::iterator iter = mMetricsGroups.find(metricsName);
        if (iter != mMetricsGroups.end())
        {
            return &(iter->second->GetMetrics(metricsName));
        }
        else
        {
            return NULL;
        }
    }

    MetricsType& GetOrInsertMetrics(const std::string& metricsName)
    {
        autil::ScopedReadWriteLock lock(mMapRWLock, 'W');

        typename GroupMap::iterator iter = mMetricsGroups.find(metricsName);
        if (iter == mMetricsGroups.end())
        {
            GroupPtrType metricsGroup(new GroupType);
            mMetricsGroups[metricsName] = metricsGroup;
            return metricsGroup->GetMetrics(metricsName);
        }
        else
        {
            return iter->second->GetMetrics(metricsName);
        }
    }

private:
    GroupMap mMetricsGroups;
    mutable autil::ReadWriteLock mMapRWLock;
};

////////////////////////////////////////////
//
template <typename T>
int64_t TypedMetricsMap<T>::GetTotalCountByMetricsName(const std::string& metricsName)
{
    autil::ScopedReadWriteLock lock(mMapRWLock, 'R');

    typename GroupMap::iterator iter = mMetricsGroups.find(metricsName);
    if (iter == mMetricsGroups.end())
    {
        return 0;
    }
    else
    {
        return iter->second->GetTotalCount();
    }
}

template <typename T>
int64_t TypedMetricsMap<T>::GetTotalValueByMetricsName(const std::string& metricsName)
{
    autil::ScopedReadWriteLock lock(mMapRWLock, 'R');

    typename GroupMap::iterator iter = mMetricsGroups.find(metricsName);
    if (iter == mMetricsGroups.end())
    {
        return 0;
    }
    else
    {
        return iter->second->GetTotalValue();
    }
}

template <typename T>
void TypedMetricsMap<T>::ExtractMetricsNames(std::vector<std::string>& names) const
{
    autil::ScopedReadWriteLock lock(mMapRWLock, 'R');
    names.reserve(mMetricsGroups.size());
    for (typename GroupMap::const_iterator it = mMetricsGroups.begin();
         it != mMetricsGroups.end(); ++it)
    {
        names.push_back(it->first);
    }
}

template <typename T>
void TypedMetricsMap<T>::ResetAll()
{
    autil::ScopedReadWriteLock lock(mMapRWLock, 'W');
    mMetricsGroups.clear();
}

template <typename T>
uint32_t TypedMetricsMap<T>::GetMetricsCount(const std::string& metricsName)
{
    autil::ScopedReadWriteLock lock(mMapRWLock, 'R');

    typename GroupMap::iterator iter = mMetricsGroups.find(metricsName);
    if (iter == mMetricsGroups.end())
    {
        return 0;
    }
    else
    {
        return iter->second->GetMetricsCount();
    }
}

template <typename T>
void TypedMetricsMap<T>::ResetCurrentThread()
{
    autil::ScopedReadWriteLock lock(mMapRWLock, 'W');
    typename GroupMap::iterator iter = mMetricsGroups.begin();
    while (iter != mMetricsGroups.end())
    {
        iter->second->ResetCurrentThread();
        iter++;
    }
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_TYPED_METRICS_MAP_H
