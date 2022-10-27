#ifndef __INDEXLIB_PROCESS_METRICS_GROUP_H
#define __INDEXLIB_PROCESS_METRICS_GROUP_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>

IE_NAMESPACE_BEGIN(util);

template <typename T>
class ProcessMetricsGroup
{
public:
    typedef T MetricsType;
    typedef std::tr1::shared_ptr<MetricsType> MetricsPtrType;

public:
    ProcessMetricsGroup() {}
    ~ProcessMetricsGroup() {}

public:
    inline MetricsType& GetMetrics(const std::string& name)
    {
        if (!mMetrics)
        {
            autil::ScopedLock lock(mLock);
            if (!mMetrics)
            {
                mMetrics.reset(new MetricsType(name));
            }
        }
        return *mMetrics;
    }

    inline uint32_t GetMetricsCount() 
    {
        if (mMetrics == NULL)
        {
            autil::ScopedLock lock(mLock);
            if (mMetrics == NULL)
            {
                return 0;
            }
        }
        return 1;
    }

    inline void ResetCurrentThread() { /* Nothing to do*/ }

    int64_t GetTotalValue()
    {
        autil::ScopedLock lock(mLock);
        if (mMetrics == NULL)
        {
            return 0;
        }
        return mMetrics->GetValue();
    }

    int64_t GetTotalCount()
    {
        autil::ScopedLock lock(mLock);
        if (mMetrics == NULL)
        {
            return 0;
        }
        return mMetrics->GetCount();
    }

private:
    MetricsPtrType mMetrics;
    autil::RecursiveThreadMutex mLock;
};

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_PROCESS_METRICS_GROUP_H
