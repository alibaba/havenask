#ifndef __INDEXLIB_UNSAFE_METRICS_H
#define __INDEXLIB_UNSAFE_METRICS_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/util/typed_metrics_base.h"
#include <tr1/memory>

IE_NAMESPACE_BEGIN(util);

class UnsafeMetrics;
typedef std::tr1::shared_ptr<UnsafeMetrics> UnsafeMetricsPtr;

class UnsafeMetrics : public TypedMetricsBase<UnsafeMetrics>
{
public:
    UnsafeMetrics(const std::string& metricsName);
    UnsafeMetrics(const std::string& metricsName, const UnsafeMetricsPtr& parent);
    virtual ~UnsafeMetrics();

public:
    inline void AddValue(int64_t value);
    inline void SubValue(int64_t value);
    inline int64_t GetValue() const {return mValue;}
    inline int64_t GetCount() const {return mCount;}
    inline double GetAverage() const {return (double)mValue / (double)mCount;}

    void Reset();
    
private:
    volatile int64_t mValue;    
    volatile int32_t mCount;
};


typedef std::tr1::shared_ptr<UnsafeMetrics> UnsafeMetricsPtr;

//////////////////////////////////////////////////////
//
inline void UnsafeMetrics::AddValue(int64_t value) 
{
    mValue += value;
    ++mCount;
    if (mParent)
    {
        mParent->AddValue(value);
    }
}

inline void UnsafeMetrics::SubValue(int64_t value)
{
    mValue -= value;
    ++mCount;
    if (mParent)
    {
        mParent->SubValue(value);
    }
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_UNSAFE_METRICS_H
