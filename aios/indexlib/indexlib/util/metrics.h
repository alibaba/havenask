#ifndef __INDEXLIB_METRICS_H
#define __INDEXLIB_METRICS_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/typed_metrics_base.h"
#include <tr1/memory>
#include <iostream>
#include "autil/Lock.h"

IE_NAMESPACE_BEGIN(util);

class Metrics;
typedef std::tr1::shared_ptr<Metrics> MetricsPtr;

class Metrics : public TypedMetricsBase<Metrics>
{
public:
    Metrics(const std::string& metricsName) 
        : TypedMetricsBase<Metrics>(metricsName)
        , mValue(0) 
        , mCount(0)
    {
    }

    Metrics(const std::string& metricsName, MetricsPtr parent) 
        : TypedMetricsBase<Metrics>(metricsName, parent)
        , mValue(0)
        , mCount(0)
    {
    }

    ~Metrics();

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

////////////////////////////////////////////////
//
inline void Metrics::AddValue(int64_t value) 
{
    asm volatile(
            "lock ; "  "addq %1,%0"
            :"=m" (mValue)
            :"ir" (value), "m" (mValue));
    
    int32_t i = 1;
    asm volatile(
            "lock ; "  "addl %1,%0"
            :"=m" (mCount)
            :"ir" (i), "m" (mCount));

    assert(mValue >= 0);
    if (mParent)
    {
        mParent->AddValue(value);
    }
}

inline void Metrics::SubValue(int64_t value)
{
     asm volatile(
            "lock ; "  "subq %1,%0"
            :"=m" (mValue)
            :"ir" (value), "m" (mValue));

    int32_t i = 1;
    asm volatile(
            "lock ; "  "addl %1,%0"
            :"=m" (mCount)
            :"ir" (i), "m" (mCount));

    assert(mValue >= 0);
    if (mParent)
    {
        mParent->SubValue(value);
    }
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_METRICS_H
