#ifndef __INDEXLIB_TYPED_METRICS_BASE_H
#define __INDEXLIB_TYPED_METRICS_BASE_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/util/metrics_base.h"
#include <tr1/memory>

IE_NAMESPACE_BEGIN(util);

template <typename T>
class TypedMetricsBase : public MetricsBase
{
public: 
    typedef T ThisType;
    typedef std::tr1::shared_ptr<ThisType> ThisTypePtr;

public:
    TypedMetricsBase(const std::string& metricsName) : MetricsBase(metricsName)
    {
    }

    TypedMetricsBase(const std::string& metricsName, const ThisTypePtr& parent)
        : MetricsBase(metricsName)
        , mParent(parent)
    {
        if (mParent && mParent->mParent)
        {
            assert(mParent->mParent->mParent == NULL); //assert three level?
        }
    }

    virtual ~TypedMetricsBase() {}

public:
    inline void SetParent(const ThisTypePtr& parent) {mParent = parent;}
    
public:
    virtual void Reset() = 0;
    
protected:
    ThisTypePtr mParent;
};

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_TYPED_METRICS_BASE_H
