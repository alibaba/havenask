#ifndef __INDEXLIB_METRICS_BASE_H
#define __INDEXLIB_METRICS_BASE_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include <string>

IE_NAMESPACE_BEGIN(util);

class MetricsBase
{
public:
    MetricsBase(const std::string& metricsName) : mMetricsName(metricsName)
    {
    }

    virtual ~MetricsBase() {}

public:
    inline std::string GetMetricsName() const {return mMetricsName;}
    
public:
    virtual void Reset() = 0;
    
protected:
    std::string mMetricsName;
};

typedef std::tr1::shared_ptr<MetricsBase> MetricsBasePtr;

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_METRICS_BASE_H
