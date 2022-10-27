#ifndef __INDEXLIB_PROXY_METRICS_H
#define __INDEXLIB_PROXY_METRICS_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/metrics_base.h"

IE_NAMESPACE_BEGIN(util);

class ProxyMetrics : public MetricsBase
{
public:
    ProxyMetrics(const std::string& metricsName)
        : MetricsBase(metricsName)
    {}
    virtual ~ProxyMetrics() {}

public:
    virtual int64_t GetValue() const = 0;
    virtual void Reset()  {}
};

DEFINE_SHARED_PTR(ProxyMetrics);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_PROXY_METRICS_H
