#ifndef __INDEXLIB_TYPED_PROXY_METRICS_H
#define __INDEXLIB_TYPED_PROXY_METRICS_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/proxy_metrics.h"

IE_NAMESPACE_BEGIN(util);

template <typename Proxy>
class TypedProxyMetrics : public ProxyMetrics
{
public:
    typedef std::tr1::shared_ptr<Proxy> ProxyPtr;

public:
    TypedProxyMetrics(const std::string& metricsName, 
                      const ProxyPtr& proxy)
        : ProxyMetrics(metricsName)
        , mProxy(proxy)
    {}
    ~TypedProxyMetrics() {}

public:
    int64_t GetValue() const override 
    {
        return mProxy->GetValue();
    }

private:
    ProxyPtr mProxy;
};

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_TYPED_PROXY_METRICS_H
