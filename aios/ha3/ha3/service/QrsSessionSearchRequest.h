#ifndef ISEARCH_QRSSESSIONSEARCHREQUEST_H
#define ISEARCH_QRSSESSIONSEARCHREQUEST_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/monitor/SessionMetricsCollector.h>

BEGIN_HA3_NAMESPACE(service);

struct QrsSessionSearchRequest {
    QrsSessionSearchRequest(const std::string &requestStr_ = "",
                            const std::string &clientIp_ = "",
                            autil::mem_pool::Pool *pool_ = NULL,
                            const monitor::SessionMetricsCollectorPtr &metricsCollector =
                            monitor::SessionMetricsCollectorPtr())
        : requestStr(requestStr_)
        , clientIp(clientIp_)
        , pool(pool_)
        , metricsCollectorPtr(metricsCollector)
    {
    }
    std::string requestStr;
    std::string clientIp;
    autil::mem_pool::Pool *pool;
    monitor::SessionMetricsCollectorPtr metricsCollectorPtr;
    SessionSrcType sessionSrcType = SESSION_SRC_UNKNOWN;
};

END_HA3_NAMESPACE(service);

#endif //ISEARCH_QRSSESSIONSEARCHREQUEST_H
