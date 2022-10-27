#ifndef ISEARCH_QRSSESSIONSQLREQUEST_H
#define ISEARCH_QRSSESSIONSQLREQUEST_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/sql/framework/SqlQueryRequest.h>
#include <ha3/monitor/SessionMetricsCollector.h>

BEGIN_HA3_NAMESPACE(service);

struct QrsSessionSqlRequest {
    QrsSessionSqlRequest(sql::SqlQueryRequest *request,
                         autil::mem_pool::Pool *pool_ = NULL,
                         const monitor::SessionMetricsCollectorPtr &metricsCollector =
                         monitor::SessionMetricsCollectorPtr())
        : sqlRequest(request)
        , pool(pool_)
        , metricsCollectorPtr(metricsCollector)
    {
    }
public:
    sql::SqlQueryRequest *sqlRequest;
    autil::mem_pool::Pool *pool;
    monitor::SessionMetricsCollectorPtr metricsCollectorPtr;
};

END_HA3_NAMESPACE(service);

#endif //ISEARCH_QRSSESSIONSQLREQUEST_H
