#ifndef ISEARCH_SERVICEDEGRADE_H
#define ISEARCH_SERVICEDEGRADE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/ServiceDegradationConfig.h>
#include <ha3/common/Request.h>
#include <multi_call/agent/QueryInfo.h>
#include <autil/Lock.h>

BEGIN_HA3_NAMESPACE(service);

class ServiceDegrade
{
public:
    ServiceDegrade(const config::ServiceDegradationConfig &config);
    ~ServiceDegrade();
private:
    ServiceDegrade(const ServiceDegrade &);
    ServiceDegrade& operator=(const ServiceDegrade &);
public:
    bool needDegade(uint32_t workerQueueSize);
    bool updateRequest(common::Request *request,
                       const multi_call::QueryInfoPtr &queryInfo);
    config::ServiceDegradationConfig getServiceDegradationConfig() const {
        return _config;
    };
private:
    config::ServiceDegradationConfig _config;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ServiceDegrade);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_SERVICEDEGRADE_H
