#ifndef ISEARCH_SERVICEDEGRADATIONCONFIG_H
#define ISEARCH_SERVICEDEGRADATIONCONFIG_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>

BEGIN_HA3_NAMESPACE(config);

class ServiceDegradationCondition : public autil::legacy::Jsonizable
{
public:
    ServiceDegradationCondition() {
        workerQueueSizeDegradeThreshold = std::numeric_limits<uint32_t>::max();
        workerQueueSizeRecoverThreshold = 0;
        duration = 0;
    }
    ~ServiceDegradationCondition() {
    }
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("worker_queue_size_degrade_threshold",
                     workerQueueSizeDegradeThreshold, workerQueueSizeDegradeThreshold);
        json.Jsonize("worker_queue_size_recover_threshold",
                     workerQueueSizeRecoverThreshold, workerQueueSizeRecoverThreshold);
        json.Jsonize("duration", duration, duration);
    }
public:
    uint32_t workerQueueSizeDegradeThreshold;
    uint32_t workerQueueSizeRecoverThreshold;
    int64_t duration; // ms
};

class ServiceDegradationRequest : public autil::legacy::Jsonizable
{
public:
    ServiceDegradationRequest() {
        rankSize = 0;
        rerankSize = 0;
    }
    ~ServiceDegradationRequest() {
    }
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("rank_size", rankSize, rankSize);
        json.Jsonize("rerank_size", rerankSize, rerankSize);
    }    
public:
    uint32_t rankSize;
    uint32_t rerankSize;
};

class ServiceDegradationConfig : public autil::legacy::Jsonizable
{
public:
    ServiceDegradationConfig() {
    }
    ~ServiceDegradationConfig() {
    }
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("condition", condition, condition);
        json.Jsonize("request", request, request);
    }    
public:
    ServiceDegradationCondition condition;
    ServiceDegradationRequest request;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ServiceDegradationConfig);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_SERVICEDEGRADATIONCONFIG_H
