#ifndef ISEARCH_ADMINCONFIG_H
#define ISEARCH_ADMINCONFIG_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>

BEGIN_HA3_NAMESPACE(config);

static const int EIGHT_DAY_IN_SECONDS = 8 * 24 * 3600;
static const int LONG_DEAD_SECONDS = 5 * 60; // 5min

struct HealthCheckConfig : public autil::legacy::Jsonizable
{
public:
    HealthCheckConfig() {
        iowaitThreshold = 1 << 30;
        requestQueueSizeThreshold = 1 << 30;
        updateInterval = 200 * 1000; // 200ms
    }
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("iowait_threshold", iowaitThreshold, iowaitThreshold);
        json.Jsonize("request_queue_size_threshold", requestQueueSizeThreshold,
                     requestQueueSizeThreshold);
        json.Jsonize("update_interval", updateInterval, updateInterval);
    }
public:
    int32_t iowaitThreshold;
    int32_t requestQueueSizeThreshold;
    int32_t updateInterval;
};

struct WorkerCapabilityConfig : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("machine_list", machineList, machineList);
        json.Jsonize("capability", capability, DEFAULT_WORKER_CAPABILITY);
    }
public:
    std::string machineList;
    uint32_t capability;
};
    
struct AdminConfig : public autil::legacy::Jsonizable
{
public:
    AdminConfig() {
        canForceSwitch = false;
        forceSwitchWhenDeadLock = true;
        maxWorkerUnavailableSeconds = EIGHT_DAY_IN_SECONDS;
        searcherLongDeadSeconds = LONG_DEAD_SECONDS;
        qrsResource = 0; // deprecated
        unavailableWorkeThreshold = 1 << 30;
        workerLoadBalance = false;
        workerUnknownTimeout = 5000; // 5000ms
        workerLongDeadTimeout = 10 * 60 * 1000; // 10min
        invalidMachineLimit = 6;
    }
    ~AdminConfig() {}
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("can_force_switch", canForceSwitch, false);
        json.Jsonize("force_switch_when_dead_lock", forceSwitchWhenDeadLock, true);
        json.Jsonize("max_worker_unavailable_seconds", maxWorkerUnavailableSeconds, EIGHT_DAY_IN_SECONDS);
        json.Jsonize("searcher_long_dead_seconds", searcherLongDeadSeconds, LONG_DEAD_SECONDS);
        json.Jsonize("qrs_resource", qrsResource, 0); // deprecated
        json.Jsonize("unavailable_worker_threshold", unavailableWorkeThreshold, 1 << 30);
        json.Jsonize("healthcheck_config", healthCheckConfig, healthCheckConfig);
        json.Jsonize("worker_load_balance", workerLoadBalance, false);
        json.Jsonize("worker_capability", workerCapabilityConfig, workerCapabilityConfig);
        json.Jsonize("worker_unknown_timeout", workerUnknownTimeout, 5000);
        json.Jsonize("worker_longdead_timeout", workerLongDeadTimeout, 10 * 60 * 1000);
        json.Jsonize("invalid_machine_limit", invalidMachineLimit, 6);
    }
public:
    bool canForceSwitch; 
    bool forceSwitchWhenDeadLock;
    bool workerLoadBalance;
    int32_t maxWorkerUnavailableSeconds;
    int32_t searcherLongDeadSeconds;
    int qrsResource; // deprecated
    int32_t unavailableWorkeThreshold;
    int64_t workerUnknownTimeout;
    int64_t workerLongDeadTimeout;
    int32_t invalidMachineLimit;
    HealthCheckConfig healthCheckConfig;
    std::vector<WorkerCapabilityConfig> workerCapabilityConfig;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(AdminConfig);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_ADMINCONFIG_H
