#ifndef ISEARCH_QRSCONFIG_H
#define ISEARCH_QRSCONFIG_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>
#include <build_service/plugin/ModuleInfo.h>
#include <ha3/config/ProcessorInfo.h>
#include <ha3/config/QrsChainInfo.h>
#include <ha3/config/TypeDefine.h>
#include <suez/turing/common/CavaConfig.h>
#include <suez/turing/common/BizInfo.h>
#include <ha3/config/ResourceTypeSet.h>
#include <ha3/common/CompressTypeConvertor.h>
#include <build_service/plugin/ModuleInfo.h>

BEGIN_HA3_NAMESPACE(config);

struct SearchTaskQueueRule {
    std::string phaseOneTaskQueue;
    std::string phaseTwoTaskQueue;

    SearchTaskQueueRule() {
        this->phaseOneTaskQueue = DEFAULT_TASK_QUEUE_NAME;
        this->phaseTwoTaskQueue = DEFAULT_TASK_QUEUE_NAME;
    }
};

class QRSRule : public autil::legacy::Jsonizable {
public:
    QRSRule() : _retHitsLimit(QRS_RETURN_HITS_LIMIT),
                _requestQueueSize(QRS_ARPC_REQUEST_QUEUE_SIZE),
                _connectionTimeout(QRS_ARPC_CONNECTION_TIMEOUT),
                _replicaCount(QRS_REPLICA_COUNT),
                _threadInitRoundRobin(THREAD_INIT_ROUND_ROBIN)
    {
        _resourceType.fromString(DEFAULT_RESOURCE_TYPE);
    }
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("return_hits_limit", _retHitsLimit, (uint32_t)QRS_RETURN_HITS_LIMIT);
        json.Jsonize("request_queue_size", _requestQueueSize, (uint32_t)QRS_ARPC_REQUEST_QUEUE_SIZE);
        json.Jsonize("connection_timeout", _connectionTimeout, (int32_t)QRS_ARPC_CONNECTION_TIMEOUT);
        json.Jsonize("search_phase_one_task_queue",
                     _searchTaskQueueRule.phaseOneTaskQueue, DEFAULT_TASK_QUEUE_NAME);
        json.Jsonize("search_phase_two_task_queue",
                     _searchTaskQueueRule.phaseTwoTaskQueue, DEFAULT_TASK_QUEUE_NAME);
        json.Jsonize("replica_count", _replicaCount, (uint32_t)QRS_REPLICA_COUNT);
        json.Jsonize("healthcheck_files", _healthcheckFiles, _healthcheckFiles);
        json.Jsonize("thread_init_round_robin", _threadInitRoundRobin, _threadInitRoundRobin);

        if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
            std::string str = _resourceType.toString();
            json.Jsonize("resource_type", str);
        } else {
            std::string str;
            json.Jsonize("resource_type", str, DEFAULT_RESOURCE_TYPE);
            _resourceType.fromString(str);
        }
    }
public:
    uint32_t _retHitsLimit;
    uint32_t _requestQueueSize;
    int32_t _connectionTimeout; //miliseconds
    SearchTaskQueueRule _searchTaskQueueRule;
    uint32_t _replicaCount;
    ResourceTypeSet _resourceType;
    std::vector<std::string> _healthcheckFiles;
    size_t _threadInitRoundRobin;
};

class QRSResultCompressConfig : public autil::legacy::Jsonizable {
public:
    QRSResultCompressConfig() : _compressType("no_compress") {}
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("compress_type", _compressType, std::string("no_compress"));
    }

public:
    std::string _compressType;
};

class QRSRequestCompressConfig : public autil::legacy::Jsonizable {
public:
    QRSRequestCompressConfig() : _compressType("no_compress") {}
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("compress_type", _compressType, std::string("no_compress"));
    }
    HaCompressType getCompressType() {
        return common::CompressTypeConvertor::convertCompressType(_compressType);
    }
public:
    std::string _compressType;
};

class QrsPoolConfig : public autil::legacy::Jsonizable {
public:
    QrsPoolConfig()
        : _poolTrunkSize(POOL_TRUNK_SIZE) // default 10M
        , _poolRecycleSizeLimit(POOL_RECYCLE_SIZE_LIMIT) // defualt 20M
        , _poolMaxCount(POOL_MAX_COUNT) // defualt 200
    {}
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        JSONIZE(json, "pool_trunk_size", _poolTrunkSize);
        JSONIZE(json, "pool_recycle_size_limit", _poolRecycleSizeLimit);
        JSONIZE(json, "pool_max_count", _poolMaxCount);
    }
public:
    size_t _poolTrunkSize; // M
    size_t _poolRecycleSizeLimit; // M
    size_t _poolMaxCount;
};

class QrsConfig : public autil::legacy::Jsonizable {
public:
    QrsConfig() {
        _qrsChainInfos.resize(1);
        _cavaConfig._cavaConf = HA3_CAVA_CONF;// important!
    }
    ~QrsConfig() {}
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("qrs_rule", _qrsRule, _qrsRule);
        json.Jsonize(QRS_RESULT_COMPRESS, _resultCompress, _resultCompress);
        json.Jsonize(QRS_REQUEST_COMPRESS, _requestCompress, _requestCompress);
        json.Jsonize("modules", _modules, _modules);
        json.Jsonize("processors", _processorInfos, _processorInfos);
        json.Jsonize("chains", _qrsChainInfos, _qrsChainInfos);
        json.Jsonize("qrs_metrics_src_whitelist", _metricsSrcWhiteList,
                     _metricsSrcWhiteList);
        json.Jsonize("pool_config", _poolConfig, _poolConfig);
        json.Jsonize("cava_config", _cavaConfig, _cavaConfig);
        json.Jsonize("dependency_service_map", _dependServices, _dependServices);
    }
public:
    QRSRule _qrsRule;
    QRSResultCompressConfig _resultCompress;
    QRSRequestCompressConfig _requestCompress;
    build_service::plugin::ModuleInfos _modules;
    std::vector<ProcessorInfo> _processorInfos;
    std::vector<QrsChainInfo> _qrsChainInfos;
    std::vector<std::string> _metricsSrcWhiteList;
    QrsPoolConfig _poolConfig;
    suez::turing::CavaConfig _cavaConfig;
    suez::turing::BizDependService _dependServices;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QrsConfig);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_QRSCONFIG_H
