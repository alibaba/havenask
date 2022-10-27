#include <ha3/config/ClusterConfigInfo.h>
#include <ha3/config/TypeDefine.h>
#include <autil/HashFuncFactory.h>

using namespace std;
using namespace autil;
using namespace suez::turing;
USE_HA3_NAMESPACE(proto);
BEGIN_HA3_NAMESPACE(config);
HA3_LOG_SETUP(config, ClusterConfigInfo);

ClusterConfigInfo::ClusterConfigInfo()
    : _returnHitThreshold(RETURN_HIT_REWRITE_THRESHOLD)
    , _returnHitRatio(RETURN_HIT_REWRITE_RATIO)    // disabled by default
    , _poolTrunkSize(POOL_TRUNK_SIZE) // default 10M
    , _poolRecycleSizeLimit(POOL_RECYCLE_SIZE_LIMIT) // defualt 20M
    , _poolMaxCount(POOL_MAX_COUNT) // defualt 200
    , _threadInitRoundRobin(THREAD_INIT_ROUND_ROBIN)
{
}

ClusterConfigInfo::~ClusterConfigInfo() {
}

void ClusterConfigInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    JSONIZE(json, "table_name", _tableName);
    JSONIZE(json, "hash_mode", _hashMode);
    JSONIZE(json, "query_config", _queryInfo);
    JSONIZE(json, "join_config", _joinConfig);
    JSONIZE(json, "return_hit_rewrite_threshold", _returnHitThreshold);
    JSONIZE(json, "return_hit_rewrite_ratio", _returnHitRatio);
    JSONIZE(json, "pool_trunk_size", _poolTrunkSize);
    JSONIZE(json, "pool_recycle_size_limit", _poolRecycleSizeLimit);
    JSONIZE(json, "pool_max_count", _poolMaxCount);
    JSONIZE(json, "thread_init_round_robin", _threadInitRoundRobin);
}

bool ClusterConfigInfo::check() const {
    if (!_hashMode.validate()) {
        HA3_LOG(WARN, "validate hash mode failed. table name [%s].", _tableName.c_str());
        return false;
    }
    const vector<JoinConfigInfo> &joinInfos = _joinConfig.getJoinInfos();
    for (size_t i = 0; i < joinInfos.size(); ++i) {
        if (joinInfos[i].useJoinCache
            && joinInfos[i].getJoinField().empty())
        {
            return false;
        }
    }
    if (_poolTrunkSize == 0) {
        return false;
    }
    // TODO: add other check
    return true;
}

bool ClusterConfigInfo::initHashFunc() {
    if (_hashFunctionBasePtr) {
        return true;
    }
    if (!_hashMode.validate()) {
        HA3_LOG(WARN, "validate hash mode failed. table name [%s].", _tableName.c_str());
        return false;
    }
    _hashFunctionBasePtr = HashFuncFactory::createHashFunc(_hashMode._hashFunction,
            _hashMode._hashParams, MAX_PARTITION_COUNT);
    if (!_hashFunctionBasePtr) {
        HA3_LOG(WARN, "invalid hash function [%s]. table name [%s].",
                _hashMode._hashFunction.c_str(), _tableName.c_str());
        return false;
    }
    return true;
}

END_HA3_NAMESPACE(config);
