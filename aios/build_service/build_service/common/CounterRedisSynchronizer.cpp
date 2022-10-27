#include "build_service/common/CounterRedisSynchronizer.h"
#include "build_service/util/RedisClient.h"
#include <indexlib/misc/exception.h>

using namespace std;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);
BS_NAMESPACE_USE(util);

namespace build_service {
namespace common {

BS_LOG_SETUP(common, CounterRedisSynchronizer);

CounterRedisSynchronizer::CounterRedisSynchronizer()
    : _ttlInSecond(-1)
{
}

CounterRedisSynchronizer::~CounterRedisSynchronizer() {
    stopSync();
}

CounterMapPtr CounterRedisSynchronizer::loadCounterMap(
            const RedisInitParam &redisParam,
            const string &key,
            const string &field,
            bool &valueExist)
{
    RedisClient redisClient;
    if (!redisClient.connect(redisParam)) {
        BS_LOG(ERROR, "connect redis failed. [%s]", redisParam.toString().c_str());
        return CounterMapPtr();
    }

    CounterMapPtr counterMap(new CounterMap());
    RedisClient::ErrorCode errorCode;
    string counterJsonStr = redisClient.getHash(key, field, errorCode);

    if (errorCode != RedisClient::RC_OK) {
        if (errorCode == RedisClient::RC_HASH_FIELD_NONEXIST) {
            valueExist = false;
            return counterMap;
        } 
        BS_LOG(ERROR, "get value from redis failed");
        return CounterMapPtr();
    }

    try {
        counterMap->FromJsonString(counterJsonStr);
    } catch (const ExceptionBase& e) {
        BS_LOG(WARN, "deserialize counterMap json[%s] for [%s] failed",
               counterJsonStr.c_str(), key.c_str());
        valueExist = false;
        return CounterMapPtr(new CounterMap());
    }

    valueExist = true;
    return counterMap;
}

bool CounterRedisSynchronizer::init(
        const CounterMapPtr& counterMap,
        const RedisInitParam &redisParam,
        const std::string &key,
        const std::string &field,
        int ttlInSecond)
{
    if (!CounterSynchronizer::init(counterMap)) {
        return false;
    }
    RedisClient redisClient;
    if (!redisClient.connect(redisParam)) {
        BS_LOG(ERROR, "connect redis failed. [%s]", redisParam.toString().c_str());
        return false;
    }
    _key = key;
    _field = field;
    _redisInitParam = redisParam;
    _ttlInSecond = ttlInSecond;
    return true;
}

bool CounterRedisSynchronizer::sync() const {
    RedisClient redisClient;
    if (!redisClient.connect(_redisInitParam)) {
        BS_LOG(ERROR, "connect redis failed. [%s]", _redisInitParam.toString().c_str());
        return false;
    }
    RedisClient::ErrorCode ec;
    redisClient.setHash(_key, _field, _counterMap->ToJsonString(), ec);
    if (ec != RedisClient::RC_OK) {
        BS_LOG(ERROR, "sync counter to redis failed, [errorCode: %d][errorMsg: %s]",
               ec, redisClient.getLastError().c_str());
        return false;
    }

    if (_ttlInSecond >= 0) {
        int ret = redisClient.expireKey(_key, _ttlInSecond, ec);
        if (ec != RedisClient::RC_OK) {
            BS_LOG(ERROR, "expire key [%s] failed, [errorCode: %d][errorMsg: %s]",
                   _key.c_str(), ec, redisClient.getLastError().c_str());
            return false;
        }

        if (ret == 0) {
            BS_LOG(ERROR, "expire key [%s] failed, key does not exists", _key.c_str());
            return false;
        }
    }
    return true;
}

}
}
