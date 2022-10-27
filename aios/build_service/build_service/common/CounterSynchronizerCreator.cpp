#include "build_service/common/CounterSynchronizerCreator.h"
#include "build_service/common/CounterFileSynchronizer.h"
#include "build_service/common/CounterRedisSynchronizer.h"
#include "build_service/util/RedisClient.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
BS_NAMESPACE_USE(config);
BS_NAMESPACE_USE(util);
IE_NAMESPACE_USE(util);

namespace build_service {
namespace common {
BS_LOG_SETUP(common, CounterSynchronizerCreator);

CounterSynchronizerCreator::CounterSynchronizerCreator() {
}

CounterSynchronizerCreator::~CounterSynchronizerCreator() {
}

bool CounterSynchronizerCreator::validateCounterConfig(
        const CounterConfigPtr &counterConfig)
{
    if (!counterConfig) {
        BS_LOG(ERROR, "CounterConfig is NULL");
        return false;
    }

    if (!counterConfig->validate()) {
        BS_LOG(ERROR, "invalid CounterConfig");
        return false;
    }
    
    const auto &kvMap = counterConfig->params;
    if (counterConfig->position == "zookeeper") {
        auto it = kvMap.find(COUNTER_PARAM_FILEPATH);
        if (it == kvMap.end()) {
            BS_LOG(ERROR, "counter file path is missing in CounterConfig");
            return false;
        }
        return true;
    }

    if (counterConfig->position == "redis") {
        if (kvMap.find(COUNTER_PARAM_REDIS_KEY) == kvMap.end()) {
            BS_LOG(ERROR, "miss config [%s] in parameters", COUNTER_PARAM_REDIS_KEY.c_str());
            return false;
        }
        if (kvMap.find(COUNTER_PARAM_REDIS_FIELD) == kvMap.end()) {
            BS_LOG(ERROR, "miss config [%s] in parameters", COUNTER_PARAM_REDIS_FIELD.c_str());
            return false;
        }
        return true;
    }    
    BS_LOG(ERROR, "unsupported counter position[%s]", counterConfig->position.c_str());
    return false;
}

CounterMapPtr CounterSynchronizerCreator::loadCounterMap(
        const CounterConfigPtr &counterConfig, bool &loadFromExisted)
{
    if (!CounterSynchronizerCreator::validateCounterConfig(counterConfig)) {
        return CounterMapPtr();
    }
    
    const auto &kvMap = counterConfig->params;
    if (counterConfig->position == "zookeeper") {
        return CounterFileSynchronizer::loadCounterMap(
                kvMap.at(COUNTER_PARAM_FILEPATH), loadFromExisted);
    }
    if (counterConfig->position == "redis") {
        RedisInitParam redisParam;
        redisParam.hostname = kvMap.at(COUNTER_PARAM_HOSTNAME);
        redisParam.port = StringUtil::numberFromString<uint16_t>(kvMap.at(COUNTER_PARAM_PORT));
        redisParam.password = kvMap.at(COUNTER_PARAM_PASSWORD);
        return CounterRedisSynchronizer::loadCounterMap(redisParam,
                kvMap.at(COUNTER_PARAM_REDIS_KEY), kvMap.at(COUNTER_PARAM_REDIS_FIELD), loadFromExisted);
    }
    BS_LOG(ERROR, "invalid position [%s] in CounterConfig", counterConfig->position.c_str());
    return CounterMapPtr();
}

CounterSynchronizer* CounterSynchronizerCreator::create(const CounterMapPtr &counterMap,
        const CounterConfigPtr &counterConfig)
{
    if (!CounterSynchronizerCreator::validateCounterConfig(counterConfig)) {
        return NULL;
    }
    
    const auto &kvMap = counterConfig->params;    
    if (counterConfig->position == "zookeeper") {
        CounterFileSynchronizer *syner = new CounterFileSynchronizer();
        if (!syner->init(counterMap, kvMap.at(COUNTER_PARAM_FILEPATH))) {
            delete syner;
            return NULL;
        }
        return syner;
    }
    if (counterConfig->position == "redis") {
        RedisInitParam redisParam;
        redisParam.hostname = kvMap.at(COUNTER_PARAM_HOSTNAME);
        redisParam.port = StringUtil::numberFromString<uint16_t>(kvMap.at(COUNTER_PARAM_PORT));
        redisParam.password = kvMap.at(COUNTER_PARAM_PASSWORD);
        CounterRedisSynchronizer *syner = new CounterRedisSynchronizer();
        int32_t ttl = -1;
        auto it = kvMap.find(COUNTER_PARAM_REDIS_KEY_TTL);
        if (it != kvMap.end()) {
            ttl = StringUtil::numberFromString<int32_t>(it->second);
        }
        if (!syner->init(counterMap, redisParam, kvMap.at(COUNTER_PARAM_REDIS_KEY),
                         kvMap.at(COUNTER_PARAM_REDIS_FIELD), ttl))
        {
            delete syner;
            return NULL;
        }
        return syner;
    }
    return NULL;    
}

}
}
