/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "build_service/admin/CounterCollector.h"

#include "autil/TimeUtility.h"
#include "build_service/admin/CounterFileCollector.h"
#include "build_service/admin/CounterRedisCollector.h"
#include "build_service/util/RedisClient.h"
#include "fslib/util/LongIntervalLog.h"
#include "indexlib/misc/common.h"
#include "indexlib/util/counter/Counter.h"

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::util;

using namespace indexlib::util;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, CounterCollector);

CounterCollector::CounterCollector() : _lastSyncTime(0), _disbleSyncCounter(false) {}

CounterCollector::~CounterCollector() {}

CounterCollector* CounterCollector::create(const CounterConfig& counterConfig, const proto::BuildId& buildId)
{
    if (!counterConfig.validate()) {
        return nullptr;
    }
    if (counterConfig.position == "zookeeper") {
        auto collector = new CounterFileCollector();
        auto iter = counterConfig.params.find(COUNTER_PARAM_FILEPATH);
        if (iter == counterConfig.params.end()) {
            BS_LOG(ERROR, "[%s] missing in counter params", COUNTER_PARAM_FILEPATH.c_str());
            delete collector;
            return nullptr;
        }
        if (!collector->init(iter->second, buildId)) {
            delete collector;
            return nullptr;
        }
        return collector;
    } else if (counterConfig.position == "redis") {
        const auto& kvMap = counterConfig.params;
        RedisInitParam redisParam;
        if (kvMap.find(COUNTER_PARAM_HOSTNAME) == kvMap.end() || kvMap.find(COUNTER_PARAM_PORT) == kvMap.end() ||
            kvMap.find(COUNTER_PARAM_PASSWORD) == kvMap.end() || kvMap.find(COUNTER_PARAM_REDIS_KEY) == kvMap.end()) {
            BS_LOG(ERROR, "redis counter param [%s][%s][%s][%s] must be specified", COUNTER_PARAM_HOSTNAME.c_str(),
                   COUNTER_PARAM_PORT.c_str(), COUNTER_PARAM_PASSWORD.c_str(), COUNTER_PARAM_REDIS_KEY.c_str());
            return nullptr;
        }
        redisParam.hostname = kvMap.at(COUNTER_PARAM_HOSTNAME);
        redisParam.port = StringUtil::numberFromString<uint16_t>(kvMap.at(COUNTER_PARAM_PORT));
        redisParam.password = kvMap.at(COUNTER_PARAM_PASSWORD);
        auto collector = new CounterRedisCollector();
        if (!collector->init(redisParam, kvMap.at(COUNTER_PARAM_REDIS_KEY))) {
            delete collector;
            return nullptr;
        }
        return collector;
    }
    return nullptr;
}

int64_t CounterCollector::collect(CounterCollector::RoleCounterMap& counterMap)
{
    autil::ScopedLock lock(_lock);
    counterMap = _roleCounterMap;
    return (int64_t)_lastSyncTime;
}

bool CounterCollector::syncCounters(bool forceSync)
{
    FSLIB_LONG_INTERVAL_LOG("syncCounter for [%s]", _buildId.ShortDebugString().c_str());
    if (_disbleSyncCounter && !forceSync) {
        return true;
    }

    uint64_t currentTime = TimeUtility::currentTimeInSeconds();
    if (!forceSync && currentTime <= _lastSyncTime + 30) {
        return true;
    }
    RoleCounterMap counterMap;
    if (!doSyncCounters(counterMap)) {
        return false;
    }
    autil::ScopedLock lock(_lock);
    _roleCounterMap.swap(counterMap);
    _lastSyncTime = currentTime;
    return true;
}

void CounterCollector::fillRoleCounterMap(const string& roleName, const CounterMapPtr& counterMap,
                                          RoleCounterMap& roleCounterMap)
{
    std::vector<CounterBasePtr> counters = counterMap->FindCounters("");

    RoleCounterInfo counterInfo;
    counterInfo.set_rolename(roleName);
    for (size_t i = 0; i < counters.size(); i++) {
        CounterPtr counter = DYNAMIC_POINTER_CAST(Counter, counters[i]);
        if (counter) {
            CounterItem* counterItem = counterInfo.mutable_counteritem()->Add();
            counterItem->set_name(counters[i]->GetPath());
            counterItem->set_value(counter->Get());
        } else {
            BS_INTERVAL_LOG(60, WARN, "admin string type counter not collected.[%s]", counters[i]->GetPath().c_str());
        }
    }
    roleCounterMap[roleName] = counterInfo;
}

}} // namespace build_service::admin
