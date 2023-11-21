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
#include "build_service/admin/CounterRedisCollector.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "build_service/util/ErrorLogCollector.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/counter/CounterMap.h"

using namespace std;
using namespace build_service::util;
using namespace indexlib::util;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, CounterRedisCollector);

CounterRedisCollector::CounterRedisCollector() {}

CounterRedisCollector::~CounterRedisCollector() {}

bool CounterRedisCollector::init(const RedisInitParam& param, const string& redisKey)
{
    _redisInitParam = param;
    _redisKey = redisKey;
    return true;
}

bool CounterRedisCollector::clearCounters()
{
    RedisClient redisClient;
    if (!redisClient.connect(_redisInitParam)) {
        BS_LOG(ERROR, "connect redis failed. [%s]", _redisInitParam.toString().c_str());
        return false;
    }

    RedisClient::ErrorCode errorCode;
    int delKeyCount = redisClient.removeKey(_redisKey, errorCode);
    if (errorCode != RedisClient::RC_OK) {
        BS_LOG(ERROR, "remove key [%s] failed, errorMsg [%s]", _redisKey.c_str(), redisClient.getLastError().c_str());
        return false;
    }
    if (delKeyCount == 0) {
        BS_LOG(WARN, "key [%s] not existed", _redisKey.c_str());
        return false;
    }
    BS_LOG(INFO, "key [%s] has been deleted", _redisKey.c_str());
    return true;
}

bool CounterRedisCollector::doSyncCounters(RoleCounterMap& roleCounterMap)
{
    RedisClient redisClient;
    if (!redisClient.connect(_redisInitParam)) {
        BS_LOG(ERROR, "connect redis failed. [%s]", _redisInitParam.toString().c_str());
        return false;
    }

    RedisClient::ErrorCode errorCode;
    auto counters = redisClient.getHash(_redisKey, errorCode);

    if (errorCode != RedisClient::RC_OK) {
        BS_LOG(ERROR, "get hash field from redis failed");
        return false;
    }

    for (const auto& counterItem : counters) {
        CounterMapPtr counterMap(new CounterMap());
        try {
            counterMap->FromJsonString(counterItem.second);
        } catch (ExceptionBase& e) {
            BS_LOG(WARN, "deserialize counter [%s] failed", counterItem.first.c_str());
            continue;
        }
        fillRoleCounterMap(counterItem.first, counterMap, roleCounterMap);
    }
    return true;
}

}} // namespace build_service::admin
