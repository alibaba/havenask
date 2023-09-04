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
#ifndef ISEARCH_BS_COUNTERREDISCOLLECTOR_H
#define ISEARCH_BS_COUNTERREDISCOLLECTOR_H

#include "build_service/admin/CounterCollector.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/util/RedisClient.h"

namespace build_service { namespace admin {

class CounterRedisCollector : public CounterCollector
{
public:
    CounterRedisCollector();
    ~CounterRedisCollector();

private:
    CounterRedisCollector(const CounterRedisCollector&);
    CounterRedisCollector& operator=(const CounterRedisCollector&);

public:
    bool init(const util::RedisInitParam& param, const std::string& redisKey);
    bool clearCounters() override;

private:
    bool doSyncCounters(RoleCounterMap& counterMap) override;

private:
    util::RedisInitParam _redisInitParam;
    std::string _redisKey;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CounterRedisCollector);

}} // namespace build_service::admin

#endif // ISEARCH_BS_COUNTERREDISCOLLECTOR_H
