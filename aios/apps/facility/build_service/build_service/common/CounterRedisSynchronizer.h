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
#pragma once

#include <stdint.h>
#include <string>

#include "build_service/common/CounterSynchronizer.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/util/RedisClient.h"
#include "indexlib/util/counter/CounterMap.h"

namespace build_service { namespace common {

class CounterRedisSynchronizer : public CounterSynchronizer
{
public:
    CounterRedisSynchronizer();
    ~CounterRedisSynchronizer();

private:
    CounterRedisSynchronizer(const CounterRedisSynchronizer&);
    CounterRedisSynchronizer& operator=(const CounterRedisSynchronizer&);

public:
    static indexlib::util::CounterMapPtr loadCounterMap(const util::RedisInitParam& redisParam, const std::string& key,
                                                        const std::string& field, bool& valueExist);

public:
    bool init(const indexlib::util::CounterMapPtr& counterMap, const util::RedisInitParam& redisParam,
              const std::string& key, const std::string& field, int32_t ttlInSecond = -1);

public:
    bool sync() const override;

private:
    util::RedisInitParam _redisInitParam;
    std::string _key;
    std::string _field;
    int32_t _ttlInSecond;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CounterRedisSynchronizer);

}} // namespace build_service::common
