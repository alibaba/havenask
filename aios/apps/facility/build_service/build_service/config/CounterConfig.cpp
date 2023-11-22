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
#include "build_service/config/CounterConfig.h"

#include <iosfwd>
#include <map>
#include <stdint.h>
#include <utility>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace build_service { namespace config {
BS_LOG_SETUP(config, CounterConfig);

CounterConfig::CounterConfig() : position("zookeeper") {}

CounterConfig::~CounterConfig() {}

void CounterConfig::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("position", position, position);
    json.Jsonize("parameters", params, params);
}

bool CounterConfig::validate() const
{
    if (position != "zookeeper" && position != "redis") {
        BS_LOG(ERROR, "invalid counter_position [%s], only support {%s, %s}", position.c_str(), "zookeeper", "redis");
        return false;
    }

    if (position == "redis") {
        auto it = params.find(COUNTER_PARAM_HOSTNAME);
        if (it == params.end()) {
            BS_LOG(ERROR, "missing [%s] in parameters", COUNTER_PARAM_HOSTNAME.c_str());
            return false;
        }
        it = params.find(COUNTER_PARAM_PORT);
        if (it == params.end()) {
            BS_LOG(ERROR, "missing [%s] in parameters", COUNTER_PARAM_PORT.c_str());
            return false;
        } else {
            uint16_t port;
            if (!StringUtil::fromString<uint16_t>(it->second, port)) {
                BS_LOG(ERROR, "invalid 'port' value [%s]", it->second.c_str());
                return false;
            }
        }
        if (params.find(COUNTER_PARAM_PASSWORD) == params.end()) {
            BS_LOG(ERROR, "missing [%s] in parameters", COUNTER_PARAM_PASSWORD.c_str());
            return false;
        }
        it = params.find(COUNTER_PARAM_REDIS_KEY_TTL);
        if (it != params.end()) {
            int32_t ttl;
            if (!StringUtil::fromString<int32_t>(it->second, ttl)) {
                BS_LOG(ERROR, "invalid [%s] value [%s], only integer is allowed", COUNTER_PARAM_REDIS_KEY_TTL.c_str(),
                       it->second.c_str());
                return false;
            }
        }
    }
    return true;
}

bool CounterConfig::operator==(const CounterConfig& other) const
{
    if (position != other.position) {
        return false;
    }
    if (params.size() != other.params.size()) {
        return false;
    }
    auto itl = params.begin();
    auto itr = other.params.begin();

    for (; itl != params.end(); ++itl, ++itr) {
        if (itl->second != itr->second) {
            return false;
        }
    }
    return true;
}

bool CounterConfig::operator!=(const CounterConfig& other) const { return !(*this == other); }

}} // namespace build_service::config
