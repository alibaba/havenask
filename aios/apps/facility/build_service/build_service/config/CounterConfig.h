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
#ifndef ISEARCH_BS_COUNTERCONFIG_H
#define ISEARCH_BS_COUNTERCONFIG_H

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

static const std::string COUNTER_CONFIG_POSITION = "position"; // default : "zookeeper"
static const std::string COUNTER_CONFIG_PARAMS = "parameters";
static const std::string COUNTER_PARAM_HOSTNAME = "hostname";
static const std::string COUNTER_PARAM_PORT = "port";
static const std::string COUNTER_PARAM_PASSWORD = "password";
static const std::string COUNTER_PARAM_FILEPATH = "path";
static const std::string COUNTER_PARAM_REDIS_KEY = "key";
static const std::string COUNTER_PARAM_REDIS_KEY_TTL =
    "key_ttl"; // key expire time in seconds; set it to negative can make key never expires
static const std::string COUNTER_PARAM_REDIS_FIELD = "field";

class CounterConfig : public autil::legacy::Jsonizable
{
public:
    CounterConfig();
    ~CounterConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool validate() const;
    bool operator==(const CounterConfig& other) const;
    bool operator!=(const CounterConfig& other) const;

public:
    std::string position;
    KeyValueMap params;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CounterConfig);

}} // namespace build_service::config

#endif // ISEARCH_BS_COUNTERCONFIG_H
