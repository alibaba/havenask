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
#include "ha3/sql/config/ExternalTableConfig.h"

#include "aios/network/gig/multi_call/config/FlowControlConfig.h"
#include "ha3/sql/common/common.h"

namespace isearch {
namespace sql {

static std::shared_ptr<autil::legacy::Jsonizable> createMiscConfig(const std::string &type) {
    if (type == EXTERNAL_TABLE_TYPE_HA3SQL) {
        return std::make_shared<Ha3ServiceMiscConfig>();
    }
    return nullptr;
}

void TableConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("schema_file", schemaFile);
    json.Jsonize("service_name", serviceName);
    json.Jsonize("database_name", dbName);
    json.Jsonize("table_name", tableName);
    json.Jsonize("allow_degraded_access", allowDegradedAccess, allowDegradedAccess);    
}

void GigConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("subscribe", subscribeConfig);
    json.Jsonize("flow_control", flowConfigMap);
}

void ServiceConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("type", type);
    json.Jsonize("engine_version", engineVersion, engineVersion);
    json.Jsonize("timeout_threshold_ms", timeoutThresholdMs, timeoutThresholdMs);
    if (json.GetMode() == FROM_JSON) {
        if ((miscConfig = createMiscConfig(type)) == nullptr) {
            AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException,
                               std::string("Invalid table type: ") + type);
        }
    }
    if (miscConfig) {
        json.Jsonize("misc_config", *miscConfig, *miscConfig);
    }
}

void Ha3ServiceMiscConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("auth_token", authToken);
    json.Jsonize("auth_key", authKey);
    json.Jsonize("auth_enabled", authEnabled, authEnabled);
}

void ExternalTableConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("gig_config", gigConfig);
    json.Jsonize("service_config", serviceConfigMap);
    json.Jsonize("table_config", tableConfigMap);
}

} // namespace sql
} // namespace isearch
