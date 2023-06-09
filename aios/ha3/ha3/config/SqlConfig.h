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

#include <stddef.h>
#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/legacy/jsonizable.h"
#include "ha3/sql/ops/agg/SqlAggPluginConfig.h"
#include "ha3/sql/ops/tvf/SqlTvfPluginConfig.h"
#include "ha3/sql/resource/SqlConfig.h"
#include "iquan/config/ClientConfig.h"
#include "iquan/config/JniConfig.h"
#include "iquan/config/KMonConfig.h"
#include "iquan/config/WarmupConfig.h"
#include "ha3/sql/config/SwiftWriteConfig.h"
#include "ha3/sql/config/TableWriteConfig.h"
#include "ha3/sql/config/AuthenticationConfig.h"

namespace isearch {
namespace config {

class SqlConfig : public autil::legacy::Jsonizable {
public:
    SqlConfig()
    {}
    ~SqlConfig() {}
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        sqlConfig.Jsonize(json);
        json.Jsonize("sql_agg_plugin_config", sqlAggPluginConfig, sqlAggPluginConfig);
        json.Jsonize("sql_tvf_plugin_config", sqlTvfPluginConfig, sqlTvfPluginConfig);
        json.Jsonize("iquan_client_config", clientConfig, clientConfig);
        json.Jsonize("iquan_jni_config", jniConfig, jniConfig);
        json.Jsonize("iquan_warmup_config", warmupConfig, warmupConfig);
        json.Jsonize("iquan_kmon_config", kmonConfig, kmonConfig);
        json.Jsonize("authentication_config", authenticationConfig, authenticationConfig);
        json.Jsonize("swift_writer_config", swiftWriterConfig, swiftWriterConfig);
        json.Jsonize("table_writer_config", tableWriterConfig, tableWriterConfig);
    }
public:
    sql::SqlConfig sqlConfig;
    sql::SqlAggPluginConfig sqlAggPluginConfig;
    sql::SqlTvfPluginConfig sqlTvfPluginConfig;
    sql::AuthenticationConfig authenticationConfig;
    sql::SwiftWriteConfig swiftWriterConfig;
    sql::TableWriteConfig tableWriterConfig;
    iquan::ClientConfig clientConfig;
    iquan::JniConfig jniConfig;
    iquan::WarmupConfig warmupConfig;
    iquan::KMonConfig kmonConfig;
    // todo: add iquan::ExecConfig
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SqlConfig> SqlConfigPtr;

} // namespace config
} // namespace isearch
