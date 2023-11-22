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

#include <string>

#include "build_service/config/BuilderConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/util/Log.h"
#include "indexlib/config/index_partition_options.h"

namespace build_service { namespace config {

class BuilderClusterConfig
{
public:
    BuilderClusterConfig();
    ~BuilderClusterConfig();

private:
    BuilderClusterConfig(const BuilderClusterConfig&);
    BuilderClusterConfig& operator=(const BuilderClusterConfig&);

public:
    bool init(const std::string& clusterName, const config::ResourceReader& resourceReader,
              const std::string& mergeConfigName = "");
    bool validate() const;

public:
    std::string clusterName;
    std::string tableName;
    BuilderConfig builderConfig;
    indexlib::config::IndexPartitionOptions indexOptions;
    bool overwriteSchemaName = false;
    bool enableFastSlowQueue = false;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::config
