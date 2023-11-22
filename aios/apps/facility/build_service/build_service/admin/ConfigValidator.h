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

#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/SwiftConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/util/Log.h"
#include "indexlib/config/index_partition_schema.h"

namespace build_service { namespace admin {

class ConfigValidator : public proto::ErrorCollector
{
public:
    ConfigValidator();
    ConfigValidator(const proto::BuildId& buildId);
    ~ConfigValidator();

private:
    ConfigValidator(const ConfigValidator&);
    ConfigValidator& operator=(const ConfigValidator&);

public:
    enum SchemaUpdateStatus { UPDATE_ILLEGAL, UPDATE_SCHEMA, NO_UPDATE_SCHEMA, UPDATE_STANDARD };

    SchemaUpdateStatus checkUpdateSchema(const std::string& originalConfigPath, const std::string& newConfigPath,
                                         const std::string& clusterName);
    SchemaUpdateStatus checkUpdateSchema(const config::ResourceReaderPtr& originalResourceReader,
                                         const config::ResourceReaderPtr& newResourceReader,
                                         const std::string& clusterName);

    bool validate(const std::string& configPath, bool firstTime);
    std::string getErrorMsg();

private:
    // todo: for build job, bs_hippo.json is not required
    bool validateBuilderClusterConfig(const std::string& configPath, const std::string& clusterName);
    bool validateOfflineIndexPartitionOptions(const std::string& configPath, const std::string& clusterName);
    bool validateAppPlanMaker(const std::string& configPath);
    bool validateSchema(const config::ResourceReaderPtr& resourceReader, const std::string& clusterName,
                        bool buildV2Mode);
    bool validateSwiftMemoryPreferMode(const config::SwiftConfig& swiftConfig,
                                       const config::BuildRuleConfig& buildRuleConfig, bool buildV2Mode);
    bool validateSwiftConfig(const config::SwiftConfig& swiftConfig,
                             const config::BuildServiceConfig& buildServiceConfig, bool buildV2Mode);

    bool validateHashMode(const config::ResourceReaderPtr& resourceReader, const std::string& configPath);
    template <typename T>
    bool getValidateConfig(const config::ResourceReader& resourceReader, const std::string& filePath,
                           const std::string& jsonPath, T& configPath);

    bool isSchemaEqual(const indexlib::config::IndexPartitionSchemaPtr& originalSchema,
                       const indexlib::config::IndexPartitionSchemaPtr& updateSchema);

private:
    proto::BuildId _buildId;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::admin
