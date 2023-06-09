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
#ifndef ISEARCH_BS_PROCESSORCONFIGURATOR_H
#define ISEARCH_BS_PROCESSORCONFIGURATOR_H

#include "build_service/common_define.h"
#include "build_service/config/BuilderClusterConfig.h"
#include "build_service/config/ProcessorConfigReader.h"
#include "build_service/config/ProcessorRuleConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/SchemaUpdatableClusterConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/DataDescriptions.h"
#include "build_service/util/Log.h"

BS_DECLARE_REFERENCE_CLASS(config, SwiftConfig);
BS_DECLARE_REFERENCE_CLASS(config, BuildRuleConfig);

namespace build_service { namespace config {

class ProcessorConfigurator
{
public:
    ProcessorConfigurator();
    ~ProcessorConfigurator();

private:
    ProcessorConfigurator(const ProcessorConfigurator&);
    ProcessorConfigurator& operator=(const ProcessorConfigurator&);

public:
    static bool getSwiftWriterMaxBufferSize(const config::ProcessorConfigReaderPtr& resourceReader,
                                            const std::string& dataTableName, const std::string& configName,
                                            const std::string& clusterName, const proto::DataDescription& ds,
                                            uint64_t& maxBufferSize);

    static bool getAllClusters(const config::ResourceReaderPtr& resourceReader, const std::string& dataTable,
                               std::vector<std::string>& clusterNames);

    static bool getSchemaUpdatableClusters(const config::ResourceReaderPtr& resourceReader,
                                           const std::string& dataTable, std::vector<std::string>& clusterNames);

    static bool getSchemaNotUpdatableClusters(const config::ResourceReaderPtr& resourceReader,
                                              const std::string& dataTable, std::vector<std::string>& clusterNames);

    static bool getSchemaUpdatableClusterConfig(const config::ProcessorConfigReaderPtr& resourceReader,
                                                const std::string& dataTable, const std::string& clusterName,
                                                config::SchemaUpdatableClusterConfig& clusterConf);

    static bool
    getSchemaUpdatableClusterConfigVec(const config::ResourceReaderPtr& resourceReader, const std::string& dataTable,
                                       config::SchemaUpdatableClusterConfigVec& SchemaUpdatableClusterConfigs,
                                       bool& isConfigExist);

private:
    template <typename configType>
    static bool getConfig(const config::ResourceReaderPtr& resourceReader, const std::string& filePath,
                          const std::string& jsonPath, configType& config)
    {
        if (!resourceReader->getConfigWithJsonPath(filePath, jsonPath, config)) {
            BS_LOG(ERROR, "read [%s] from config path [%s] failed", jsonPath.c_str(), filePath.c_str());
            return false;
        }
        return true;
    }
    static bool getCustomizeWriterBufferSize(const config::SwiftConfig& swiftConfig, const std::string& dataTableName,
                                             const std::string& configName, const proto::DataDescription& ds,
                                             uint64_t& customizeWriterBufferSize);

    static uint64_t calculateTotalBuilderMemoryUse(const config::BuildRuleConfig& buildRuleConfig,
                                                   const config::BuilderClusterConfig& builderClusterConfig);

public:
    static const int64_t DEFAULT_WRITER_BUFFER_SIZE;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorConfigurator);

}} // namespace build_service::config

#endif // ISEARCH_BS_PROCESSORCONFIGURATOR_H
