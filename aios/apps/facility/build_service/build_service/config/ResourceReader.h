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

#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <stdint.h>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/Lock.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/GenerationMeta.h"
#include "build_service/config/GraphConfig.h"
#include "build_service/util/Log.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/function_executor_resource.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/updateable_schema_standards.h"

namespace build_service { namespace config {

class ResourceReader
{
public:
    enum ErrorCode {
        JSON_PATH_OK,
        JSON_PATH_NOT_EXIST,
        JSON_PATH_INVALID,
    };

    enum Status {
        OK,
        ERROR,
        FILE_NOT_EXIST,
        CONFIG_NOT_EXIST,
    };

public:
    ResourceReader(const std::string& configRoot);
    ~ResourceReader();

public:
    static bool parseRealtimeInfo(const std::string& realtimeInfo, KeyValueMap& kvMap);

public:
    // apis below will not enable cache
    bool getConfigContent(const std::string& relativePath, std::string& content) const;
    bool getFileContent(std::string& result, const std::string& fileName) const;
    void clearCache();

public:
    void init();
    bool isInit() const { return _isInit; }
    bool initGenerationMeta(const std::string& generationIndexRoot); // for searcher
    bool initGenerationMeta(generationid_t gid, const std::string& dataTable,
                            const std::string& indexRootDir); // for processor

    /**
     * example : processor_chain_config[0].table_name
     */
    template <typename T>
    bool getConfigWithJsonPath(const std::string& relativePath, const std::string& jsonPath, T& config) const;

    template <typename T>
    bool getConfigWithJsonPath(const std::string& relativePath, const std::string& jsonPath, T& config,
                               bool& isConfigExist) const;

    template <typename T>
    Status getConfigWithJsonPathReturnStatus(const std::string& relativePath, const std::string& jsonPath,
                                             T& config) const;

    bool getGraphConfig(GraphConfig& config) const;

    Status getGraphConfigReturnStatus(GraphConfig& config) const;

    bool getAgentGroupConfig(bool forGeneralTask, AgentGroupConfig& config) const;

    bool getRawDocumentFunctionResource(indexlib::config::FunctionExecutorResource& resource) const;

    template <typename T>
    bool getDataTableConfigWithJsonPath(const std::string& dataTableName, const std::string& jsonPath, T& config) const;

    template <typename T>
    bool getDataTableConfigWithJsonPath(const std::string& dataTableName, const std::string& jsonPath, T& config,
                                        bool& isExist) const;

    template <typename T>
    Status getDataTableConfigWithJsonPathReturnStatus(const std::string& dataTableName, const std::string& jsonPath,
                                                      T& config) const;

    template <typename T>
    bool getClusterConfigWithJsonPath(const std::string& clusterName, const std::string& jsonPath, T& config) const;

    template <typename T>
    bool getClusterConfigWithJsonPath(const std::string& clusterName, const std::string& jsonPath, T& config,
                                      bool& isExist) const;

    template <typename T>
    bool getTaskConfigWithJsonPath(const std::string& configPath, const std::string& clusterName,
                                   const std::string& taskName, const std::string& jsonPath, T& config, bool& isExist);

    const std::string& getGenerationMetaValue(const std::string& key) const;
    std::string getConfigPath() const;
    std::string getOriginalConfigPath() const { return _configRoot; }
    std::string getPluginPath() const;
    bool getAllClusters(const std::string& dataTable, std::vector<std::string>& clusterNames);
    bool getAllClusters(std::vector<std::string>& clusterNames);
    bool getAllDataTables(std::vector<std::string>& dataTables);
    bool getDataTableFromClusterName(const std::string& clusterName, std::string& dataTable);

    indexlib::config::IndexPartitionSchemaPtr getSchema(const std::string& clusterName) const;
    indexlib::config::IndexPartitionSchemaPtr getSchemaBySchemaTableName(const std::string& schemaName) const;
    indexlib::config::IndexPartitionSchemaPtr getSchemaByRelativeFileName(const std::string& schemaFileName) const;

    std::shared_ptr<indexlibv2::config::TabletOptions> getTabletOptions(const std::string& clusterName) const;
    std::shared_ptr<indexlibv2::config::TabletSchema> getTabletSchema(const std::string& clusterName) const;
    std::shared_ptr<indexlibv2::config::TabletSchema>
    getTabletSchemaBySchemaTableName(const std::string& schemaName) const;
    std::shared_ptr<indexlibv2::config::TabletSchema>
    getTabletSchemaByRelativeFileName(const std::string& schemaFileName) const;

    bool getSchemaPatch(const std::string& clusterName,
                        indexlib::config::UpdateableSchemaStandardsPtr& standards) const;
    bool getSchemaPatchBySchemaTableName(const std::string& tableName,
                                         indexlib::config::UpdateableSchemaStandardsPtr& standards) const;
    bool getSchemaPatchByRelativeFileName(const std::string& schemaPatchFileName,
                                          indexlib::config::UpdateableSchemaStandardsPtr& standards) const;

    bool getTableType(const std::string& clusterName, std::string& tableType) const;
    void updateWorkerProtocolVersion(int32_t protocolVersion);
    int32_t getWorkerProtocolVersion();

    bool getLuaFileContent(const std::string& luaFilePath, std::string& luaFileContent) const;
    /*
     * @return relative schema path, etc: schemas/cluster1_schema.json
     */
    std::vector<std::string> getAllSchemaFileNames() const;
    std::vector<std::string> getSchemaPatchFileNames(const std::vector<std::string>& tableNames) const;

public:
    static std::string getClusterConfRelativePath(const std::string& clusterName);
    static std::string getTaskConfRelativePath(const std::string& configPath, const std::string& clusterName,
                                               const std::string& taskName);

    static std::string getSchemaConfRelativePath(const std::string& tableName);
    static std::string getSchemaPatchConfRelativePath(const std::string& tableName);
    static std::string getDataTableRelativePath(const std::string& dataTableName);

    static std::string getLuaScriptFileRelativePath(const std::string& luaFileName);

    static std::string getTaskScriptFileRelativePath(const std::string& filePath);

    static bool getAllClusters(const std::string& configPath, const std::string& dataTable,
                               std::vector<std::string>& clusterNames);

public:
    // tools
    static ErrorCode getJsonNode(const std::string& jsonString, const std::string& jsonPath, autil::legacy::Any& any);
    static ErrorCode getJsonNode(const autil::legacy::json::JsonMap& jsonMap, const std::string& jsonPath,
                                 autil::legacy::Any& any);

public:
    // only for test
    static bool getJsonString(const std::string& jsonStr, const std::string& nodePath, std::string& result);

private:
    Status getJsonMap(const std::string& relativePath, autil::legacy::json::JsonMap& jsonMap) const;

private:
    std::string _configRoot;
    GenerationMeta _generationMeta;
    mutable std::map<std::string, autil::legacy::json::JsonMap> _jsonFileCache;
    mutable std::map<std::string, std::string> _luaFileCache;
    mutable std::set<std::string> _notExistFileCache;
    mutable autil::ThreadMutex _cacheMapLock;
    int64_t _maxZipSize;
    int32_t _workerProtocolVersion;
    std::vector<std::string> _allSchemaFileNames;
    bool _isInit;

private:
    friend class ResourceReaderManager;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ResourceReader);
///////////////////////////////////////////////////////
template <typename T>
bool ResourceReader::getConfigWithJsonPath(const std::string& relativePath, const std::string& jsonPath,
                                           T& config) const
{
    bool isConfigExist;
    return getConfigWithJsonPath(relativePath, jsonPath, config, isConfigExist);
}

template <typename T>
bool ResourceReader::getConfigWithJsonPath(const std::string& relativePath, const std::string& jsonPath, T& config,
                                           bool& isConfigExist) const
{
    Status status = getConfigWithJsonPathReturnStatus(relativePath, jsonPath, config);
    if (status == Status::OK) {
        isConfigExist = true;
        return true;
    } else if (status == Status::CONFIG_NOT_EXIST) {
        isConfigExist = false;
        return true;
    }
    return false;
}

template <typename T>
ResourceReader::Status ResourceReader::getConfigWithJsonPathReturnStatus(const std::string& relativePath,
                                                                         const std::string& jsonPath, T& config) const
{
    autil::legacy::json::JsonMap jsonMap;
    Status status = getJsonMap(relativePath, jsonMap);
    if (status != Status::OK) {
        return status;
    }

    autil::legacy::Any any;
    ErrorCode ec = getJsonNode(jsonMap, jsonPath, any);
    if (ec == JSON_PATH_NOT_EXIST) {
        return Status::CONFIG_NOT_EXIST;
    } else if (ec == JSON_PATH_INVALID) {
        return Status::ERROR;
    }
    try {
        FromJson(config, any);
    } catch (const autil::legacy::ExceptionBase& e) {
        std::stringstream ss;
        ss << "get config failed, relativePath[" << relativePath << "] jsonPath[" << jsonPath << "], exception["
           << std::string(e.what()) << "]";
        std::string errorMsg = ss.str();
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return Status::ERROR;
    }
    return Status::OK;
}

template <typename T>
bool ResourceReader::getClusterConfigWithJsonPath(const std::string& clusterName, const std::string& jsonPath,
                                                  T& config) const
{
    std::string clusterFile = getClusterConfRelativePath(clusterName);
    return getConfigWithJsonPath(clusterFile, jsonPath, config);
}

inline bool ResourceReader::getGraphConfig(GraphConfig& config) const
{
    bool isConfigExist = false;
    return getConfigWithJsonPath(GRAPH_CONFIG_FILE, "", config, isConfigExist);
}

inline ResourceReader::Status ResourceReader::getGraphConfigReturnStatus(GraphConfig& config) const
{
    return getConfigWithJsonPathReturnStatus(GRAPH_CONFIG_FILE, "", config);
}

inline bool ResourceReader::getAgentGroupConfig(bool forGeneralTask, AgentGroupConfig& config) const
{
    bool isConfigExist = false;
    std::string jsonPath = forGeneralTask ? "for_general_tasks" : "";
    return getConfigWithJsonPath(AGENT_GROUP_CONFIG_FILE, jsonPath, config, isConfigExist);
}

inline bool ResourceReader::getRawDocumentFunctionResource(indexlib::config::FunctionExecutorResource& resource) const
{
    bool isConfigExist = false;
    return getConfigWithJsonPath(RAW_DOC_FUNCTION_RESOURCE_FILE, "", resource, isConfigExist);
}

template <typename T>
bool ResourceReader::getClusterConfigWithJsonPath(const std::string& clusterName, const std::string& jsonPath,
                                                  T& config, bool& isConfigExist) const
{
    std::string clusterFile = getClusterConfRelativePath(clusterName);
    return getConfigWithJsonPath(clusterFile, jsonPath, config, isConfigExist);
}

template <typename T>
bool ResourceReader::getTaskConfigWithJsonPath(const std::string& configPath, const std::string& clusterName,
                                               const std::string& taskName, const std::string& jsonPath, T& config,
                                               bool& isExist)
{
    std::string taskFile = getTaskConfRelativePath(configPath, clusterName, taskName);
    return getConfigWithJsonPath(taskFile, jsonPath, config, isExist);
}

template <typename T>
bool ResourceReader::getDataTableConfigWithJsonPath(const std::string& dataTableName, const std::string& jsonPath,
                                                    T& config) const
{
    std::string dataTableFile = getDataTableRelativePath(dataTableName);
    return getConfigWithJsonPath(dataTableFile, jsonPath, config);
}

template <typename T>
bool ResourceReader::getDataTableConfigWithJsonPath(const std::string& dataTableName, const std::string& jsonPath,
                                                    T& config, bool& isExist) const
{
    std::string dataTableFile = getDataTableRelativePath(dataTableName);
    return getConfigWithJsonPath(dataTableFile, jsonPath, config, isExist);
}

template <typename T>
ResourceReader::Status ResourceReader::getDataTableConfigWithJsonPathReturnStatus(const std::string& dataTableName,
                                                                                  const std::string& jsonPath,
                                                                                  T& config) const
{
    std::string dataTableFile = getDataTableRelativePath(dataTableName);
    return getConfigWithJsonPathReturnStatus(dataTableFile, jsonPath, config);
}

}} // namespace build_service::config
