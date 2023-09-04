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
#include "build_service/config/ResourceReader.h"

#include "autil/EnvUtil.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ConfigParser.h"
#include "build_service/config/DocProcessorChainConfig.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/ZipFileUtil.h"
#include "fslib/fslib.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/ITabletFactory.h"
#include "indexlib/framework/TabletFactoryCreator.h"
#include "indexlib/framework/TabletSchemaLoader.h"

using namespace indexlib::config;

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace fslib;
using namespace fslib::fs;
using namespace build_service::util;
using namespace fslib::util;
namespace build_service { namespace config {
BS_LOG_SETUP(config, ResourceReader);

ResourceReader::ResourceReader(const string& configRoot)
    : _configRoot(configRoot)
    , _maxZipSize(1024 * 1024 * 10)
    , _workerProtocolVersion(UNKNOWN_WORKER_PROTOCOL_VERSION)
    , _isInit(false)
{
    string param = autil::EnvUtil::getEnv(BS_ENV_ENABLE_RESOURCE_READER_MAX_ZIP_SIZE.c_str());
    int64_t maxZipSize;
    if (!param.empty() && StringUtil::fromString(param, maxZipSize)) {
        _maxZipSize = maxZipSize * 1024;
    }
}

ResourceReader::~ResourceReader() {}

bool ResourceReader::parseRealtimeInfo(const string& realtimeInfo, KeyValueMap& kvMap)
{
    JsonMap jsonMap;
    try {
        FromJsonString(jsonMap, realtimeInfo);
    } catch (const ExceptionBase& e) {
        string errorMsg = "jsonize " + realtimeInfo + " failed, exception: " + string(e.what());
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    for (auto iter : jsonMap) {
        const string& key = iter.first;
        string value;
        try {
            FromJson(value, iter.second);
        } catch (const ExceptionBase& e) {
            BS_LOG(ERROR, "fail to parse json map[%s].", ToJsonString(jsonMap).c_str());
            return false;
        }
        kvMap[key] = value;
    }
    return true;
}

void ResourceReader::init()
{
    // init method should block getJsonMap to avoid redundant access
    autil::ScopedLock lock(_cacheMapLock);
    if (_isInit) {
        return;
    }
    auto loadZipFile = [this](const string& srcFile, bool isSchema) {
        unordered_map<string, string> fileMap;
        int64_t fileLen;
        if (!fslib::util::FileUtil::getFileLength(srcFile, fileLen, false)) {
            BS_LOG(INFO, "get file len for [%s] failed.", srcFile.c_str());
            return;
        }
        if (fileLen > _maxZipSize) {
            BS_LOG(WARN, "size of [%s] is greater then bs_resource_reader_max_zip_size[%ld].", srcFile.c_str(),
                   _maxZipSize);
            return;
        }
        if (!ZipFileUtil::readZipFile(srcFile, fileMap)) {
            BS_LOG(INFO, "read [%s] failed.", srcFile.c_str());
            return;
        }
        ConfigParser parser(_configRoot, fileMap);
        for (auto it = fileMap.begin(); it != fileMap.end(); it++) {
            JsonMap jsonMap;
            if (parser.parse(it->first, jsonMap)) {
                _jsonFileCache[it->first] = jsonMap;
            }
            if (isSchema) {
                _allSchemaFileNames.push_back(it->first);
            }
        }
    };

    auto loadLuaZipFile = [this](const string& srcFile) {
        int64_t fileLen;
        if (!fslib::util::FileUtil::getFileLength(srcFile, fileLen, false)) {
            BS_LOG(INFO, "get file len for [%s] failed.", srcFile.c_str());
            return;
        }
        if (fileLen > _maxZipSize) {
            BS_LOG(WARN, "size of [%s] is greater then bs_resource_reader_max_zip_size[%ld].", srcFile.c_str(),
                   _maxZipSize);
            return;
        }
        unordered_map<string, string> fileMap;
        if (!ZipFileUtil::readZipFile(srcFile, fileMap)) {
            BS_LOG(WARN, "read [%s] failed.", srcFile.c_str());
            return;
        }
        for (auto it = fileMap.begin(); it != fileMap.end(); it++) {
            _luaFileCache[it->first] = it->second;
        }
    };

    string clustersZipFile = fslib::util::FileUtil::joinFilePath(_configRoot, CLUSTER_CONFIG_PATH + ".zip");
    string schemasZipFile = fslib::util::FileUtil::joinFilePath(_configRoot, SCHEMA_CONFIG_PATH + ".zip");
    string luaZipFile = fslib::util::FileUtil::joinFilePath(_configRoot, LUA_CONFIG_PATH + ".zip");
    loadZipFile(clustersZipFile, false);
    loadZipFile(schemasZipFile, true);
    loadLuaZipFile(luaZipFile);
    _isInit = true;
}

bool ResourceReader::initGenerationMeta(const string& generationIndexRoot)
{
    if (!generationIndexRoot.empty()) {
        GenerationMeta meta;
        if (!meta.loadFromFile(generationIndexRoot)) {
            return false;
        }
        _generationMeta = meta;
    }
    return true;
}

bool ResourceReader::initGenerationMeta(generationid_t gid, const string& dataTable, const string& indexRootDir)
{
    vector<string> clusterNames;
    if (!getAllClusters(dataTable, clusterNames)) {
        string errorMsg = "getAllClusters for dataTable[" + dataTable + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    for (size_t i = 0; i < clusterNames.size(); i++) {
        const string& clusterName = clusterNames[i];
        string generationIndexRoot = IndexPathConstructor::getGenerationIndexPath(indexRootDir, clusterName, gid);
        GenerationMeta meta;
        if (!meta.loadFromFile(generationIndexRoot)) {
            string errorMsg = "load generation meta for cluster[" + clusterName + "] failed";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        if (i == 0) {
            _generationMeta = meta;
        } else if (meta != _generationMeta) {
            string errorMsg = "generation meta of all clusters from one data_table should be same";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }
    return true;
}

const string& ResourceReader::getGenerationMetaValue(const string& key) const { return _generationMeta.getValue(key); }

string ResourceReader::getConfigPath() const { return fslib::util::FileUtil::normalizeDir(_configRoot); }

string ResourceReader::getPluginPath() const
{
    return fslib::util::FileUtil::joinFilePath(getConfigPath(), "plugins") + '/';
}

ResourceReader::ErrorCode ResourceReader::getJsonNode(const string& jsonString, const string& jsonPath,
                                                      autil::legacy::Any& any)
{
    vector<string> jsonPathVec = StringTokenizer::tokenize(
        StringView(jsonPath), '.', StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    any = json::ParseJson(jsonString);
    for (size_t i = 0; i < jsonPathVec.size(); ++i) {
        size_t pos = jsonPathVec[i].find('[');
        string nodeName;
        if (pos == string::npos) {
            nodeName = jsonPathVec[i];
        } else {
            nodeName = jsonPathVec[i].substr(0, pos);
        }
        JsonMap jsonMap = AnyCast<JsonMap>(any);
        JsonMap::const_iterator it = jsonMap.find(nodeName);
        if (it == jsonMap.end()) {
            BS_LOG(DEBUG,
                   "get config by json path failed."
                   "key[%s] not found in json map.",
                   nodeName.c_str());
            ERROR_COLLECTOR_LOG(DEBUG,
                                "get config by json path failed."
                                "key[%s] not found in json map.",
                                nodeName.c_str());

            return JSON_PATH_NOT_EXIST;
        }
        any = it->second;
        while (pos != string::npos) {
            size_t end = jsonPathVec[i].find(']', pos);
            if (end == string::npos) {
                string errorMsg =
                    "get config by json path failed.Invalid json path[" + jsonPathVec[i] + "], missing ']'.";
                BS_LOG(WARN, "%s", errorMsg.c_str());
                return JSON_PATH_INVALID;
            }

            size_t idx;
            string idxStr = jsonPathVec[i].substr(pos + 1, end - pos - 1);
            if (!StringUtil::fromString<size_t>(idxStr, idx)) {
                string errorMsg = "get config by json path failed.Invalid json vector idx[" + idxStr + "], .";
                BS_LOG(WARN, "%s", errorMsg.c_str());
                return JSON_PATH_INVALID;
            }
            JsonArray jsonVec = AnyCast<JsonArray>(any);
            if (idx >= jsonVec.size()) {
                stringstream ss;
                ss << "get config by json path failed.[" << jsonPathVec[i] << "] out of range, total size["
                   << jsonVec.size() << "].";
                string errorMsg = ss.str();
                BS_LOG(WARN, "%s", errorMsg.c_str());
                return JSON_PATH_INVALID;
            }
            any = jsonVec[idx];
            pos = jsonPathVec[i].find('[', end + 1);
        }
    }
    return JSON_PATH_OK;
}

ResourceReader::ErrorCode ResourceReader::getJsonNode(const JsonMap& jsonMap, const string& jsonPath, Any& any)
{
    return getJsonNode(ToJsonString(jsonMap), jsonPath, any);
}

ResourceReader::Status ResourceReader::getJsonMap(const string& relativePath, JsonMap& jsonMap) const
{
    autil::ScopedLock lock(_cacheMapLock);
    auto iter = _jsonFileCache.find(relativePath);
    if (iter != _jsonFileCache.end()) {
        jsonMap = iter->second;
        return Status::OK;
    }
    if (_notExistFileCache.find(relativePath) != _notExistFileCache.end()) {
        return Status::FILE_NOT_EXIST;
    }
    ConfigParser parser(_configRoot);
    bool isNotExist = false;
    if (parser.parseRelativePath(relativePath, jsonMap, &isNotExist)) {
        _jsonFileCache[relativePath] = jsonMap;
        return Status::OK;
    }
    if (isNotExist) {
        _notExistFileCache.insert(relativePath);
        return Status::FILE_NOT_EXIST;
    }
    return Status::ERROR;
}

bool ResourceReader::getAllClusters(const string& dataTable, vector<string>& clusters)
{
    bool exist = false;
    if (!getDataTableConfigWithJsonPath(dataTable, "clusters", clusters, exist)) {
        string errorMsg = "Get clusters for data table[" + dataTable + "] failed.";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    if (exist) {
        return true;
    }
    DocProcessorChainConfigVec chainConfig;
    if (!getDataTableConfigWithJsonPath(dataTable, "processor_chain_config", chainConfig)) {
        string errorMsg = "Get processor_chain_config for data table[" + dataTable + "] failed.";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    for (size_t i = 0; i < chainConfig.size(); ++i) {
        clusters.insert(clusters.end(), chainConfig[i].clusterNames.begin(), chainConfig[i].clusterNames.end());
    }
    sort(clusters.begin(), clusters.end());
    clusters.resize(distance(clusters.begin(), unique(clusters.begin(), clusters.end())));
    return true;
}

bool ResourceReader::getAllClusters(std::vector<std::string>& clusterNames)
{
    vector<string> dataTables;
    if (!getAllDataTables(dataTables)) {
        string errorMsg = "get all data tables failed.";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    for (size_t i = 0; i < dataTables.size(); ++i) {
        if (!getAllClusters(dataTables[i], clusterNames)) {
            return false;
        }
    }
    return true;
}

bool ResourceReader::getDataTableFromClusterName(const string& clusterName, string& dataTable)
{
    vector<string> dataTables;
    if (!getAllDataTables(dataTables)) {
        string errorMsg = "get data table for cluster[" + clusterName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    for (size_t i = 0; i < dataTables.size(); ++i) {
        vector<string> clusterNames;
        if (!getAllClusters(dataTables[i], clusterNames)) {
            string errorMsg = "get data table for cluster[" + clusterName + "] failed";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        if (find(clusterNames.begin(), clusterNames.end(), clusterName) != clusterNames.end()) {
            dataTable = dataTables[i];
            return true;
        }
    }
    string errorMsg = "get data table for cluster[" + clusterName + "] failed";
    BS_LOG(ERROR, "%s", errorMsg.c_str());
    return false;
}

bool ResourceReader::getAllDataTables(std::vector<std::string>& dataTables)
{
    FileList dataTableFiles;
    string dataTableConfPath = fslib::util::FileUtil::joinFilePath(_configRoot, DATA_TABLE_PATH);
    fslib::ErrorCode ec = FileSystem::listDir(dataTableConfPath, dataTableFiles);
    if (ec != EC_OK) {
        string errorMsg = "get all data tables failed : list dir[" + dataTableConfPath + "] failed.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    for (size_t i = 0; i < dataTableFiles.size(); ++i) {
        if (dataTableFiles[i].size() < DATA_TABLE_JSON_FILE_SUFFIX.size()) {
            continue;
        }
        size_t dataTableNameEnd = dataTableFiles[i].size() - DATA_TABLE_JSON_FILE_SUFFIX.size();
        if (dataTableNameEnd == 0) {
            continue;
        }
        if (dataTableFiles[i].substr(dataTableNameEnd) != DATA_TABLE_JSON_FILE_SUFFIX) {
            continue;
        }
        dataTables.push_back(dataTableFiles[i].substr(0, dataTableNameEnd));
    }
    return true;
}

string ResourceReader::getLuaScriptFileRelativePath(const string& luaFileName)
{
    return LUA_CONFIG_PATH + "/" + luaFileName;
}

string ResourceReader::getTaskScriptFileRelativePath(const string& filePath)
{
    return TASK_SCRIPTS_CONFIG_PATH + "/" + filePath;
}

string ResourceReader::getClusterConfRelativePath(const string& clusterName)
{
    return CLUSTER_CONFIG_PATH + "/" + clusterName + CLUSTER_JSON_FILE_SUFFIX;
}

string ResourceReader::getTaskConfRelativePath(const string& configPath, const string& clusterName,
                                               const string& taskName)
{
    string customizedTaskConfigPath = CLUSTER_CONFIG_PATH + "/" + clusterName + "_" + taskName + TASK_CONFIG_SUFFIX;
    string taskConfigPath = CLUSTER_CONFIG_PATH + "/" + taskName + TASK_CONFIG_SUFFIX;
    string taskConfigFile = FileUtil::joinFilePath(configPath, customizedTaskConfigPath);
    bool exist;
    if (!FileUtil::isFile(taskConfigFile, exist)) {
        BS_LOG(ERROR, "check whether [%s] is existed failed.", taskConfigFile.c_str());
        return "";
    }
    if (exist) {
        return customizedTaskConfigPath;
    } else {
        return taskConfigPath;
    }
}

string ResourceReader::getSchemaConfRelativePath(const string& tableName)
{
    return SCHEMA_CONFIG_PATH + "/" + tableName + SCHEMA_JSON_FILE_SUFFIX;
}

string ResourceReader::getSchemaPatchConfRelativePath(const string& tableName)
{
    return SCHEMA_CONFIG_PATH + "/" + tableName + SCHEMA_PATCH_JSON_FILE_SUFFIX;
}

string ResourceReader::getDataTableRelativePath(const std::string& dataTableName)
{
    return DATA_TABLE_PATH + "/" + dataTableName + DATA_TABLE_JSON_FILE_SUFFIX;
}

bool ResourceReader::getAllClusters(const std::string& configPath, const std::string& dataTable,
                                    std::vector<std::string>& clusterNames)
{
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(configPath);
    return resourceReader->getAllClusters(dataTable, clusterNames);
}

IndexPartitionSchemaPtr ResourceReader::getSchemaBySchemaTableName(const string& tableName) const
{
    string schemaFileName = getSchemaConfRelativePath(tableName);
    return getSchemaByRelativeFileName(schemaFileName);
}

std::shared_ptr<indexlibv2::config::TabletOptions>
ResourceReader::getTabletOptions(const std::string& clusterName) const
{
    JsonMap jsonMap;
    auto clusterFileName = getClusterConfRelativePath(clusterName);
    auto status = getJsonMap(clusterFileName, jsonMap);
    if (status != Status::OK) {
        AUTIL_LOG(ERROR, "get json map from [%s] failed", clusterFileName.c_str());
        return nullptr;
    }

    auto options = std::make_shared<indexlibv2::config::TabletOptions>();
    try {
        FromJson(*options, jsonMap);
        return options;
    } catch (const ExceptionBase& e) {
        BS_LOG(ERROR, "parse TabletOptions from %s failed, error: %s", ToJsonString(jsonMap).c_str(), e.what());
        return nullptr;
    }
}

std::shared_ptr<indexlibv2::config::TabletSchema>
ResourceReader::getTabletSchemaBySchemaTableName(const string& tableName) const
{
    string schemaFileName = getSchemaConfRelativePath(tableName);
    return getTabletSchemaByRelativeFileName(schemaFileName);
}

bool ResourceReader::getSchemaPatchBySchemaTableName(const string& tableName,
                                                     UpdateableSchemaStandardsPtr& standards) const
{
    string schemaPatchFileName = getSchemaPatchConfRelativePath(tableName);
    return getSchemaPatchByRelativeFileName(schemaPatchFileName, standards);
}

std::shared_ptr<indexlibv2::config::TabletSchema>
ResourceReader::getTabletSchemaByRelativeFileName(const string& schemaFileName) const
{
    try {
        JsonMap jsonMap;
        if (getJsonMap(schemaFileName, jsonMap) != Status::OK) {
            BS_LOG(ERROR, "get schema[%s] json map failed", schemaFileName.c_str());
            return nullptr;
        }
        std::string jsonStr = ToJsonString(jsonMap);
        auto tabletSchema = indexlibv2::framework::TabletSchemaLoader::LoadSchema(jsonStr);
        if (!tabletSchema) {
            BS_LOG(ERROR, "load schema[%s] json map failed", schemaFileName.c_str());
            return nullptr;
        }
        auto status = indexlibv2::framework::TabletSchemaLoader::ResolveSchema(nullptr, "", tabletSchema.get());
        if (!status.IsOK()) {
            BS_LOG(ERROR, "resolve schema failed, [%s]", schemaFileName.c_str());
            return nullptr;
        }
        return tabletSchema;
    } catch (const exception& e) {
        string errorMsg = "load " + schemaFileName + " failed, exception[" + string(e.what()) + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return nullptr;
    } catch (...) {
        BS_LOG(ERROR, "unknown exception");
        return nullptr;
    }
    return nullptr;
}

IndexPartitionSchemaPtr ResourceReader::getSchemaByRelativeFileName(const string& schemaFileName) const
{
    try {
        JsonMap jsonMap;
        if (getJsonMap(schemaFileName, jsonMap) != Status::OK) {
            BS_LOG(ERROR, "get schema json map [%s] failed", schemaFileName.c_str());
            return nullptr;
        }
        IndexPartitionSchemaPtr schema;
        std::string jsonStr = ToJsonString(jsonMap);
        if (!indexlibv2::config::TabletSchema::LoadLegacySchema(jsonStr, schema)) {
            BS_LOG(ERROR, "load legacy schema [%s] failed", schemaFileName.c_str());
            return nullptr;
        }
        if (!schema) {
            BS_LOG(ERROR, "no schema [%s] failed", schemaFileName.c_str());
        }
        return schema;
    } catch (const exception& e) {
        string errorMsg = "load " + schemaFileName + " failed, exception[" + string(e.what()) + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return nullptr;
    } catch (...) {
        BS_LOG(ERROR, "unknown exception");
        return nullptr;
    }
    return nullptr;
}

bool ResourceReader::getSchemaPatchByRelativeFileName(const string& schemaPatchFileName,
                                                      UpdateableSchemaStandardsPtr& standards) const
{
    try {
        JsonMap jsonMap;
        if (getJsonMap(schemaPatchFileName, jsonMap) != Status::OK) {
            standards = UpdateableSchemaStandardsPtr();
            return true;
        }
        FromJson(*standards, jsonMap);
        return true;
    } catch (const exception& e) {
        string errorMsg = "load " + schemaPatchFileName + " failed, exception[" + string(e.what()) + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        standards = UpdateableSchemaStandardsPtr();
        return false;
    } catch (...) {
        BS_LOG(ERROR, "unknown exception");
        standards = UpdateableSchemaStandardsPtr();
        return false;
    }
    standards = UpdateableSchemaStandardsPtr();
    return false;
}

IndexPartitionSchemaPtr ResourceReader::getSchema(const string& clusterName) const
{
    string tableName;
    if (!getClusterConfigWithJsonPath(clusterName, "cluster_config.table_name", tableName)) {
        BS_LOG(ERROR, "parse table_name for %s failed", clusterName.c_str());
        return nullptr;
    }
    return getSchemaBySchemaTableName(tableName);
}

std::shared_ptr<indexlibv2::config::TabletSchema> ResourceReader::getTabletSchema(const string& clusterName) const
{
    string tableName;
    if (!getClusterConfigWithJsonPath(clusterName, "cluster_config.table_name", tableName)) {
        BS_LOG(ERROR, "parse table_name for [%s] failed", clusterName.c_str());
        return nullptr;
    }
    string schemaFileName = getSchemaConfRelativePath(tableName);
    try {
        JsonMap jsonMap;
        if (getJsonMap(schemaFileName, jsonMap) != Status::OK) {
            BS_LOG(ERROR, "get schema[%s] json map failed", schemaFileName.c_str());
            return nullptr;
        }
        bool overwriteSchemaName = false;
        if (!getClusterConfigWithJsonPath(clusterName, "cluster_config.overwrite_schema_name", overwriteSchemaName)) {
            BS_LOG(ERROR, "get cluster_config.overwrite_schema_name for schema[%s] failed", schemaFileName.c_str());
            return nullptr;
        }
        if (overwriteSchemaName) {
            jsonMap["table_name"] = clusterName;
        }
        std::string jsonStr = ToJsonString(jsonMap);
        auto tabletSchema = indexlibv2::framework::TabletSchemaLoader::LoadSchema(jsonStr);
        if (!tabletSchema) {
            BS_LOG(ERROR, "load schema[%s] json map failed", schemaFileName.c_str());
            return nullptr;
        }
        auto status = indexlibv2::framework::TabletSchemaLoader::ResolveSchema(nullptr, "", tabletSchema.get());
        if (!status.IsOK()) {
            BS_LOG(ERROR, "resolve schema failed, [%s]", schemaFileName.c_str());
            return nullptr;
        }
        return tabletSchema;
    } catch (const exception& e) {
        string errorMsg = "load " + schemaFileName + " failed, exception[" + string(e.what()) + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return nullptr;
    } catch (...) {
        BS_LOG(ERROR, "unknown exception");
        return nullptr;
    }
    return nullptr;
}

bool ResourceReader::getSchemaPatch(const std::string& clusterName, UpdateableSchemaStandardsPtr& standards) const
{
    string tableName;
    if (!getClusterConfigWithJsonPath(clusterName, "cluster_config.table_name", tableName)) {
        BS_LOG(ERROR, "parse table_name for %s failed", clusterName.c_str());
        standards = UpdateableSchemaStandardsPtr();
        return false;
    }
    return getSchemaPatchBySchemaTableName(tableName, standards);
}

bool ResourceReader::getTableType(const std::string& clusterName, std::string& tableType) const
{
    auto schema = getTabletSchema(clusterName);
    if (!schema) {
        return false;
    }
    tableType = schema->GetTableType();
    return true;
}

bool ResourceReader::getJsonString(const string& jsonStr, const string& nodePath, std::string& result)
{
    result = "";
    autil::legacy::Any any;
    ResourceReader::ErrorCode ec = ResourceReader::getJsonNode(jsonStr, nodePath, any);
    if (ec == ResourceReader::JSON_PATH_NOT_EXIST) {
        return false;
    } else if (ec == ResourceReader::JSON_PATH_INVALID) {
        return false;
    }
    result = autil::legacy::json::ToString(any);
    return true;
}

bool ResourceReader::getConfigContent(const string& relativePath, string& content) const
{
    string absPath = fslib::util::FileUtil::joinFilePath(_configRoot, relativePath);
    return fslib::util::FileUtil::readFile(absPath, content);
}

bool ResourceReader::getFileContent(string& result, const string& fileName) const
{
    return getConfigContent(fileName, result);
}

bool ResourceReader::getLuaFileContent(const string& luaFilePath, string& luaFileContent) const
{
    {
        autil::ScopedLock lock(_cacheMapLock);
        auto iter = _luaFileCache.find(luaFilePath);
        if (iter != _luaFileCache.end()) {
            luaFileContent = iter->second;
            return true;
        }
    }

    if (!getFileContent(luaFileContent, luaFilePath)) {
        return false;
    }
    autil::ScopedLock lock(_cacheMapLock);
    _luaFileCache[luaFilePath] = luaFileContent;
    return true;
}

void ResourceReader::updateWorkerProtocolVersion(int32_t protocolVersion)
{
    if (_workerProtocolVersion == UNKNOWN_WORKER_PROTOCOL_VERSION) {
        _workerProtocolVersion = protocolVersion;
    }
}

int32_t ResourceReader::getWorkerProtocolVersion() { return _workerProtocolVersion; }

vector<string> ResourceReader::getAllSchemaFileNames() const
{
    if (!_allSchemaFileNames.empty()) {
        return _allSchemaFileNames;
    }
    vector<string> files;
    string schemaDir = fslib::util::FileUtil::joinFilePath(_configRoot, SCHEMA_CONFIG_PATH);
    fslib::util::FileUtil::listDir(schemaDir, files);
    vector<string> schemas;
    for (size_t i = 0; i < files.size(); i++) {
        if (StringUtil::endsWith(files[i], SCHEMA_JSON_FILE_SUFFIX)) {
            schemas.push_back(fslib::util::FileUtil::joinFilePath(SCHEMA_CONFIG_PATH, files[i]));
        }
    }
    return schemas;
}

std::vector<std::string> ResourceReader::getAllSchemaPatchFileNames() const
{
    vector<string> files;
    string schemaDir = fslib::util::FileUtil::joinFilePath(_configRoot, SCHEMA_CONFIG_PATH);
    fslib::util::FileUtil::listDir(schemaDir, files);
    vector<string> schemaPatches;
    for (size_t i = 0; i < files.size(); i++) {
        if (StringUtil::endsWith(files[i], SCHEMA_PATCH_JSON_FILE_SUFFIX)) {
            schemaPatches.push_back(fslib::util::FileUtil::joinFilePath(SCHEMA_CONFIG_PATH, files[i]));
        }
    }
    return schemaPatches;
}

void ResourceReader::clearCache()
{
    autil::ScopedLock lock(_cacheMapLock);
    _jsonFileCache.clear();
    _notExistFileCache.clear();
}

}} // namespace build_service::config
