#include "build_service/config/ResourceReader.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/util/FileUtil.h"
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include <fslib/fslib.h>
#include <indexlib/index_base/schema_adapter.h>
#include "build_service/config/DocProcessorChainConfig.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/config/ConfigParser.h"
#include "build_service/config/ResourceReaderManager.h"

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace fslib;
using namespace fslib::fs;
using namespace build_service::util;

namespace build_service {
namespace config {
BS_LOG_SETUP(config, ResourceReader);

ResourceReader::ResourceReader(const string &configRoot)
    : _configRoot(configRoot)
    , _maxZipSize(1024 * 1024 * 10)
    , _workerProtocolVersion(UNKNOWN_WORKER_PROTOCOL_VERSION)
{
    const char* param = getenv(BS_ENV_ENABLE_RESOURCE_READER_MAX_ZIP_SIZE.c_str());
    int64_t maxZipSize;
    if (param != NULL &&
        StringUtil::fromString(string(param), maxZipSize))
    {
        _maxZipSize = maxZipSize * 1024;
    }
}

ResourceReader::~ResourceReader() {
}

void ResourceReader::init()
{
    return;
}

bool ResourceReader::initGenerationMeta(const string &generationIndexRoot) {
    if (!generationIndexRoot.empty()) {
        GenerationMeta meta;
        if (!meta.loadFromFile(generationIndexRoot)) {
            return false;
        }
        _generationMeta = meta;
    }
    return true;
}

bool ResourceReader::initGenerationMeta(
        generationid_t gid, const string &dataTable,
        const string &indexRootDir)
{
    vector<string> clusterNames;
    if (!getAllClusters(dataTable, clusterNames)) {
        string errorMsg = "getAllClusters for dataTable["
                          + dataTable + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    for (size_t i = 0; i < clusterNames.size(); i++) {
        const string &clusterName = clusterNames[i];
        string generationIndexRoot = IndexPathConstructor::getGenerationIndexPath(
                indexRootDir, clusterName, gid);
        GenerationMeta meta;
        if (!meta.loadFromFile(generationIndexRoot)) {
            string errorMsg = "load generation meta for cluster["
                              + clusterName + "] failed";
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

const string& ResourceReader::getGenerationMetaValue(const string &key) const {
    return _generationMeta.getValue(key);
}

string ResourceReader::getConfigPath() const {
    return FileUtil::normalizeDir(_configRoot);
}

string ResourceReader::getPluginPath() const {
    return FileUtil::joinFilePath(getConfigPath(), "plugins") + '/';
}

ResourceReader::ErrorCode ResourceReader::getJsonNode(const string &jsonString,
        const string &jsonPath, autil::legacy::Any &any)
{
    vector<string> jsonPathVec = StringTokenizer::tokenize(ConstString(jsonPath), '.',
            StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
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
            BS_LOG(DEBUG, "get config by json path failed."
                   "key[%s] not found in json map.", nodeName.c_str());
            ERROR_COLLECTOR_LOG(DEBUG, "get config by json path failed."
                    "key[%s] not found in json map.", nodeName.c_str());

            return JSON_PATH_NOT_EXIST;
        }
        any = it->second;
        while (pos != string::npos) {
            size_t end = jsonPathVec[i].find(']', pos);
            if (end == string::npos) {
                string errorMsg = "get config by json path failed.Invalid json path[" +
                                  jsonPathVec[i] + "], missing ']'.";
                BS_LOG(WARN, "%s", errorMsg.c_str());
                return JSON_PATH_INVALID;
            }

            size_t idx;
            string idxStr = jsonPathVec[i].substr(pos + 1, end - pos - 1);
            if (!StringUtil::fromString<size_t>(idxStr, idx)) {
                string errorMsg = "get config by json path failed.Invalid json vector idx[" +
                                  idxStr + "], .";
                BS_LOG(WARN, "%s", errorMsg.c_str());
                return JSON_PATH_INVALID;
            }
            JsonArray jsonVec = AnyCast<JsonArray>(any);
            if (idx >= jsonVec.size()) {
                stringstream ss;
                ss << "get config by json path failed.[" << jsonPathVec[i]
                   << "] out of range, total size[" << jsonVec.size() << "].";
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

ResourceReader::ErrorCode ResourceReader::getJsonNode(
        const JsonMap &jsonMap, const string &jsonPath, Any &any)
{
    return getJsonNode(ToJsonString(jsonMap), jsonPath, any);
}

bool ResourceReader::getJsonMap(const string &relativePath, JsonMap &jsonMap) const {
    ConfigParser parser(_configRoot);
    autil::ScopedLock lock(_cacheMapLock);
    auto iter = _jsonFileCache.find(relativePath);
    if (iter != _jsonFileCache.end()) {
        jsonMap = iter->second;
        return true;
    }
    if (parser.parse(relativePath, jsonMap))
    {
        _jsonFileCache[relativePath] = jsonMap;
        return true;
    }
    return false;
}

bool ResourceReader::getAllClusters(const string &dataTable, vector<string> &clusters)
{
    DocProcessorChainConfigVec chainConfig;
    if (!getDataTableConfigWithJsonPath(dataTable, "processor_chain_config", chainConfig))
    {
        string errorMsg = "Get processor_chain_config for data table["
                          + dataTable + "] failed.";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    for (size_t i = 0; i < chainConfig.size(); ++i) {
        clusters.insert(clusters.end(),
                        chainConfig[i].clusterNames.begin(),
                        chainConfig[i].clusterNames.end());
    }
    sort(clusters.begin(), clusters.end());
    clusters.resize(distance(clusters.begin(), unique(clusters.begin(), clusters.end())));
    return true;
}

bool ResourceReader::getAllClusters(std::vector<std::string> &clusterNames) {
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

bool ResourceReader::getDataTableFromClusterName(const string &clusterName, string &dataTable) {
    vector<string> dataTables;
    if (!getAllDataTables(dataTables)) {
        string errorMsg = "get data table for cluster["
                          + clusterName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    for (size_t i = 0; i < dataTables.size(); ++i) {
        vector<string> clusterNames;
        if (!getAllClusters(dataTables[i], clusterNames)) {
            string errorMsg = "get data table for cluster["
                              + clusterName + "] failed";
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

bool ResourceReader::getAllDataTables(std::vector<std::string> &dataTables) {
    FileList dataTableFiles;
    string dataTableConfPath = FileUtil::joinFilePath(_configRoot, DATA_TABLE_PATH);
    fslib::ErrorCode ec = FileSystem::listDir(dataTableConfPath, dataTableFiles);
    if (ec != EC_OK) {
        string errorMsg = "get all data tables failed : list dir["
                          + dataTableConfPath + "] failed.";
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

string ResourceReader::getLuaScriptFileRelativePath(const string &luaFileName) {
    return LUA_CONFIG_PATH + "/" + luaFileName;
}

string ResourceReader::getTaskScriptFileRelativePath(const string &filePath) {
    return TASK_SCRIPTS_CONFIG_PATH + "/" + filePath;
}

string ResourceReader::getClusterConfRelativePath(const string &clusterName) {
    return CLUSTER_CONFIG_PATH + "/" + clusterName + CLUSTER_JSON_FILE_SUFFIX;
}

string ResourceReader::getTaskConfRelativePath(const string &taskName) {
    return CLUSTER_CONFIG_PATH + "/" + taskName + TASK_CONFIG_SUFFIX;
}

string ResourceReader::getSchemaConfRelativePath(const string &tableName) {
    return SCHEMA_CONFIG_PATH + "/" + tableName + SCHEMA_JSON_FILE_SUFFIX;
}

string ResourceReader::getDataTableRelativePath(const std::string &dataTableName) {
    return DATA_TABLE_PATH + "/" + dataTableName + DATA_TABLE_JSON_FILE_SUFFIX;
}

bool ResourceReader::getAllClusters(const std::string &configPath,
                                    const std::string &dataTable,
                                    std::vector<std::string> &clusterNames)
{
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(configPath);
    return resourceReader->getAllClusters(dataTable, clusterNames);
}

IndexPartitionSchemaPtr ResourceReader::getSchemaBySchemaTableName(
    const string &tableName) const
{
    string schemaFileName = getSchemaConfRelativePath(tableName);
    return getSchemaByRelativeFileName(schemaFileName);
}

IndexPartitionSchemaPtr ResourceReader::getSchemaByRelativeFileName(
        const string &schemaFileName) const
{
    try {
        JsonMap jsonMap;
        if (!getJsonMap(schemaFileName, jsonMap))
        {
            return IndexPartitionSchemaPtr();
        }
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        FromJson(*schema, jsonMap);
        return schema;
    } catch (const exception &e) {
        string errorMsg = "load " + schemaFileName + " failed, exception[" + string(e.what()) + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return IndexPartitionSchemaPtr();
    } catch (...) {
        BS_LOG(ERROR, "unknown exception");
        return IndexPartitionSchemaPtr();
    }
    return IndexPartitionSchemaPtr();
}

IndexPartitionSchemaPtr ResourceReader::getSchema(
        const string &clusterName) const
{
    string tableName;
    if (!getClusterConfigWithJsonPath(clusterName,
                    "cluster_config.table_name", tableName))
    {
        BS_LOG(ERROR, "parse table_name for %s failed", clusterName.c_str());
        return IndexPartitionSchemaPtr();
    }
    return getSchemaBySchemaTableName(tableName);
}

bool ResourceReader::getTableType(
        const std::string &clusterName,
        heavenask::indexlib::TableType& tableType) const
{
    auto schema = getSchema(clusterName);
    if (!schema) {
        return false;
    }
    tableType = schema->GetTableType();
    return true;
}

bool ResourceReader::getJsonString(const string& jsonStr,
                                   const string& nodePath,
                                   std::string& result)
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

bool ResourceReader::getConfigContent(const string &relativePath, string &content) const
{
    string absPath = FileUtil::joinFilePath(_configRoot, relativePath);
    return FileUtil::readFile(absPath, content);
}

bool ResourceReader::getFileContent(string &result, const string &fileName) const
{
    return getConfigContent(fileName, result);
}

bool ResourceReader::getLuaFileContent(const string& luaFilePath,
                                       string& luaFileContent) const
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

void ResourceReader::updateWorkerProtocolVersion(int32_t protocolVersion) {
    if (_workerProtocolVersion == UNKNOWN_WORKER_PROTOCOL_VERSION) {
        _workerProtocolVersion = protocolVersion;
    }
}

int32_t ResourceReader::getWorkerProtocolVersion() {
    return _workerProtocolVersion;
}

vector<string> ResourceReader::getAllSchemaFileNames() const {
    if (!_allSchemaFileNames.empty()) {
        return _allSchemaFileNames;
    }
    vector<string> files;
    string schemaDir = FileUtil::joinFilePath(_configRoot, SCHEMA_CONFIG_PATH);
    FileUtil::listDir(schemaDir, files);
    vector<string> schemas;
    for (size_t i = 0; i < files.size(); i++) {
        if (StringUtil::endsWith(files[i], SCHEMA_JSON_FILE_SUFFIX)) {
            schemas.push_back(FileUtil::joinFilePath(SCHEMA_CONFIG_PATH, files[i]));
        }
    }
    return schemas;
}

void ResourceReader::clearCache() {
    autil::ScopedLock lock(_cacheMapLock);
    _jsonFileCache.clear();
}

}
}
