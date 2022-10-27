#ifndef ISEARCH_BS_RESOURCEREADER_H
#define ISEARCH_BS_RESOURCEREADER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/GenerationMeta.h"
#include <autil/legacy/jsonizable.h>
#include <autil/legacy/json.h>
#include <autil/Lock.h>
#include <indexlib/config/index_partition_schema.h>


namespace build_service {
namespace config {

class ResourceReaderManager;
    
class ResourceReader
{
public:
    enum ErrorCode {
        JSON_PATH_OK,
        JSON_PATH_NOT_EXIST,
        JSON_PATH_INVALID,
    };
public:
    ResourceReader(const std::string &configRoot);
    ~ResourceReader();
public:
    // apis below will not enable cache
    bool getConfigContent(const std::string &relativePath, std::string &content) const;
    bool getFileContent(std::string &result, const std::string &fileName) const;
    void clearCache();
    
public:
    void init();    
    bool initGenerationMeta(const std::string &generationIndexRoot); // for searcher
    bool initGenerationMeta(generationid_t gid,
                            const std::string &dataTable,
                            const std::string &indexRootDir); // for processor

    /**
     * example : processor_chain_config[0].table_name
     */
    template <typename T>
    bool getConfigWithJsonPath(const std::string &relativePath,
                               const std::string &jsonPath, T &config) const;

    template <typename T>
    bool getConfigWithJsonPath(const std::string &relativePath,
                               const std::string &jsonPath,
                               T &config, bool& isConfigExist) const;
    
    template <typename T>
    bool getDataTableConfigWithJsonPath(const std::string &dataTableName,
            const std::string &jsonPath, T &config) const;

    template <typename T>
    bool getDataTableConfigWithJsonPath(const std::string &dataTableName,
            const std::string &jsonPath, T &config, bool& isExist) const;

    template <typename T>
    bool getClusterConfigWithJsonPath(const std::string &clusterName,
            const std::string &jsonPath, T &config) const;

    template <typename T>
    bool getClusterConfigWithJsonPath(const std::string &clusterName,
                                      const std::string &jsonPath,
                                      T &config, bool& isExist) const;

    template <typename T>
    bool getTaskConfigWithJsonPath(const std::string &taskName,
                                   const std::string &jsonPath,
                                   T &config, bool& isExist);

    const std::string &getGenerationMetaValue(const std::string &key) const;
    std::string getConfigPath() const;
    std::string getOriginalConfigPath() const { return _configRoot; }
    std::string getPluginPath() const;
    bool getAllClusters(const std::string &dataTable, std::vector<std::string> &clusterNames);
    bool getAllClusters(std::vector<std::string> &clusterNames);
    bool getAllDataTables(std::vector<std::string> &dataTables);
    bool getDataTableFromClusterName(const std::string &clusterName, std::string &dataTable);
    
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr getSchema(
        const std::string &clusterName) const;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr getSchemaBySchemaTableName(
        const std::string &schemaName) const;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr getSchemaByRelativeFileName(
        const std::string &schemaFileName) const;

    bool getTableType(const std::string &clusterName,
                      heavenask::indexlib::TableType& tableType) const;
    void updateWorkerProtocolVersion(int32_t protocolVersion);
    int32_t getWorkerProtocolVersion();

    bool getLuaFileContent(const std::string& luaFilePath,
                           std::string& luaFileContent) const;
    /*
     * @return relative schema path, etc: schemas/cluster1_schema.json
     */
    std::vector<std::string> getAllSchemaFileNames() const;
    
public:
    static std::string getClusterConfRelativePath(const std::string &clusterName);
    static std::string getTaskConfRelativePath(const std::string &taskName);
    
    static std::string getSchemaConfRelativePath(const std::string &tableName);
    
    static std::string getDataTableRelativePath(const std::string &dataTableName);

    static std::string getLuaScriptFileRelativePath(const std::string &luaFileName);

    static std::string getTaskScriptFileRelativePath(const std::string &filePath);
    
    static bool getAllClusters(const std::string &configPath,
                               const std::string &dataTable,
                               std::vector<std::string> &clusterNames);
public:
    // tools
    static ErrorCode getJsonNode(const std::string &jsonString,
                                 const std::string &jsonPath,
                                 autil::legacy::Any &any);
    static ErrorCode getJsonNode(const autil::legacy::json::JsonMap &jsonMap,
                                 const std::string &jsonPath,
                                 autil::legacy::Any &any);
public:
    //only for test
    static bool getJsonString(
            const std::string& jsonStr, const std::string& nodePath,
            std::string& result);
private:
    bool getJsonMap(const std::string &relativePath,
                    autil::legacy::json::JsonMap &jsonMap) const;
private:
    std::string _configRoot;
    GenerationMeta _generationMeta;
    mutable std::map<std::string, autil::legacy::json::JsonMap> _jsonFileCache;
    mutable std::map<std::string, std::string> _luaFileCache;
    mutable autil::ThreadMutex _cacheMapLock;
    int64_t _maxZipSize;
    int32_t _workerProtocolVersion;
    std::vector<std::string> _allSchemaFileNames;

private:
    friend class ResourceReaderManager;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ResourceReader);
///////////////////////////////////////////////////////
template <typename T>
bool ResourceReader::getConfigWithJsonPath(const std::string &relativePath,
        const std::string &jsonPath, T &config) const
{
    bool isConfigExist;
    return getConfigWithJsonPath(relativePath, jsonPath, config, isConfigExist);
}

template <typename T>
bool ResourceReader::getConfigWithJsonPath(const std::string &relativePath,
                                           const std::string &jsonPath,
                                           T &config, bool& isConfigExist) const
{
    isConfigExist = false;
    autil::legacy::json::JsonMap jsonMap;
    if (!getJsonMap(relativePath, jsonMap)) {
        return false;
    }

    autil::legacy::Any any;
    ErrorCode ec = getJsonNode(jsonMap, jsonPath, any);
    if (ec == JSON_PATH_NOT_EXIST) {
        return true;
    } else if (ec == JSON_PATH_INVALID) {
        return false;
    }
    isConfigExist = true;
    try {
        FromJson(config, any);
    } catch (const autil::legacy::ExceptionBase &e) {
        std::stringstream ss;
        ss << "get config failed, relativePath[" << relativePath
           << "] jsonPath[" << jsonPath
           << "], exception[" << std::string(e.what()) << "]";
        std::string errorMsg = ss.str();
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}


template <typename T>
bool ResourceReader::getClusterConfigWithJsonPath(const std::string &clusterName,
        const std::string &jsonPath, T &config) const
{
    std::string clusterFile = getClusterConfRelativePath(clusterName);
    return getConfigWithJsonPath(clusterFile, jsonPath, config);
}

template <typename T>
bool ResourceReader::getClusterConfigWithJsonPath(const std::string &clusterName,
                                                  const std::string &jsonPath,
                                                  T &config, bool& isConfigExist) const
{
    std::string clusterFile = getClusterConfRelativePath(clusterName);
    return getConfigWithJsonPath(clusterFile, jsonPath, config, isConfigExist);
}

template <typename T>
bool ResourceReader::getTaskConfigWithJsonPath(const std::string &taskName,
                                               const std::string &jsonPath,
                                               T &config, bool& isExist) {
    std::string taskFile = getTaskConfRelativePath(taskName);
    return getConfigWithJsonPath(taskFile, jsonPath, config, isExist);
}

template <typename T>
bool ResourceReader::getDataTableConfigWithJsonPath(const std::string &dataTableName,
        const std::string &jsonPath, T &config) const
{
    std::string dataTableFile = getDataTableRelativePath(dataTableName);
    return getConfigWithJsonPath(dataTableFile, jsonPath, config);
}

template <typename T>
bool ResourceReader::getDataTableConfigWithJsonPath(const std::string &dataTableName,
        const std::string &jsonPath, T &config, bool& isExist) const
{
    std::string dataTableFile = getDataTableRelativePath(dataTableName);
    return getConfigWithJsonPath(dataTableFile, jsonPath, config, isExist);
}


}
}

#endif //ISEARCH_BS_RESOURCEREADER_H
