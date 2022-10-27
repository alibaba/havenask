#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/util/FileUtil.h"

using namespace std;
using namespace build_service::util;

namespace build_service {
namespace config {
BS_LOG_SETUP(config, ConfigReaderAccessor);

ConfigReaderAccessor::ConfigReaderAccessor(const string& dataTable)
    : _dataTableName(dataTable)
{
}

ConfigReaderAccessor::~ConfigReaderAccessor() {
}

void ConfigReaderAccessor::getAllClusterInfos(std::vector<ClusterInfo>& allClusterInfos) {
    allClusterInfos.clear();
    autil::ScopedLock lock(_lock);
    auto iter = _clusterInfoToResourceReader.begin();
    for(; iter != _clusterInfoToResourceReader.end(); iter++) {
        allClusterInfos.push_back(iter->first);
    }
}

void ConfigReaderAccessor::clearUselessConfig() {
    for (int64_t i = 0; i < (int64_t)_resourceReaders.size(); i++) {
        bool useless = true;
        if (!_resourceReaders[i]) {
            continue;
        }
        for (auto iter = _clusterInfoToResourceReader.begin();
             iter != _clusterInfoToResourceReader.end(); iter++) {
            if (iter->second == i) {
                useless = false;
                break;
            }
        }
        if (useless) {
            auto tmpIter = _configToResourceReader.find(
                    _resourceReaders[i]->getConfigPath());
            _configToResourceReader.erase(tmpIter);
            _resourceReaders[i].reset();
        }
    }
}

bool ConfigReaderAccessor::addConfig(const ResourceReaderPtr& resourceReader)
{
    string configPath = resourceReader->getConfigPath();
    std::vector<ClusterInfo> clusterInfos;
    if (!getClusterInfos(resourceReader, _dataTableName, clusterInfos))
    {
        BS_LOG(ERROR, "get cluster info from config [%s] failed",
               configPath.c_str());
        return false;
    }

    autil::ScopedLock lock(_lock);
    auto iter = _configToResourceReader.find(configPath);
    int64_t index = -1;
    if (iter == _configToResourceReader.end())
    {
        _resourceReaders.push_back(resourceReader);
        index = _resourceReaders.size() - 1;
        _configToResourceReader[configPath] = index;
    }
    else
    {
        index = iter->second;
        _resourceReaders[index] = resourceReader;
    }
    // update clusterInfo -> resourceReader
    for (const ClusterInfo& clusterInfo : clusterInfos)
    {
        _clusterInfoToResourceReader[clusterInfo] = index;
    }
    _latestConfigPath = configPath;
    clearUselessConfig();
    return true;
}

int64_t ConfigReaderAccessor::getMinSchemaId(const std::string& clusterName) {
    int64_t minSchemaId = -1;
    for (auto iter = _clusterInfoToResourceReader.begin();
         iter != _clusterInfoToResourceReader.end(); iter++) {
        ClusterInfo clusterInfo = iter->first;
        if (clusterInfo.clusterName != clusterName) {
            continue;
        }
        if (minSchemaId == -1) {
            minSchemaId = clusterInfo.schemaId;
            continue;
        }
        if (minSchemaId > clusterInfo.schemaId) {
            minSchemaId = clusterInfo.schemaId;
        }
    }
    return minSchemaId;
}

int64_t ConfigReaderAccessor::getMaxSchemaId(const std::string& clusterName) {
    int64_t maxSchemaId = -1;
    for (auto iter = _clusterInfoToResourceReader.begin();
         iter != _clusterInfoToResourceReader.end(); iter++) {
        ClusterInfo clusterInfo = iter->first;
        if (clusterInfo.clusterName != clusterName) {
            continue;
        }
        if (maxSchemaId == -1) {
            maxSchemaId = clusterInfo.schemaId;
            continue;
        }
        if (maxSchemaId < clusterInfo.schemaId) {
            maxSchemaId = clusterInfo.schemaId;
        }
    }
    return maxSchemaId;
}

ResourceReaderPtr ConfigReaderAccessor::getConfig(const std::string& path)
{
    autil::ScopedLock lock(_lock);
    string configPath = FileUtil::normalizeDir(path);
    auto iter = _configToResourceReader.find(configPath);
    if (iter == _configToResourceReader.end())
    {
        BS_LOG(ERROR, "no resource reader for config [%s]", configPath.c_str());
        return ResourceReaderPtr();
    }
    if (iter->second >= (int64_t)_resourceReaders.size())
    {
        BS_LOG(ERROR, "no resource reader for config [%s]", configPath.c_str());
        return ResourceReaderPtr();
    }
    return _resourceReaders[iter->second];
}
    
ResourceReaderPtr ConfigReaderAccessor::getConfig(const std::string& clusterName,
        const int64_t schemaId)
{
    autil::ScopedLock lock(_lock);
    ClusterInfo clusterInfo(clusterName, schemaId);
    auto iter = _clusterInfoToResourceReader.find(clusterInfo);
    if (iter == _clusterInfoToResourceReader.end())
    {
        BS_LOG(ERROR, "no resource reader for clusterName [%s], shemaId [%ld]", 
               clusterName.c_str(), schemaId);
        return ResourceReaderPtr();
    }
    if (iter->second >= (int64_t)_resourceReaders.size())
    {
        BS_LOG(ERROR, "no resource reader for clusterName [%s], shemaId [%ld]", 
               clusterName.c_str(), schemaId);
        return ResourceReaderPtr();
    }
    return _resourceReaders[iter->second];
}

// void ConfigReaderAccessor::deleteConfig(const std::string& path)
// {
//     autil::ScopedLock lock(_lock);
//     string configPath = FileUtil::normalizeDir(path);
//     auto iter = _configToResourceReader.find(configPath);
//     if (iter == _configToResourceReader.end())
//     {
//         return;
//     }
//     if (iter->second >= (int64_t)_resourceReaders.size())
//     {
//         BS_LOG(ERROR, "no resource reader for config [%s]", path.c_str());
//         return ;
//     }
//     int64_t index = iter->second;
//     std::vector<ClusterInfo> clusterInfos;
//     if (!getClusterInfos(_resourceReaders[iter->second], _dataTableName, clusterInfos))
//     {
//         assert(false);
//     }
//     for (const ClusterInfo& clusterInfo : clusterInfos)
//     {
//         auto tmpIter = _clusterInfoToResourceReader.find(clusterInfo);
//         if (tmpIter != _clusterInfoToResourceReader.end())
//         {
//             if (tmpIter->second == index) {
//                 _clusterInfoToResourceReader.erase(tmpIter);
//             }
//         }
//     }
//     _configToResourceReader.erase(iter);
//     _resourceReaders[index].reset();
// }

void ConfigReaderAccessor::deleteConfig(const std::string& clusterName, 
                  const int64_t schemaId) {
    autil::ScopedLock lock(_lock);
    ClusterInfo clusterInfo(clusterName, schemaId);
    auto iter = _clusterInfoToResourceReader.find(clusterInfo);
    if (iter == _clusterInfoToResourceReader.end()) {
        return;
    }
    int64_t idx = iter->second;
    _clusterInfoToResourceReader.erase(iter);
    bool needConfig = false;
    for (auto iter = _clusterInfoToResourceReader.begin(); iter != _clusterInfoToResourceReader.end(); ++iter) {
        if (iter->second == idx) {
            needConfig = true;
        }
    }
    if (!needConfig) {
        auto iter = _configToResourceReader.find(
                _resourceReaders[idx]->getConfigPath());
        _configToResourceReader.erase(iter);
        _resourceReaders[idx].reset();
    }
}


bool ConfigReaderAccessor::getClusterInfos(const ResourceReaderPtr& resourceReader,
        string dataTableName, vector<ClusterInfo>& clusterInfos)
{
    clusterInfos.clear();
    vector<string> allClusters;
    if (!resourceReader->getAllClusters(dataTableName, allClusters)) {
        string errorMsg = "getAllClusters from [" + dataTableName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    for (auto& cluster : allClusters)
    {
        auto schemaPtr = resourceReader->getSchema(cluster);
        if (!schemaPtr)
        {
            BS_LOG(ERROR, "get [%s] schema for dataTable [%s] failed", cluster.c_str(),
                   dataTableName.c_str());
            return false;
        }
        ClusterInfo clusterInfo(cluster, schemaPtr->GetSchemaVersionId());
        clusterInfos.push_back(clusterInfo);
    }
    return true;
}

ResourceReaderPtr ConfigReaderAccessor::getLatestConfig()
{
    autil::ScopedLock lock(_lock);
    auto iter = _configToResourceReader.find(_latestConfigPath);
    if (iter == _configToResourceReader.end())
    {
        BS_LOG(ERROR, "cannot find lastest config [%s]",
               _latestConfigPath.c_str());
        return ResourceReaderPtr();
    }
    return _resourceReaders[iter->second];
}


string ConfigReaderAccessor::getLatestConfigPath() 
{
    autil::ScopedLock lock(_lock);
    auto reader = getLatestConfig();
    return reader->getOriginalConfigPath();
}
    
void ConfigReaderAccessor::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json)
{
    json.Jsonize("latestConfig", _latestConfigPath,
                 _latestConfigPath);
    typedef std::pair<int32_t, vector<ClusterInfo> > ConfigInfo;
    map<string, ConfigInfo> configInfos;
    if (json.GetMode() == TO_JSON)
    {
        // for (auto iter = _configToResourceReader.begin();
        //      iter != _configToResourceReader.end(); iter++)
        // {
        //     vector<ClusterInfo> clusterInfos;
        //     getClusterInfos(_resourceReaders[iter->second], _dataTableName, clusterInfos);
        //     string originalConfigPath = _resourceReaders[iter->second]->getOriginalConfigPath();
        //     configInfos[originalConfigPath] = make_pair(
        //             _resourceReaders[iter->second]->getWorkerProtocolVersion(), clusterInfos);
        // }
        // string jsonStr = ToJsonString(configInfos);
        // cout << "-------------" << endl;
        // cout << jsonStr << endl;
        // configInfos.clear();
        for (auto iter = _clusterInfoToResourceReader.begin();
             iter != _clusterInfoToResourceReader.end(); iter++) {
            auto& clusterInfo = iter->first;
            string configPath = _resourceReaders[iter->second]->getOriginalConfigPath();
            int32_t workerVersion = _resourceReaders[iter->second]->getWorkerProtocolVersion();
            auto tmpIter = configInfos.find(configPath);
            if (tmpIter == configInfos.end()) {
                vector<ClusterInfo> clusterInfos;
                clusterInfos.push_back(clusterInfo);
                configInfos[configPath] = make_pair(workerVersion, clusterInfos);
            } else {
                tmpIter->second.second.push_back(clusterInfo);
            }
        }
        // jsonStr = ToJsonString(configInfos);
        // cout << "===================" << endl;
        // cout << jsonStr << endl;
        json.Jsonize("configInfo", configInfos, configInfos);
    }
    else
    {
        json.Jsonize("configInfo", configInfos, configInfos);
        for (auto iter = configInfos.begin(); iter != configInfos.end(); iter++)
        {
            autil::ScopedLock lock(_lock);
            ResourceReaderPtr resourceReader(new ResourceReader(iter->first));
            resourceReader->init();
            resourceReader->updateWorkerProtocolVersion(iter->second.first);
            vector<ClusterInfo>& clusterInfos = iter->second.second;
            _resourceReaders.push_back(resourceReader);
            size_t idx = _resourceReaders.size() - 1;
            _configToResourceReader[resourceReader->getConfigPath()] = idx;
            
            for (auto& clusterInfo : clusterInfos)
            {
                _clusterInfoToResourceReader[clusterInfo] = idx;
            }
        }
    }
}

}
}
