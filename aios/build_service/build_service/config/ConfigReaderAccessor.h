#ifndef ISEARCH_BS_CONFIGREADERACCESSOR_H
#define ISEARCH_BS_CONFIGREADERACCESSOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/ResourceReader.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace config {

class ConfigReaderAccessor : public autil::legacy::Jsonizable
{
public:
    ConfigReaderAccessor(const std::string& dataTableName);
    ~ConfigReaderAccessor();

    struct ClusterInfo : public autil::legacy::Jsonizable
    {
        std::string clusterName;
        int64_t schemaId;
        ClusterInfo()
            : schemaId(-1)
        {
        }
        ClusterInfo(std::string clusterNameInput, int64_t schemaIdInput)
        {
            clusterName = clusterNameInput;
            schemaId = schemaIdInput;
        }
        bool operator< (const ClusterInfo& other) const
        {
            if (schemaId != other.schemaId)
            {
                return schemaId < other.schemaId;
            }
            return clusterName < other.clusterName;
        }
        bool operator== (const ClusterInfo& other) const
        {
            return clusterName == other.clusterName ? schemaId == other.schemaId : false;
        }
        void Jsonize(Jsonizable::JsonWrapper &json)
        {
            json.Jsonize("clusterName", clusterName, clusterName);
            json.Jsonize("schemaId", schemaId, schemaId);
        }
    };

private:
    ConfigReaderAccessor(const ConfigReaderAccessor&);
    ConfigReaderAccessor& operator=(const ConfigReaderAccessor &);

public:
    std::string getLatestConfigPath();
    bool addConfig(const ResourceReaderPtr& resourceReader);
    ResourceReaderPtr getLatestConfig();
    ResourceReaderPtr getConfig(const std::string& configPath);
    ResourceReaderPtr getConfig(const std::string& clusterName,
                                const int64_t schemaId);
    
    void deleteConfig(const std::string& clusterName, 
                      const int64_t schemaId);
    static bool getClusterInfos(const ResourceReaderPtr& resourceReader,
                                std::string dataTableName,
                                std::vector<ClusterInfo>& clusterInfos);
    int64_t getMinSchemaId(const std::string& clusterName);
    int64_t getMaxSchemaId(const std::string& clusterName);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    void getAllClusterInfos(std::vector<ClusterInfo>& allClusterInfos);
    bool getAllClusterNames(std::vector<std::string>& clusterNames)
    {
        auto reader = getLatestConfig();
        return reader->getAllClusters(_dataTableName, clusterNames);
    }
private:
    void clearUselessConfig();

private:
    typedef std::map<std::string, int64_t> ConfigPathToResourceReaderMap;
    typedef std::map<ClusterInfo, int64_t> ClusterInfoToResourceReaderMap;
    std::string _dataTableName;
    std::string _latestConfigPath;
    std::vector<ResourceReaderPtr> _resourceReaders;
    ConfigPathToResourceReaderMap _configToResourceReader;
    ClusterInfoToResourceReaderMap _clusterInfoToResourceReader;
    mutable autil::RecursiveThreadMutex _lock;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ConfigReaderAccessor);

}
}

#endif //ISEARCH_BS_CONFIGREADERACCESSOR_H
