#ifndef ISEARCH_BS_HASHMODECONFIG_H
#define ISEARCH_BS_HASHMODECONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/HashMode.h"
#include <indexlib/config/index_partition_schema.h>

namespace build_service {
namespace config {

class HashModeConfig
{
public:
    HashModeConfig();
    ~HashModeConfig();
private:
    HashModeConfig(const HashModeConfig &);
    HashModeConfig& operator=(const HashModeConfig &);
public:
    bool init(const std::string &clusterName,
              ResourceReader *resourceReader,
              const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr);
    bool getHashMode(const std::string &regionName,
                     HashMode &hashMode) const
    {
        const auto iter = _hashModeMap.find(regionName);
        if (iter == _hashModeMap.end()) {
            return false;
        }
        hashMode = iter->second;
        return true;
    }
    const std::string& getClusterName() const
    {
        return _clusterName;
    }

private:
    typedef std::map<std::string, HashMode> HashModeMap;
    
private:
    bool initHashModeInfo(
            const std::string &clusterName,
            ResourceReader *resourceReader,
            const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr,
            HashModeMap& regionNameToHashMode,
            HashMode& defaultHashMode,
            bool& hasDefaultHashMode);   
    
private:
    const IE_NAMESPACE(config)::IndexPartitionSchemaPtr _schemaPtr;
    const ResourceReader *_resourceReader;
    HashModeMap _hashModeMap;
    std::string _clusterName;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(HashModeConfig);

}
}

#endif //ISEARCH_BS_HASHMODECONFIG_H
