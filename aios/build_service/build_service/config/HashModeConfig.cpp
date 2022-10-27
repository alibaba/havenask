#include "build_service/config/HashModeConfig.h"

using namespace std;
using namespace autil;
using namespace build_service::config;

IE_NAMESPACE_USE(config);

namespace build_service {
namespace config {
BS_LOG_SETUP(config, HashModeConfig);

HashModeConfig::HashModeConfig() {
}

HashModeConfig::~HashModeConfig() {
}

bool HashModeConfig::initHashModeInfo(
        const string &clusterName,
        ResourceReader *resourceReader,
        const IndexPartitionSchemaPtr &schemaPtr,
        HashModeMap& regionNameToHashMode,
        HashMode& defaultHashMode,
        bool& hasDefaultHashMode)
{
    // init hash mode
    string relativePath = ResourceReader::getClusterConfRelativePath(_clusterName);
    if (!resourceReader->getConfigWithJsonPath(
                    relativePath, "cluster_config.hash_mode", defaultHashMode, hasDefaultHashMode))
    {
        BS_LOG(ERROR, "jsonize HashModeConfig failed");
        return false;
    }
    if (hasDefaultHashMode && !defaultHashMode.validate()) {
        BS_LOG(ERROR, "validate default hash mode failed in cluster %s.", clusterName.c_str());        
        return false ;
    }
    RegionHashModes regionHashModes;
    if (!resourceReader->getConfigWithJsonPath(
                    relativePath, "cluster_config.region_hash_mode", regionHashModes))
    {
        BS_LOG(ERROR, "invalid region_hash_mode config in cluster %s", clusterName.c_str());        
        return false;
    }
    if (regionHashModes.empty() && !hasDefaultHashMode) {
        BS_LOG(ERROR, "no hash_mode config cluster %s", clusterName.c_str());        
        return false;        
    }
    for (size_t i = 0; i < regionHashModes.size(); ++i)
    {
        RegionHashMode regionHashMode = regionHashModes[i];
        if (!regionHashMode.validate()) {
            BS_LOG(ERROR, "validate failed for region hash mode in cluster %s",
                    clusterName.c_str());
            return false;        
        }
        regionid_t regionId = schemaPtr->GetRegionId(regionHashMode._regionName);
        if (regionId == INVALID_REGIONID) {
            BS_LOG(ERROR, "invalid region_hash_mode for cluster %s, invalid region_name",
                    clusterName.c_str());
            return false;
        }
        regionNameToHashMode.insert(make_pair(regionHashMode._regionName, regionHashMode));
    }
    return true;
}

bool HashModeConfig::init(
        const string &clusterName,
        ResourceReader *resourceReader,
        const IndexPartitionSchemaPtr &schemaPtr)
{
    _clusterName = clusterName;
    HashModeMap regionNameToHashMode;
    HashMode defaultHashMode;
    bool hasDefaultHashMode = false;
    
    if (!initHashModeInfo(clusterName, resourceReader, schemaPtr,
                          regionNameToHashMode, defaultHashMode, hasDefaultHashMode))
    {
        return false;
    }
    for (size_t regionId = 0; regionId < schemaPtr->GetRegionCount(); ++regionId)
    {
        const auto& regionName = schemaPtr->GetRegionSchema(regionId)->GetRegionName();
        const auto iter = regionNameToHashMode.find(regionName);
        if (iter != regionNameToHashMode.end()) {
            _hashModeMap.insert(*iter);
        } else {
            if (!hasDefaultHashMode) {
                BS_LOG(ERROR, "no hash_mode config for region %s", regionName.c_str());
                return false;        
            }
            _hashModeMap.insert(make_pair(regionName, defaultHashMode)); 
        }
    }
    return true;
}

}
}
