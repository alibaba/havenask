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
#include "build_service/config/HashModeConfig.h"

#include "indexlib/config/legacy_schema_adapter.h"

using namespace std;
using namespace autil;
using namespace build_service::config;

using namespace indexlib::config;

namespace build_service { namespace config {
BS_LOG_SETUP(config, HashModeConfig);

HashModeConfig::HashModeConfig() {}

HashModeConfig::~HashModeConfig() {}

bool HashModeConfig::initHashModeInfo(const string& clusterName, ResourceReader* resourceReader,
                                      const std::shared_ptr<indexlibv2::config::ITabletSchema>& schemaPtr,
                                      HashModeMap& regionNameToHashMode, HashMode& defaultHashMode,
                                      bool& hasDefaultHashMode)
{
    // init hash mode
    string relativePath = ResourceReader::getClusterConfRelativePath(_clusterName);
    if (!resourceReader->getConfigWithJsonPath(relativePath, "cluster_config.hash_mode", defaultHashMode,
                                               hasDefaultHashMode)) {
        BS_LOG(ERROR, "jsonize HashModeConfig failed");
        return false;
    }
    if (hasDefaultHashMode && !defaultHashMode.validate()) {
        BS_LOG(ERROR, "validate default hash mode failed in cluster %s.", clusterName.c_str());
        return false;
    }
    RegionHashModes regionHashModes;
    if (!resourceReader->getConfigWithJsonPath(relativePath, "cluster_config.region_hash_mode", regionHashModes)) {
        BS_LOG(ERROR, "invalid region_hash_mode config in cluster %s", clusterName.c_str());
        return false;
    }
    if (regionHashModes.empty() && !hasDefaultHashMode) {
        BS_LOG(ERROR, "no hash_mode config cluster %s", clusterName.c_str());
        return false;
    }
    for (size_t i = 0; i < regionHashModes.size(); ++i) {
        RegionHashMode regionHashMode = regionHashModes[i];
        if (!regionHashMode.validate()) {
            BS_LOG(ERROR, "validate failed for region hash mode in cluster %s", clusterName.c_str());
            return false;
        }
        const auto& legacySchemaAdapter = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schemaPtr);
        assert(legacySchemaAdapter);
        regionid_t regionId = legacySchemaAdapter->GetLegacySchema()->GetRegionId(regionHashMode._regionName);
        if (regionId == INVALID_REGIONID) {
            BS_LOG(ERROR, "invalid region_hash_mode for cluster %s, invalid region_name", clusterName.c_str());
            return false;
        }
        regionNameToHashMode.insert(make_pair(regionHashMode._regionName, regionHashMode));
    }
    return true;
}

bool HashModeConfig::insertHashNode(const HashModeMap& regionNameToHashMode, const std::string& regionName,
                                    bool hasDefaultHashMode, HashMode defaultHashMode)
{
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
    return true;
}

bool HashModeConfig::init(const string& clusterName, ResourceReader* resourceReader,
                          const std::shared_ptr<indexlibv2::config::ITabletSchema>& schemaPtr)
{
    _clusterName = clusterName;
    HashModeMap regionNameToHashMode;
    HashMode defaultHashMode;
    bool hasDefaultHashMode = false;

    if (!initHashModeInfo(clusterName, resourceReader, schemaPtr, regionNameToHashMode, defaultHashMode,
                          hasDefaultHashMode)) {
        return false;
    }
    const auto& legacySchemaAdapter = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schemaPtr);
    if (legacySchemaAdapter) {
        const auto& legacySchema = legacySchemaAdapter->GetLegacySchema();
        for (size_t regionId = 0; regionId < legacySchema->GetRegionCount(); ++regionId) {
            const auto& regionName = legacySchema->GetRegionSchema(regionId)->GetRegionName();
            if (!insertHashNode(regionNameToHashMode, regionName, hasDefaultHashMode, defaultHashMode)) {
                return false;
            }
        }

    } else {
        if (!insertHashNode(regionNameToHashMode, DEFAULT_REGIONNAME, hasDefaultHashMode, defaultHashMode)) {
            return false;
        }
    }
    return true;
}

}} // namespace build_service::config
