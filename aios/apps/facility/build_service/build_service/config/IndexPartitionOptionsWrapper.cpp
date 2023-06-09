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
#include "build_service/config/IndexPartitionOptionsWrapper.h"

#include "autil/StringUtil.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/CheckpointList.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/OfflineIndexConfigMap.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;

namespace build_service { namespace config {
BS_LOG_SETUP(config, IndexPartitionOptionsWrapper);
const string IndexPartitionOptionsWrapper::DEFAULT_FULL_MERGE_CONFIG = "full";

bool IndexPartitionOptionsWrapper::getFullMergeConfigName(ResourceReader* resourceReader, const string& dataTable,
                                                          const string& clusterName, string& fullMergeConfig)
{
    if (!resourceReader->getDataTableConfigWithJsonPath(
            dataTable, "full_merge_condition.full_merge_configs." + clusterName, fullMergeConfig)) {
        string errorMsg = "parse full_merge_condition.full_merge_configs from[" + dataTable + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (fullMergeConfig.empty()) {
        fullMergeConfig = DEFAULT_FULL_MERGE_CONFIG;
    }
    indexlib::config::IndexPartitionOptions indexOptions;
    IndexPartitionOptionsWrapper indexPartitionOptionsWrapper;
    if (!indexPartitionOptionsWrapper.load(*resourceReader, clusterName)) {
        string errorMsg = "load index_partition_option [" + clusterName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (!indexPartitionOptionsWrapper.getIndexPartitionOptions(fullMergeConfig, indexOptions)) {
        string errorMsg = "fullMergeConfig[" + fullMergeConfig + "] does not exist, use default merge config";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        fullMergeConfig = "";
    }
    return true;
}

bool IndexPartitionOptionsWrapper::load(const ResourceReader& resourceReader, const string& clusterName)
{
    IndexPartitionOptionsMap indexPartitionOptionsMap;
    OfflineIndexConfigMap offlineIndexConfigMap;

    if (!resourceReader.getClusterConfigWithJsonPath(clusterName, "offline_index_config", offlineIndexConfigMap)) {
        string errorMsg = "offline_index_config for cluster[" + clusterName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    // if offline_index_config not exist in json file, use default OfflineIndexConfig
    if (offlineIndexConfigMap.size() == 0) {
        offlineIndexConfigMap[""] = OfflineIndexConfig();
    }

    OfflineIndexConfig offlineIndexConfig;
    offlineIndexConfig.offlineMergeConfig.mergeConfig.mergeStrategyStr = ALIGN_VERSION_MERGE_STRATEGY_STR;
    offlineIndexConfigMap[ALIGN_VERSION_MERGE_STRATEGY_STR] = offlineIndexConfig;

    auto iter = offlineIndexConfigMap.find(BS_ALTER_FIELD_MERGE_CONFIG);
    if (iter == offlineIndexConfigMap.end()) {
        offlineIndexConfigMap[BS_ALTER_FIELD_MERGE_CONFIG] = OfflineIndexConfig();
    }

    // if not set batch_mode_merge, use default
    iter = offlineIndexConfigMap.find(BS_BATCH_MODE_MERGE_CONFIG);
    if (iter == offlineIndexConfigMap.end()) {
        offlineIndexConfigMap[BS_BATCH_MODE_MERGE_CONFIG] = OfflineIndexConfig();
    }

    indexlib::config::OnlineConfig onlineConfig;
    resourceReader.getClusterConfigWithJsonPath(clusterName, "online_index_config", onlineConfig);

    for (OfflineIndexConfigMap::ConstIterator it = offlineIndexConfigMap.begin(); it != offlineIndexConfigMap.end();
         it++) {
        const string& configName = it->first;
        indexlib::config::IndexPartitionOptions indexPartitionOptions;
        indexPartitionOptions.SetOnlineConfig(onlineConfig);
        indexlib::config::OfflineConfig offlineConfig(it->second.buildConfig,
                                                      it->second.offlineMergeConfig.mergeConfig);

        indexPartitionOptions.SetOfflineConfig(offlineConfig);
        indexPartitionOptionsMap[configName] = indexPartitionOptions;
    }

    _indexPartitionOptionsMap = indexPartitionOptionsMap;
    return true;
}

bool IndexPartitionOptionsWrapper::getIndexPartitionOptions(const string& configName,
                                                            indexlib::config::IndexPartitionOptions& indexOptions) const
{
    IndexPartitionOptionsMap::const_iterator it = _indexPartitionOptionsMap.find(configName);
    if (_indexPartitionOptionsMap.end() == it) {
        string errorMsg = "config name[" + configName + "] not found";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    indexOptions = it->second;
    return true;
}

bool IndexPartitionOptionsWrapper::hasIndexPartitionOptions(const string& configName) const
{
    return _indexPartitionOptionsMap.end() != _indexPartitionOptionsMap.find(configName);
}

bool IndexPartitionOptionsWrapper::getIndexPartitionOptions(const ResourceReader& resourceReader,
                                                            const string& clusterName, const string& configName,
                                                            indexlib::config::IndexPartitionOptions& indexOptions)
{
    IndexPartitionOptionsWrapper indexPartitionOptionsWrapper;
    if (!indexPartitionOptionsWrapper.load(resourceReader, clusterName)) {
        return false;
    }
    if (!indexPartitionOptionsWrapper.getIndexPartitionOptions(configName, indexOptions)) {
        return false;
    }
    return true;
}

bool IndexPartitionOptionsWrapper::initReservedVersions(const KeyValueMap& kvMap, IndexPartitionOptions& options)
{
    string reservedVersion = getValueFromKeyValueMap(kvMap, RESERVED_VERSION_LIST, "");
    if (!reservedVersion.empty()) {
        vector<versionid_t> versions;
        StringUtil::fromString(reservedVersion, versions, " ");
        set<versionid_t> idSet;
        idSet.insert(versions.begin(), versions.end());
        options.SetReservedVersions(idSet);
        return true;
    }
    // compatible with old admin
    string reservedCCPJsonStr = getValueFromKeyValueMap(kvMap, RESERVED_CLUSTER_CHECKPOINT_LIST, "");
    if (!reservedCCPJsonStr.empty()) {
        CheckpointList checkpoints;
        if (!checkpoints.loadFromString(reservedCCPJsonStr)) {
            BS_LOG(ERROR, "jsonize checkpoint list from content[%s] failed", reservedCCPJsonStr.c_str());
            return false;
        }
        options.SetReservedVersions(checkpoints.getIdSet());
    }
    return true;
}

void IndexPartitionOptionsWrapper::initOperationIds(const KeyValueMap& kvMap, IndexPartitionOptions& options)
{
    string opIdsStr = getValueFromKeyValueMap(kvMap, OPERATION_IDS, "");
    if (opIdsStr.empty()) {
        return;
    }

    vector<uint32_t> opIds;
    StringUtil::fromString(opIdsStr, opIds, ",");
    options.SetOngoingModifyOperationIds(opIds);
}

void IndexPartitionOptionsWrapper::initGenerationId(const KeyValueMap& kvMap, IndexPartitionOptions& options)
{
    string generationId = getValueFromKeyValueMap(kvMap, GENERATION_ID, "");
    if (generationId.empty()) {
        return;
    }
    options.AddVersionDesc(GENERATION_ID, generationId);
}

void IndexPartitionOptionsWrapper::initSchemaUpdateStandard(const KeyValueMap& kvMap, IndexPartitionOptions& options)
{
    string schemaPatch = getValueFromKeyValueMap(kvMap, SCHEMA_PATCH, "");
    if (schemaPatch.empty()) {
        return;
    }
    UpdateableSchemaStandards standards;
    try {
        FromJsonString(standards, schemaPatch);
        options.SetUpdateableSchemaStandards(standards);
    } catch (...) {
        IE_LOG(ERROR, "invalid schema patch [%s]", schemaPatch.c_str());
    }
}

void IndexPartitionOptionsWrapper::initBatchMask(const KeyValueMap& kvMap, IndexPartitionOptions& options)
{
    string batchMaskStr = getValueFromKeyValueMap(kvMap, BATCH_MASK, "");
    if (batchMaskStr.empty()) {
        return;
    }
    int32_t batchMask;
    if (!StringUtil::fromString(batchMaskStr, batchMask) || batchMask < 0) {
        return;
    }
    options.AddVersionDesc(BATCH_MASK, batchMaskStr);
}

}} // namespace build_service::config
