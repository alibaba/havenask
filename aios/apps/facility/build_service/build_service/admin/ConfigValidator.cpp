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
#include "build_service/admin/ConfigValidator.h"

#include <cstring>

#include "autil/StringUtil.h"
#include "build_service/admin/AppPlanMaker.h"
#include "build_service/config/BuilderClusterConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ControlConfig.h"
#include "build_service/config/DocProcessorChainConfig.h"
#include "build_service/config/HashMode.h"
#include "build_service/config/IndexPartitionOptionsWrapper.h"
#include "build_service/config/OfflineIndexConfigMap.h"
#include "build_service/config/ProcessorConfig.h"
#include "build_service/config/ProcessorRuleConfig.h"
#include "build_service/config/SwiftTopicConfig.h"
#include "build_service/proto/DataDescriptions.h"
#include "build_service/util/RangeUtil.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/schema_differ.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/util/PathUtil.h"
#include "swift/client/RangeUtil.h"

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::util;
using namespace build_service::proto;
using namespace build_service::admin;

using namespace indexlib::config;

namespace build_service { namespace admin {

BS_LOG_SETUP(admin, ConfigValidator);

ConfigValidator::ConfigValidator(const proto::BuildId& buildId)
{
    setBeeperCollector(GENERATION_ERROR_COLLECTOR_NAME);
    initBeeperTag(buildId);
}

ConfigValidator::ConfigValidator() {}

ConfigValidator::~ConfigValidator() {}

#define VALIDATE_CONTAINER(ConfigType, filePath, jsonPath)                                                             \
    do {                                                                                                               \
        ConfigType config##ConfigType;                                                                                 \
        if (!resourceReader->getConfigWithJsonPath(filePath, jsonPath, config##ConfigType)) {                          \
            string errorMsg = "read " + string(filePath) + " from " + configPath + "failed";                           \
            REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);                                                              \
            return false;                                                                                              \
        }                                                                                                              \
        for (ConfigType::const_iterator it = config##ConfigType.begin(); it != config##ConfigType.end(); ++it) {       \
            if (!it->validate()) {                                                                                     \
                string errorMsg = "validate " + string(filePath) + " from " + configPath + "failed";                   \
                REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);                                                          \
                return false;                                                                                          \
            }                                                                                                          \
        }                                                                                                              \
    } while (0)

#define VALIDATE(ConfigType, filePath, jsonPath)                                                                       \
    do {                                                                                                               \
        ConfigType config##ConfigType;                                                                                 \
        if (!resourceReader->getConfigWithJsonPath(filePath, jsonPath, config##ConfigType)) {                          \
            string errorMsg = "read " + string(filePath) + " from " + configPath + "failed";                           \
            REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);                                                              \
            return false;                                                                                              \
        }                                                                                                              \
        if (!config##ConfigType.validate()) {                                                                          \
            string errorMsg = "validate " + string(filePath) + " from " + configPath + "failed";                       \
            REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);                                                              \
            return false;                                                                                              \
        }                                                                                                              \
    } while (0)

template <typename T>
bool ConfigValidator::getValidateConfig(const ResourceReader& resourceReader, const std::string& filePath,
                                        const std::string& jsonPath, T& configObject)
{
    if (!resourceReader.getConfigWithJsonPath(filePath, jsonPath, configObject)) {
        string errorMsg = "read " + filePath + "failed";
        REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
        return false;
    }
    if (!configObject.validate()) {
        string errorMsg = "validate " + filePath + "failed";
        REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
        return false;
    }
    return true;
}

bool ConfigValidator::validateSchema(const config::ResourceReaderPtr& resourceReader, const std::string& clusterName,
                                     bool buildV2Mode)
{
    if (!buildV2Mode) {
        auto schema = resourceReader->getSchema(clusterName);
        if (!schema) {
            BS_LOG(ERROR, "get index partition schema for cluster [%s] failed", clusterName.c_str());
            return false;
        }
        return true;
    }
    // check v2 not support feature
    auto tabletSchema = resourceReader->getTabletSchema(clusterName);
    if (!tabletSchema) {
        string errorMsg = "schema is not valid for cluster " + clusterName;
        REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
        return false;
    }
    auto legacySchema = tabletSchema->GetLegacySchema();
    if (legacySchema) {
        if (legacySchema->GetSubIndexPartitionSchema()) {
            REPORT_ERROR(SERVICE_ERROR_CONFIG, string("v2 build mode not support sub schema"));
            return false;
        }
        if (legacySchema->GetSourceSchema()) {
            REPORT_ERROR(SERVICE_ERROR_CONFIG, string("v2 build mode not support source schema"));
            return false;
        }
        if (legacySchema->GetRegionCount() > 1) {
            REPORT_ERROR(SERVICE_ERROR_CONFIG, string("v2 build mode not support multi region"));
            return false;
        }
        auto indexSchema = legacySchema->GetIndexSchema();
        if (indexSchema) {
            for (auto iter = indexSchema->Begin(); iter != indexSchema->End(); iter++) {
                if ((*iter)->GetInvertedIndexType() == indexlib::it_trie) {
                    REPORT_ERROR(SERVICE_ERROR_CONFIG, string("v2 build mode not support trie index"));
                    return false;
                }
            }
        }
    }
    return true;
}

bool ConfigValidator::validate(const string& configPath, bool firstTime)
{
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(configPath);
    GraphConfig graphConfig;
    bool globalBuildV2Mode = false;
    if (resourceReader->getGraphConfig(graphConfig)) {
        auto targetGraph = graphConfig.getGraphName();
        globalBuildV2Mode = (targetGraph == "BatchBuildV2/FullBatchBuild.graph" ||
                             targetGraph == "BuildIndexV2.graph" || targetGraph == "BuildIndexV2.npc_mode.graph");
    }
    BuildServiceConfig buildServiceConfig;
    if (!resourceReader->getConfigWithJsonPath((BUILD_APP_FILE_NAME.c_str()), "", buildServiceConfig)) {
        string errorMsg = "read " + string((BUILD_APP_FILE_NAME.c_str())) + " from " + configPath + "failed";
        REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
        return false;
    }
    if (!buildServiceConfig.validate()) {
        string errorMsg = "validate " + string((BUILD_APP_FILE_NAME.c_str())) + " from " + configPath + "failed";
        REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
        return false;
    }

    vector<string> dataTables;
    if (!resourceReader->getAllDataTables(dataTables)) {
        string errorMsg = "read data table config[" + configPath + "] failed!";
        REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
        return false;
    }

    for (size_t i = 0; i < dataTables.size(); ++i) {
        bool buildV2Mode = globalBuildV2Mode;
        if (!buildV2Mode) {
            config::ControlConfig controlConfig;
            if (!resourceReader->getDataTableConfigWithJsonPath(dataTables[i], "control_config", controlConfig)) {
                string errorMsg = "get control config failed!";
                REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
                return false;
            }
            buildV2Mode = (controlConfig.useIndexV2() ||
                           controlConfig.getDataLinkMode() == ControlConfig::DataLinkMode::NPC_MODE);
        }
        string dataTableFilePath = ResourceReader::getDataTableRelativePath(dataTables[i]);
        do {
            DataDescriptions dataDescriptions;
            if (!resourceReader->getConfigWithJsonPath(dataTableFilePath, "data_descriptions",
                                                       dataDescriptions.toVector())) {
                string errorMsg = "read " + dataTableFilePath + " from " + configPath + " failed";
                REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
                return false;
            }
            if (!dataDescriptions.validate()) {
                string errorMsg = "validate " + dataTableFilePath + " from " + configPath + " failed";
                REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
                return false;
            }
        } while (0);

        VALIDATE(ProcessorRuleConfig, dataTableFilePath.c_str(), "processor_rule_config");
        VALIDATE(ProcessorConfig, dataTableFilePath.c_str(), "processor_config");
        VALIDATE_CONTAINER(DocProcessorChainConfigVec, dataTableFilePath.c_str(), "processor_chain_config");
        vector<string> clusterNames;
        if (!resourceReader->getAllClusters(dataTables[i], clusterNames)) {
            string errorMsg = "read cluster config[" + configPath + "] failed!";
            REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
            return false;
        }
        for (size_t i = 0; i < clusterNames.size(); i++) {
            if (!validateSchema(resourceReader, clusterNames[i], buildV2Mode)) {
                string errorMsg = "validate schema for cluster [" + clusterNames[i] + "] failed";
                REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
                return false;
            }
            if (buildV2Mode) {
                BuilderClusterConfig clusterConfig;
                if (!clusterConfig.init(clusterNames[i], *resourceReader)) {
                    REPORT_ERROR(SERVICE_ERROR_CONFIG, string("get cluster config failed"));
                    return false;
                }
                if (clusterConfig.enableFastSlowQueue) {
                    REPORT_ERROR(SERVICE_ERROR_CONFIG, string("buildV2 mode not support fast slow mode now"));
                    return false;
                }
            }
            string clusterConfig = ResourceReader::getClusterConfRelativePath(clusterNames[i]);
            BuildRuleConfig buildRuleConfig;
            if (!getValidateConfig<BuildRuleConfig>(*(resourceReader.get()), clusterConfig.c_str(),
                                                    "cluster_config.builder_rule_config", buildRuleConfig)) {
                return false;
            }
            SwiftConfig swiftConfig;
            if (!getValidateConfig<SwiftConfig>(*(resourceReader.get()), clusterConfig.c_str(), "swift_config",
                                                swiftConfig)) {
                return false;
            }
            if (!validateHashMode(resourceReader, clusterConfig.c_str())) {
                return false;
            }

            if (!validateBuilderClusterConfig(configPath, clusterNames[i])) {
                return false;
            }
            if (!validateOfflineIndexPartitionOptions(configPath, clusterNames[i])) {
                return false;
            }
            if (!validateSwiftMemoryPreferMode(swiftConfig, buildRuleConfig, buildV2Mode)) {
                return false;
            }
            if (!validateSwiftConfig(swiftConfig, buildServiceConfig, buildV2Mode)) {
                return false;
            }
        }
    }
    if (!validateAppPlanMaker(configPath)) {
        return false;
    }
    if (firstTime) {
        vector<string> patchShemas = resourceReader->getAllSchemaPatchFileNames();
        if (!patchShemas.empty()) {
            string errorMsg = "first time start build should not take schema patch file in" + configPath;
            REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
            return false;
        }
    }
    return true;
}

#undef VALIDATE
#undef VALIDATE_CONTAINER

bool ConfigValidator::validateHashMode(const config::ResourceReaderPtr& resourceReader, const std::string& configPath)
{
    // one of HashMode or RegionHashMode is validate will be ok
    HashMode hashModeConfig;
    string errorMsg = "hash mode is not validate from " + configPath;
    if (resourceReader->getConfigWithJsonPath(configPath, "cluster_config.hash_mode", hashModeConfig)) {
        if (hashModeConfig.validate()) {
            return true;
        }
    }
    RegionHashModes regHashModeConfigs;
    if (resourceReader->getConfigWithJsonPath(configPath, "cluster_config.region_hash_mode", regHashModeConfigs)) {
        if (regHashModeConfigs.size() == 0) {
            REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
            return false;
        }
        for (auto it = regHashModeConfigs.begin(); it != regHashModeConfigs.end(); ++it) {
            if (!it->validate()) {
                REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
                return false;
            }
        }
        return true;
    }
    REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
    return false;
}

bool ConfigValidator::validateBuilderClusterConfig(const string& configPath, const string& clusterName)
{
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(configPath);
    BuilderClusterConfig config;
    if (!config.init(clusterName, *(resourceReader.get()), "")) {
        return false;
    }
    if (!config.validate()) {
        return false;
    }
    if (!config.builderConfig.sortBuild) {
        return true;
    }
    auto schema = resourceReader->getTabletSchema(clusterName);
    if (!schema) {
        string errorMsg = "load schema for cluster[" + clusterName + "] failed.";
        REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
        return false;
    }

    if (schema->GetLegacySchema() && schema->GetLegacySchema()->GetRegionCount() > 1) {
        string errorMsg = "multi region schema for cluster[" + clusterName + "] not support sort builder!";
        REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
        return false;
    }

    if (config.builderConfig.sortDescriptions.empty() && schema->GetTableType() != indexlib::table::TABLE_TYPE_KKV) {
        string errorMsg = "sort_descriptions can not be empty in sortBuild";
        REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);

        return false;
    }

    const auto& fieldConfigs = schema->GetFieldConfigs();
    if (fieldConfigs.empty()) {
        return false;
    }
    for (size_t i = 0; i < config.builderConfig.sortDescriptions.size(); i++) {
        const string& sortFieldName = config.builderConfig.sortDescriptions[i].GetSortFieldName();
        auto fieldConfig = schema->GetFieldConfig(sortFieldName);
        if (!fieldConfig) {
            string errorMsg = "sortField[" + sortFieldName + "] does not exist in schema";
            REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
            return false;
        }
        if (!fieldConfig->SupportSort()) {
            string errorMsg = "[" + sortFieldName + "] does not support sort";
            REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
            return false;
        }
        const auto& config = schema->GetIndexConfig(indexlib::index::ATTRIBUTE_INDEX_TYPE_STR, sortFieldName);
        if (!config) {
            string errorMsg = "sortField[" + sortFieldName + "]does not exist in attribute";
            REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
            return false;
        }
        const auto& attributeConfig = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(config);
        assert(attributeConfig);
        CompressTypeOption compressType = attributeConfig->GetCompressType();
        if (compressType.HasFp16EncodeCompress() || compressType.HasInt8EncodeCompress()) {
            string errorMsg = "sort field [" + sortFieldName + "] not support fp16 or int8 compress";
            REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
            return false;
        }
    }
    return true;
}

bool ConfigValidator::validateOfflineIndexPartitionOptions(const string& configPath, const string& clusterName)
{
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(configPath);
    IndexPartitionOptionsWrapper optionWrapper;
    if (!optionWrapper.load(*(resourceReader.get()), clusterName)) {
        return false;
    }
    const IndexPartitionOptionsWrapper::IndexPartitionOptionsMap& optionsMap =
        optionWrapper.getIndexPartitionOptionsMap();

    IndexPartitionOptionsWrapper::IndexPartitionOptionsMap::const_iterator it = optionsMap.begin();

    for (; it != optionsMap.end(); ++it) {
        const indexlib::config::IndexPartitionOptions& indexOption = it->second;
        try {
            indexOption.Check();
        } catch (const autil::legacy::ExceptionBase& e) {
            string errorMsg = "check IndexPartitionOptions for [" + clusterName + "].[" + it->first +
                              "] failed, error[" + string(e.what()) + "]";
            REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
            return false;
        }
    }
    return true;
}

bool ConfigValidator::isSchemaEqual(const IndexPartitionSchemaPtr& originalSchema,
                                    const IndexPartitionSchemaPtr& updateSchema)
{
    try {
        originalSchema->AssertEqual(*updateSchema);
        return true;
    } catch (const autil::legacy::ExceptionBase& e) {
        string errorMsg = "schema not equal:[" + string(e.what()) + "]";
        REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
        return false;
    }
    return false;
}

ConfigValidator::SchemaUpdateStatus ConfigValidator::checkUpdateSchema(const ResourceReaderPtr& originalResourceReader,
                                                                       const ResourceReaderPtr& newResourceReader,
                                                                       const string& clusterName)
{
    if (autil::EnvUtil::getEnv(BS_ENV_ALLOW_UPDATE_SCHEMA, false)) {
        BS_LOG(WARN, "allow update schema without check");
        return NO_UPDATE_SCHEMA;
    }

    IndexPartitionSchemaPtr originalSchema = originalResourceReader->getSchema(clusterName);
    IndexPartitionSchemaPtr updateSchema = newResourceReader->getSchema(clusterName);
    if (!originalSchema) {
        string errorMsg = "cluster [" + clusterName + "] get origianl schema failed, config path [" +
                          originalResourceReader->getConfigPath() + "]";
        REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
        return UPDATE_ILLEGAL;
    }

    if (!updateSchema) {
        string errorMsg = "cluster [" + clusterName + "] get upadte schema failed, config path [" +
                          newResourceReader->getConfigPath() + "]";
        REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
        return UPDATE_ILLEGAL;
    }

    UpdateableSchemaStandardsPtr standards(new UpdateableSchemaStandards());
    if (!newResourceReader->getSchemaPatch(clusterName, standards)) {
        string errorMsg = "cluster [ " + clusterName + " ] patch schema  invalid, update config path [" +
                          newResourceReader->getConfigPath() + "]";
        REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
        return UPDATE_ILLEGAL;
    }

    int64_t updateSchemaId = updateSchema->GetSchemaVersionId();
    int64_t originalSchemaId = originalSchema->GetSchemaVersionId();
    if (updateSchemaId == originalSchemaId) {
        if (!isSchemaEqual(originalSchema, updateSchema)) {
            string errorMsg = "cluster [ " + clusterName + " ] schema not equal between original config path[" +
                              originalResourceReader->getConfigPath() + "] and update config path [" +
                              newResourceReader->getConfigPath() + "]";
            REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
            return UPDATE_ILLEGAL;
        }
        if (standards != nullptr) {
            if (!standards->CheckConfigValid(originalSchema)) {
                string errorMsg = "cluster [ " + clusterName + " ] update standard is invalid " +
                                  "update config path [" + newResourceReader->getConfigPath() + "]";
                REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
                return UPDATE_ILLEGAL;
            }
            if (originalSchema->HasUpdateableStandard()) {
                return UPDATE_STANDARD;
            } else {
                string errorMsg = "cluster [ " + clusterName + " ] orginal schema not support update standard" +
                                  originalResourceReader->getConfigPath() + "] and update config path [" +
                                  newResourceReader->getConfigPath() + "]";
                REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
                return UPDATE_ILLEGAL;
            }
        }
        return NO_UPDATE_SCHEMA;
    }

    string errorMsg;
    if (indexlib::config::SchemaDiffer::CheckAlterFields(originalSchema, updateSchema, errorMsg)) {
        if (standards != nullptr) {
            string errorMsg = "cluster [ " + clusterName +
                              " ] update schema patch && schema field at same time, which is not allow[" +
                              originalResourceReader->getConfigPath() + "] and update config path [" +
                              newResourceReader->getConfigPath() + "]";
            REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
            return UPDATE_ILLEGAL;
        }
        return UPDATE_SCHEMA;
    }
    REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
    return UPDATE_ILLEGAL;
}

ConfigValidator::SchemaUpdateStatus ConfigValidator::checkUpdateSchema(const string& originalConfigPath,
                                                                       const string& newConfigPath,
                                                                       const string& clusterName)
{
    ResourceReaderPtr originalResourceReader = ResourceReaderManager::getResourceReader(originalConfigPath);
    ResourceReaderPtr newResourceReader = ResourceReaderManager::getResourceReader(newConfigPath);
    return checkUpdateSchema(originalResourceReader, newResourceReader, clusterName);
}

string ConfigValidator::getErrorMsg()
{
    std::vector<ErrorInfo> errorInfos;
    fillErrorInfos(errorInfos);
    return ErrorCollector::errorInfosToString(errorInfos);
}

bool ConfigValidator::validateAppPlanMaker(const string& configPath)
{
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(configPath);
    AppPlanMaker appPlanMaker("configValidater", "rpc");
    if (!resourceReader->getConfigWithJsonPath(HIPPO_FILE_NAME, "", appPlanMaker)) {
        string errorMsg = "parse hippo config failed";
        REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
        return false;
    }
    vector<string> clusterNames;
    if (!resourceReader->getAllClusters(clusterNames)) {
        string errorMsg = "read config failed!";
        REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
        return false;
    }
    if (clusterNames.size() == 0) {
        string errorMsg = "no cluster specified";
        REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
        return false;
    }
    map<string, vector<string>> mergeNames;
    for (size_t i = 0; i < clusterNames.size(); i++) {
        string clusterConfig = resourceReader->getClusterConfRelativePath(clusterNames[i]);
        OfflineIndexConfigMap configMap;
        if (!resourceReader->getConfigWithJsonPath(clusterConfig, "offline_index_config", configMap)) {
            string errorMsg = "read offline_index_config from " + clusterConfig + " failed";
            REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
            return false;
        }

        OfflineIndexConfigMap::ConstIterator it = configMap.begin();
        for (; it != configMap.end(); ++it) {
            if (it->first != "") {
                const string& mergeName = it->first;
                mergeNames[clusterNames[i]].push_back(mergeName);
            }
        }
    }

    vector<string> dataTables;
    if (!resourceReader->getAllDataTables(dataTables)) {
        string errorMsg = "get all data tables failed";
        REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
        return false;
    }
    if (!appPlanMaker.validate(clusterNames, mergeNames, dataTables)) {
        string errorMsg = "validate hippo config failed";
        REPORT_ERROR(SERVICE_ERROR_CONFIG, errorMsg);
        return false;
    }

    return true;
}

bool ConfigValidator::validateSwiftMemoryPreferMode(const SwiftConfig& swiftConfig,
                                                    const BuildRuleConfig& buildRuleConfig, bool buildV2Mode)
{
    const SwiftTopicConfigPtr& swiftTopicConfig = swiftConfig.getFullBrokerTopicConfig();
    if (!swiftTopicConfig->hasPartitionRestrict()) {
        return true;
    }
    if (buildV2Mode) {
        REPORT_ERROR(SERVICE_ERROR_CONFIG, string("buildV2 not support build with swift memory prefer mode"));
        return false;
    }
    std::vector<proto::Range> buildParition = util::RangeUtil::splitRange(
        RANGE_MIN, RANGE_MAX, buildRuleConfig.partitionCount, buildRuleConfig.buildParallelNum);

    uint32_t swiftPartitionCount = swiftTopicConfig->getPartitionCount();
    swift::client::RangeUtil swiftRangeUtil(swiftPartitionCount);
    bool swiftBuildNum[swiftPartitionCount + 1];
    memset(swiftBuildNum, 0, sizeof(bool) * (swiftPartitionCount + 1));

    for (auto& range : buildParition) {
        std::vector<uint32_t> ids = swiftRangeUtil.getPartitionIds(range.from(), range.to());
        for (uint32_t id : ids) {
            if (id >= swiftPartitionCount) {
                stringstream ss;
                ss << "swift partition id invalid, the id is " << id
                   << ", swift_partition_count: " << swiftPartitionCount;
                REPORT_ERROR(SERVICE_ERROR_CONFIG, ss.str());
                return false;
            } else if (swiftBuildNum[id]) {
                stringstream ss;
                ss << "two builder would read from the same "
                   << "swfit-partition, swift_config.topic_full."
                   << "parition_count: " << swiftPartitionCount
                   << ", builder_rule_config. partition_count: " << buildRuleConfig.partitionCount
                   << ", builder_rule_config."
                   << "build_parallel_num: " << buildRuleConfig.buildParallelNum;
                REPORT_ERROR(SERVICE_ERROR_CONFIG, ss.str());
                return false;
            }
            swiftBuildNum[id] = true;
        }
    }
    return true;
}

bool ConfigValidator::validateSwiftConfig(const SwiftConfig& swiftConfig, const BuildServiceConfig& buildServiceConfig,
                                          bool buildV2Mode)
{
    if (buildServiceConfig.getSwiftRoot(true) != buildServiceConfig.getSwiftRoot(false)) {
        if (!swiftConfig.hasTopicConfig(config::FULL_SWIFT_BROKER_TOPIC_CONFIG) ||
            !swiftConfig.hasTopicConfig(config::INC_SWIFT_BROKER_TOPIC_CONFIG)) {
            stringstream ss;
            ss << "set diff swift cluster config [" << buildServiceConfig.getSwiftRoot(true) << "] and ["
               << buildServiceConfig.getSwiftRoot(false) << "], should set both full and inc swift topic";
            REPORT_ERROR(SERVICE_ERROR_CONFIG, ss.str());
            return false;
        }
    }
    if (buildV2Mode) {
        auto swiftWriterConfig = swiftConfig.getSwiftWriterConfig(config::INC_SWIFT_BROKER_TOPIC_CONFIG);
        if (swiftWriterConfig.find("mergeMessage=true") != std::string::npos) {
            REPORT_ERROR(SERVICE_ERROR_CONFIG, string("buildv2 not support swift merge message"));
            return false;
        }
        swiftWriterConfig = swiftConfig.getSwiftWriterConfig(config::FULL_SWIFT_BROKER_TOPIC_CONFIG);
        if (swiftWriterConfig.find("mergeMessage=true") != std::string::npos) {
            REPORT_ERROR(SERVICE_ERROR_CONFIG, string("buildv2 not support swift merge message"));
            return false;
        }
    }
    return true;
}

}} // namespace build_service::admin
