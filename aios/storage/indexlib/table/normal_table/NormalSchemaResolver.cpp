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
#include "indexlib/table/normal_table/NormalSchemaResolver.h"

#include "autil/Scope.h"
#include "indexlib/config/BuildOptionConfig.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/FileCompressConfigV2.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/SingleFileCompressConfig.h"
#include "indexlib/config/SortDescription.h"
#include "indexlib/config/SortParam.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/config/date_index_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/range_index_config.h"
#include "indexlib/document/document_rewriter/DocumentInfoToAttributeRewriter.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/index/ann/ANNIndexConfig.h"
#include "indexlib/index/ann/Common.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/deletionmap/DeletionMapConfig.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/DateIndexConfig.h"
#include "indexlib/index/inverted_index/config/HighFreqVocabularyCreator.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/config/PayloadConfig.h"
#include "indexlib/index/inverted_index/config/RangeIndexConfig.h"
#include "indexlib/index/inverted_index/config/SpatialIndexConfig.h"
#include "indexlib/index/inverted_index/config/TruncateIndexNameMapper.h"
#include "indexlib/index/inverted_index/config/TruncateProfileConfig.h"
#include "indexlib/index/inverted_index/config/TruncateStrategy.h"
#include "indexlib/index/operation_log/OperationLogConfig.h"
#include "indexlib/index/pack_attribute/Common.h"
#include "indexlib/index/pack_attribute/PackAttributeConfig.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"
#include "indexlib/index/primary_key/config/PrimaryKeyLoadStrategyParam.h"
#include "indexlib/index/statistics_term/Constant.h"
#include "indexlib/index/statistics_term/StatisticsTermIndexConfig.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/legacy/index_base/PartitionMeta.h"
#include "indexlib/legacy/index_base/SchemaAdapter.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/virtual_attribute/VirtualAttributeConfig.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalSchemaResolver);

namespace {
Status ResolveFileCompressConfig(
    const std::shared_ptr<std::vector<config::SingleFileCompressConfig>>& singleFileCompressConfigVec,
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> invertedIndexConfig)
{
    // TODO: add test: no compress config in setting
    auto fileCompressConfig = invertedIndexConfig->GetFileCompressConfig();
    if (!fileCompressConfig) {
        return Status::OK();
    }
    std::string compressName = fileCompressConfig->GetCompressName();
    auto fileCompressConfigV2 =
        std::make_shared<config::FileCompressConfigV2>(singleFileCompressConfigVec, compressName);
    invertedIndexConfig->SetFileCompressConfigV2(fileCompressConfigV2);
    return Status::OK();
}

Status ResolveFileCompressConfig(
    const std::shared_ptr<std::vector<config::SingleFileCompressConfig>>& singleFileCompressConfigVec,
    std::shared_ptr<indexlibv2::index::AttributeConfig> attrConfig)
{
    // TODO: add test: no compress config in setting
    auto fileCompressConfig = attrConfig->GetFileCompressConfig();
    if (!fileCompressConfig) {
        return Status::OK();
    }
    std::string compressName = fileCompressConfig->GetCompressName();
    auto fileCompressConfigV2 =
        std::make_shared<config::FileCompressConfigV2>(singleFileCompressConfigVec, compressName);
    attrConfig->SetFileCompressConfigV2(fileCompressConfigV2);
    return Status::OK();
}

Status ResolveFileCompressConfig(
    const std::shared_ptr<std::vector<config::SingleFileCompressConfig>>& singleFileCompressConfigVec,
    std::shared_ptr<indexlibv2::index::PackAttributeConfig> packAttrConfig)
{
    auto fileCompressConfig = packAttrConfig->GetFileCompressConfig();
    if (!fileCompressConfig) {
        return Status::OK();
    }
    std::string compressName = fileCompressConfig->GetCompressName();
    auto fileCompressConfigV2 =
        std::make_shared<config::FileCompressConfigV2>(singleFileCompressConfigVec, compressName);
    packAttrConfig->SetFileCompressConfigV2(fileCompressConfigV2);
    return Status::OK();
}

Status ResolveFileCompressConfig(
    const std::shared_ptr<std::vector<config::SingleFileCompressConfig>>& singleFileCompressConfigVec,
    std::shared_ptr<indexlibv2::config::SummaryIndexConfig>& summaryIndexConfig)
{
    for (summarygroupid_t groupId = 0; groupId < summaryIndexConfig->GetSummaryGroupConfigCount(); ++groupId) {
        auto groupConfig = summaryIndexConfig->GetSummaryGroupConfig(groupId);
        auto currentParam = groupConfig->GetSummaryGroupDataParam();
        auto legacyCompressConfig = currentParam.GetFileCompressConfig();
        if (!legacyCompressConfig) {
            continue;
        }
        std::string compressName = legacyCompressConfig->GetCompressName();
        auto fileCompressConfigV2 =
            std::make_shared<config::FileCompressConfigV2>(singleFileCompressConfigVec, compressName);
        currentParam.SetFileCompressConfigV2(fileCompressConfigV2);
        groupConfig->SetSummaryGroupDataParam(currentParam);
    }
    return Status::OK();
}
} // namespace

Status NormalSchemaResolver::CheckStatisticsTermConfig(const config::TabletSchema& schema) const
{
    for (auto indexConfig : schema.GetIndexConfigs(index::STATISTICS_TERM_INDEX_TYPE_STR)) {
        auto statConfig = std::dynamic_pointer_cast<index::StatisticsTermIndexConfig>(indexConfig);
        assert(statConfig != nullptr);
        for (auto invertedIndexName : statConfig->GetInvertedIndexNames()) {
            auto invertedIndexConfig =
                schema.GetIndexConfig(indexlib::index::INVERTED_INDEX_TYPE_STR, invertedIndexName);
            if (invertedIndexConfig == nullptr) {
                AUTIL_LOG(ERROR, "associated inverted index not exist in schema, index[%s] invert index[%s]",
                          statConfig->GetIndexName().c_str(), invertedIndexName.c_str());
                return Status::InternalError("associated inverted index not exist in schema");
            }
        }
    }
    return Status::OK();
}

Status NormalSchemaResolver::CheckIndexTruncateProfiles(
    const std::vector<indexlibv2::config::TruncateProfileConfig>& truncateProfileConfigs,
    const config::TabletSchema& schema, const std::shared_ptr<config::InvertedIndexConfig>& indexConfig) const
{
    std::vector<std::string> profileNames = indexConfig->GetUseTruncateProfiles();
    for (const auto& profileName : profileNames) {
        bool find = false;
        for (const auto& truncateProfileConfig : truncateProfileConfigs) {
            if (truncateProfileConfig.GetTruncateProfileName() == profileName) {
                find = true;
                auto sortParams = truncateProfileConfig.GetTruncateSortParams();
                for (auto sortParam : sortParams) {
                    std::string sortField = sortParam.GetSortField();

                    auto fieldConfig = schema.GetFieldConfig(sortField);
                    if (!fieldConfig) {
                        // may be DOC_PAYLOAD
                        continue;
                    }
                    auto attributeConfig = std::dynamic_pointer_cast<index::AttributeConfig>(
                        schema.GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, sortField));
                    assert(attributeConfig);
                    indexlib::config::CompressTypeOption compress = attributeConfig->GetCompressType();
                    if (compress.HasFp16EncodeCompress() || compress.HasInt8EncodeCompress()) {
                        return Status::ConfigError("invalid field[%s] for truncate profile name [%s] of index [%s]",
                                                   fieldConfig->GetFieldName().c_str(), profileName.c_str(),
                                                   indexConfig->GetIndexName().c_str());
                    }
                }
                break;
            }
        }
        if (!find) {
            return Status::ConfigError("index [%s] truncate profile name [%s] is invalid",
                                       indexConfig->GetIndexName().c_str(), profileName.c_str());
        }
    }
    return Status::OK();
}

Status NormalSchemaResolver::CheckInvertedIndexConfig(const config::TabletSchema& schema) const
{
    uint32_t fieldCount = schema.GetFieldConfigs().size();
    std::vector<uint32_t> singleFieldIndexCounts(fieldCount, 0);
    std::map<indexlib::fieldid_t, std::string> fieldId2IndexName;
    std::map<std::string, std::set<std::string>> singleFieldIndexConfigsWithProfileNames;
    auto invertedIndexConfigs = schema.GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR);
    auto [truncateStatus, truncateProfileConfigs] =
        schema.GetRuntimeSettings().GetValue<std::vector<indexlibv2::config::TruncateProfileConfig>>(
            "truncate_profiles");

    auto pkIndexConfig = schema.GetPrimaryKeyIndexConfig();
    if (pkIndexConfig) {
        invertedIndexConfigs.push_back(pkIndexConfig);
    }
    for (const auto& indexConfig : invertedIndexConfigs) {
        std::string indexName = indexConfig->GetIndexName();
        if (indexName == index::SUMMARY_INDEX_NAME) {
            return Status::ConfigError("index name can not use summary");
        }
        try {
            indexConfig->Check();
        } catch (const std::exception& e) {
            AUTIL_LOG(ERROR, "check invertedIndexConfig [%s] failed exception [%s].", indexName.c_str(), e.what());
            return Status::ConfigError("check invertedIndexConfig failed");
        }
        auto invertedIndexConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(indexConfig);
        std::vector<std::string> profileNames = invertedIndexConfig->GetUseTruncateProfiles();
        std::set<std::string> profileNameSet(profileNames.begin(), profileNames.end());
        if (profileNames.size() != profileNameSet.size()) {
            return Status::ConfigError("index [%s] has duplicate profile name", indexName.c_str());
        }
        if (invertedIndexConfig->HasTruncate()) {
            if (!truncateStatus.IsOK()) {
                return Status::ConfigError("index [%s] get truncate config failed", indexName.c_str());
            }
            auto status = CheckIndexTruncateProfiles(truncateProfileConfigs, schema, invertedIndexConfig);
            RETURN_IF_STATUS_ERROR(status, "check truncate profiles for index [%s] failed",
                                   indexConfig->GetIndexName().c_str());
        }

        InvertedIndexType indexType = invertedIndexConfig->GetInvertedIndexType();
        if (indexType == it_pack || indexType == it_expack || indexType == it_customized) {
            RETURN_IF_STATUS_ERROR(CheckFieldsOrderInPackIndex(invertedIndexConfig, schema), "check index [%s] failed",
                                   indexName.c_str());
        } else {
            RETURN_IF_STATUS_ERROR(CheckSingleFieldIndex(invertedIndexConfig, schema, &fieldId2IndexName,
                                                         &singleFieldIndexConfigsWithProfileNames),
                                   "check index [%s] failed", indexName.c_str());
        }

        if (indexType == it_spatial) {
            RETURN_IF_STATUS_ERROR(CheckSpatialIndexConfig(invertedIndexConfig, schema), "check index [%s] failed",
                                   indexName.c_str());
        }
    }

    return Status::OK();
}

Status
NormalSchemaResolver::CheckSpatialIndexConfig(const std::shared_ptr<config::InvertedIndexConfig>& spatialIndexConfig,
                                              const config::TabletSchema& schema) const
{
    auto fieldConfigs = spatialIndexConfig->GetFieldConfigs();
    assert(fieldConfigs.size() == 1);
    // TODO: support line and polygon
    if (fieldConfigs[0]->GetFieldType() != ft_location) {
        return Status::OK();
    }
    std::string fieldName = fieldConfigs[0]->GetFieldName();
    auto attributeConfig = schema.GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, fieldName);
    if (!attributeConfig) {
        return Status::ConfigError("field [%s] should in attributes, because in spatial index [%s]", fieldName.c_str(),
                                   spatialIndexConfig->GetIndexName().c_str());
    }
    // TODO (zhuanghaolin.zhl) remove when support pack attribute
    // if (attrConf->GetPackAttributeConfig()) {
    //     INDEXLIB_FATAL_ERROR(Schema, "field [%s] should not in pack attribute, because in spatial index [%s]",
    //                          fieldName.c_str(), spatialIndexConf->GetIndexName().c_str());
    // }

    return Status::OK();
}
// The check is intended to prevent adding redundant single field inverted index configs.
// However, multiple single field index configs are necessary to support different term payload loads.
// Thus multiple single field index configs with payload names are allowed.
Status NormalSchemaResolver::CheckSingleFieldIndex(
    const std::shared_ptr<config::InvertedIndexConfig>& invertedIndexConfig, const config::TabletSchema& schema,
    std::map<fieldid_t, std::string>* fieldId2IndexName,
    std::map<std::string, std::set<std::string>>* singleFieldIndexConfigsWithProfileNames) const
{
    for (const auto& fieldConfig : schema.GetFieldConfigs()) {
        fieldid_t fieldId = fieldConfig->GetFieldId();
        if (!invertedIndexConfig->IsInIndex(fieldId)) {
            continue;
        }

        std::string fieldName = fieldConfig->GetFieldName();
        std::vector<std::string> profileNames = invertedIndexConfig->GetUseTruncateProfiles();
        std::set<std::string> profileNameSet(profileNames.begin(), profileNames.end());
        if (singleFieldIndexConfigsWithProfileNames->find(fieldName) !=
            singleFieldIndexConfigsWithProfileNames->end()) {
            for (const std::string& profileName : profileNameSet) {
                if (singleFieldIndexConfigsWithProfileNames->at(fieldName).find(profileName) !=
                    singleFieldIndexConfigsWithProfileNames->at(fieldName).end()) {
                    return Status::ConfigError("single field [%s] has more than one index with the same profile [%s]",
                                               fieldName.c_str(), profileName.c_str());
                } else {
                    singleFieldIndexConfigsWithProfileNames->at(fieldName).insert(profileName);
                }
            }
        } else {
            (*singleFieldIndexConfigsWithProfileNames)[fieldName] = std::set<std::string> {profileNameSet};
        }

        auto iter = fieldId2IndexName->find(fieldId);
        if (iter != fieldId2IndexName->end()) {
            std::string indexName = iter->second;
            auto lastInvertedIndexConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(
                schema.GetIndexConfig(indexlib::index::INVERTED_INDEX_TYPE_STR, indexName));
            assert(lastInvertedIndexConfig);
            auto hasEmptyNamePayLoadConfig =
                [](const std::shared_ptr<config::InvertedIndexConfig>& invertedIndexConfig) {
                    if (invertedIndexConfig->GetShardingType() ==
                        config::InvertedIndexConfig::IndexShardingType::IST_NEED_SHARDING) {
                        for (const auto& shardConfig : invertedIndexConfig->GetShardingIndexConfigs()) {
                            auto payloadConfig = shardConfig->GetTruncatePayloadConfig();
                            if (!payloadConfig.IsInitialized()) {
                                return true;
                            }
                        }
                        return false;
                    } else {
                        auto payloadConfig = invertedIndexConfig->GetTruncatePayloadConfig();
                        return !payloadConfig.IsInitialized();
                    }
                };
            if (hasEmptyNamePayLoadConfig(lastInvertedIndexConfig) || hasEmptyNamePayLoadConfig(invertedIndexConfig)) {
                return Status::ConfigError("field [%s] has more than one single field index and payload name is empty",
                                           fieldName.c_str());
            }
        }
        (*fieldId2IndexName)[fieldId] = invertedIndexConfig->GetIndexName();
    }
    return Status::OK();
}

Status NormalSchemaResolver::CheckFieldsOrderInPackIndex(
    const std::shared_ptr<config::InvertedIndexConfig>& invertedIndexConfig, const config::TabletSchema& schema) const
{
    // some legacy index config cannot convert
    auto packageConfigV2 = std::dynamic_pointer_cast<config::PackageIndexConfig>(invertedIndexConfig);
    auto packageConfigV1 = std::dynamic_pointer_cast<indexlib::config::PackageIndexConfig>(invertedIndexConfig);
    if (!packageConfigV2 && !packageConfigV1) {
        return Status::ConfigError("get package config failed for index [%s]",
                                   invertedIndexConfig->GetIndexName().c_str());
    }
    int32_t lastFieldPosition = -1;
    fieldid_t lastFieldId = -1;
    for (const auto& fieldConfig : schema.GetFieldConfigs()) {
        fieldid_t fieldId = fieldConfig->GetFieldId();
        if (packageConfigV1 && !packageConfigV1->IsInIndex(fieldId)) {
            continue;
        }
        if (packageConfigV2 && !packageConfigV2->IsInIndex(fieldId)) {
            continue;
        }
        int32_t curFieldPosition =
            packageConfigV2 ? packageConfigV2->GetFieldIdxInPack(fieldId) : packageConfigV1->GetFieldIdxInPack(fieldId);
        if (curFieldPosition < lastFieldPosition) {
            std::string beforeFieldName = schema.GetFieldConfig(lastFieldId)->GetFieldName();
            std::string afterFieldName = schema.GetFieldConfig(fieldId)->GetFieldName();
            return Status::ConfigError(
                "wrong field order in indexConfig [%s], expect field [%s] before field [%s], but not",
                invertedIndexConfig->GetIndexName().c_str(), beforeFieldName.c_str(), afterFieldName.c_str());
        }
        lastFieldPosition = curFieldPosition;
        lastFieldId = fieldId;
    }
    return Status::OK();
}

Status NormalSchemaResolver::CheckTruncateSortParams(
    const std::vector<indexlibv2::config::TruncateProfileConfig>& truncateProfileConfigs,
    const config::TabletSchema& schema) const
{
    for (const auto& truncateProfileConfig : truncateProfileConfigs) {
        try {
            truncateProfileConfig.Check();
        } catch (const std::exception& e) {
            AUTIL_LOG(ERROR, "check truncateProfileConfig [%s] failed exception [%s].",
                      truncateProfileConfig.GetTruncateProfileName().c_str(), e.what());
            return Status::ConfigError("check truncate profile config failed");
        }
        auto sortParams = truncateProfileConfig.GetTruncateSortParams();
        for (const auto& sortParam : sortParams) {
            std::string sortFieldName = sortParam.GetSortField();
            if (DOC_PAYLOAD_FIELD_NAME == sortFieldName) {
                continue;
            }
            auto fieldConfig = schema.GetFieldConfig(sortFieldName);
            if (!fieldConfig) {
                return Status::ConfigError("truncate sort field [%s] is not in schema", sortFieldName.c_str());
            }
            auto attributeConfig = schema.GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, sortFieldName);
            if (!attributeConfig) {
                return Status::ConfigError("truncate sort field [%s] has no attribute", sortFieldName.c_str());
            }

            // TODO (zhuanghaolin.zhl) remove when support pack atrribute
            // if (attrConfig->GetPackAttributeConfig() != NULL) {
            //     INDEXLIB_FATAL_ERROR(Schema,
            //                          "truncate sort field [%s] "
            //                          "should not be in pack attribute.",
            //                          fieldConfig->GetFieldName().c_str());
            // }
        }
    }
    return Status::OK();
}

// Check the case that if payload name is used in any of the sort params that uses DOC_PAYLOAD, all other sort params
// that use DOC_PAYLOAD should also specify payload name. It's also valid that non of the sort params that uses
// DOC_PAYLOAD specifies payload name. This is to be backward compatible.
Status NormalSchemaResolver::CheckTruncateProfileSchema(
    const std::vector<indexlibv2::config::TruncateProfileConfig>& truncateProfileConfigs) const
{
    bool payloadNameSpecified = false;
    std::vector<std::string> unspecifiedFieldProfiles;
    for (const auto& truncateProfileConfig : truncateProfileConfigs) {
        auto sortParams = truncateProfileConfig.GetTruncateSortParams();
        for (const auto& sortParam : sortParams) {
            if (DOC_PAYLOAD_FIELD_NAME == sortParam.GetSortField()) {
                if (truncateProfileConfig.GetPayloadConfig().IsInitialized()) {
                    payloadNameSpecified = true;
                } else {
                    unspecifiedFieldProfiles.emplace_back(truncateProfileConfig.GetTruncateProfileName());
                }
            }
        }
    }
    if (payloadNameSpecified && !unspecifiedFieldProfiles.empty()) {
        std::stringstream ss;
        ss << "payload name specified, all truncate profiles should specify payload name, but ";
        for (const auto& profile : unspecifiedFieldProfiles) {
            ss << "[" << profile << "] ";
        }
        ss << "not specify payload name";
        return Status::ConfigError("%s", ss.str().c_str());
    }
    return Status::OK();
}

Status NormalSchemaResolver::CheckSummaryConfig(const config::TabletSchema& schema) const
{
    for (const auto& summaryConfig : schema.GetIndexConfigs(index::SUMMARY_INDEX_TYPE_STR)) {
        try {
            summaryConfig->Check();
        } catch (const std::exception& e) {
            AUTIL_LOG(ERROR, "check summaryConfig [%s] failed exception [%s].", summaryConfig->GetIndexName().c_str(),
                      e.what());
            return Status::ConfigError("check summaryConfig failed");
        }
    }
    return Status::OK();
}

Status NormalSchemaResolver::CheckAttributeConfig(const config::TabletSchema& schema) const
{
    auto attributeConfigs = schema.GetIndexConfigs(index::ATTRIBUTE_INDEX_TYPE_STR);
    for (const auto& attributeConfig : attributeConfigs) {
        std::string indexName = attributeConfig->GetIndexName();
        try {
            attributeConfig->Check();
        } catch (const std::exception& e) {
            AUTIL_LOG(ERROR, "check attributeConfig [%s] failed exception [%s].", indexName.c_str(), e.what());
            return Status::ConfigError("check attributeConfig failed");
        }
        // TODO (zhuanghaolin.zhl) remove when support pack attribute
        // if (config->SupportNull() && config->GetPackAttributeConfig() != NULL) {
        //     INDEXLIB_FATAL_ERROR(Schema, "attribute [%s] in pack attribute [%s] not support enable null.",
        //             config->GetAttrName().c_str(), config->GetPackAttributeConfig()->GetPackName().c_str());
        // }

        // FieldType ft = config->GetFieldType();
        // if ((ft == ft_date || ft == ft_time || ft == ft_timestamp) && config->GetPackAttributeConfig() != NULL) {
        //     INDEXLIB_FATAL_ERROR(Schema, "attribute [%s] with field type [%s] not support in pack attribute [%s].",
        //             config->GetAttrName().c_str(), FieldConfig::FieldTypeToStr(ft),
        //             config->GetPackAttributeConfig()->GetPackName().c_str());
        // }
    }
    return Status::OK();
}

Status NormalSchemaResolver::Check(const config::TabletSchema& schema) const
{
    RETURN_IF_STATUS_ERROR(CheckStatisticsTermConfig(schema), "check statistics term config failed.");
    auto fieldConfigs = schema.GetFieldConfigs();
    if (fieldConfigs.empty()) {
        return Status::ConfigError("field config is empty");
    }

    auto [truncateStatus, truncateProfileConfigs] =
        schema.GetRuntimeSettings().GetValue<std::vector<indexlibv2::config::TruncateProfileConfig>>(
            "truncate_profiles");
    if (truncateStatus.IsOK()) {
        RETURN_IF_STATUS_ERROR(CheckTruncateSortParams(truncateProfileConfigs, schema),
                               "check truncate sort param failed");
        RETURN_IF_STATUS_ERROR(CheckTruncateProfileSchema(truncateProfileConfigs), "check truncate profile failed");
    }
    if (!truncateStatus.IsNotFound() && !truncateStatus.IsOK()) {
        return truncateStatus;
    }

    RETURN_IF_STATUS_ERROR(CheckInvertedIndexConfig(schema), "check inverted index config failed.");
    RETURN_IF_STATUS_ERROR(CheckAttributeConfig(schema), "check attribute config failed");
    RETURN_IF_STATUS_ERROR(CheckSummaryConfig(schema), "check summary config failed");
    return Status::OK();
}

Status NormalSchemaResolver::LegacySchemaToTabletSchema(const indexlib::config::IndexPartitionSchema& legacySchema,
                                                        config::UnresolvedSchema* schema)
{
    RETURN_IF_STATUS_ERROR(FillFileCompressToSettings(legacySchema, schema),
                           "fill file compress config to settings failed");
    RETURN_IF_STATUS_ERROR(FillIndexConfigs(legacySchema, schema),
                           "fill index configs from legacy schema to tablet schema failed");
    FillTTLToSettings(legacySchema, schema);
    RETURN_IF_STATUS_ERROR(FillTruncateProfile(legacySchema, schema), "fill truncate profile failed");
    return Status::OK();
}

Status NormalSchemaResolver::ResolveLegacySchema(const std::string& indexPath,
                                                 indexlib::config::IndexPartitionSchema* schema)
{
    RETURN_IF_STATUS_ERROR(LoadAdaptiveVocabulary(schema, indexPath), "load adaptive vocabulary failed");
    return Status::OK();
}

Status NormalSchemaResolver::ResolveTabletSchema(const std::string& indexPath, config::UnresolvedSchema* schema)
{
    RETURN_IF_STATUS_ERROR(ResolveRuntimeSettings(indexPath, schema), "resolve runtime settings failed");
    RETURN_IF_STATUS_ERROR(AddBuiltinIndex(schema), "add built index failed");
    RETURN_IF_STATUS_ERROR(ResolveInvertedIndex(schema), "resolve inverted index failed");
    RETURN_IF_STATUS_ERROR(ResolveSummary(schema, _options), "resolve summary failed");
    RETURN_IF_STATUS_ERROR(ResolveAttribute(schema, _options), "resolve attribute failed");
    return Status::OK();
}

Status NormalSchemaResolver::ResolveRuntimeSettings(const std::string& indexPath, config::UnresolvedSchema* schema)
{
    RETURN_IF_STATUS_ERROR(LoadAdaptiveVocabulary(schema, indexPath), "load adaptive vocabulary failed");
    RETURN_IF_STATUS_ERROR(ResolveTTLField(schema), "resolve ttl field failed");
    RETURN_IF_STATUS_ERROR(ResolveSortDescriptions(schema, _options, indexPath), "resolve sort descs failed");
    RETURN_IF_STATUS_ERROR(ResolveTruncateIndexConfigs(indexPath, schema), "resolve truncate index configs failed");
    return Status::OK();
}

Status NormalSchemaResolver::ResolveInvertedIndex(config::UnresolvedSchema* schema)
{
    RETURN_IF_STATUS_ERROR(ResolveSpatialIndex(schema), "resolve spatial index failed");
    RETURN_IF_STATUS_ERROR(ResolvePKIndexConfig(schema, _options), "resolve pk index config failed");
    RETURN_IF_STATUS_ERROR(ResolveInvertedIndexId(schema), "resolve inverted index id failed");
    RETURN_IF_STATUS_ERROR(ResolveGeneralInvertedIndex(schema, _options), "resolve inverted index failed");
    return Status::OK();
}

Status NormalSchemaResolver::ResolveAttribute(config::UnresolvedSchema* schema, config::TabletOptions* options)
{
    RETURN_IF_STATUS_ERROR(ResolveGeneralValue(schema, options), "resolve generalized value failed");
    return Status::OK();
}

Status NormalSchemaResolver::ResolveInvertedIndexId(config::UnresolvedSchema* schema)
{
    const auto& invertedConfigs = schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR);
    int indexId = invertedConfigs.size();
    const auto& pkIndexConfig =
        std::dynamic_pointer_cast<index::PrimaryKeyIndexConfig>(schema->GetPrimaryKeyIndexConfig());
    if (pkIndexConfig) {
        if (pkIndexConfig->GetIndexId() == INVALID_INDEXID) {
            pkIndexConfig->SetIndexId(indexId);
        }
        indexId++;
    }

    const auto& configs = schema->GetIndexConfigs(indexlibv2::index::ANN_INDEX_TYPE_STR);
    for (const auto& config : configs) {
        const auto& annConfig = std::dynamic_pointer_cast<indexlibv2::config::ANNIndexConfig>(config);
        if (annConfig) {
            annConfig->SetIndexId(indexId++);
        }
    }
    return Status::OK();
}

Status NormalSchemaResolver::ResolvePKIndexConfig(config::UnresolvedSchema* schema, config::TabletOptions* option)
{
    const auto& configs = schema->GetIndexConfigs(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR);
    if (configs.empty()) {
        return Status::OK();
    }
    assert(configs.size() == 1);
    const auto& pkIndexConfig = std::dynamic_pointer_cast<index::PrimaryKeyIndexConfig>(configs[0]);
    assert(pkIndexConfig);

    autil::ScopeGuard done([&schema, &pkIndexConfig]() { schema->SetPrimaryKeyIndexConfig(pkIndexConfig); });

    if (!option) {
        AUTIL_LOG(WARN, "option is NULL, not resolve pk index config");
        return Status::OK();
    }
    std::string speedUpPKReaderPath = "online_index_config.build_config.speedup_primary_key_reader";
    bool speedUpPrimaryKeyReader = false;
    std::string speedUpPKParamPath = "online_index_config.build_config.speedup_primary_key_param";
    std::string speedUpPKParam = "";
    if (auto status = option->GetFromRawJson(speedUpPKReaderPath, &speedUpPrimaryKeyReader); !status.IsOKOrNotFound()) {
        AUTIL_LOG(WARN, "get config [%s] from option failed, status[%s]", speedUpPKReaderPath.c_str(),
                  status.ToString().c_str());
    }
    if (speedUpPrimaryKeyReader) {
        if (auto status = option->GetFromRawJson(speedUpPKParamPath, &speedUpPKParam); !status.IsOKOrNotFound()) {
            AUTIL_LOG(WARN, "get config [%s] from option failed, status[%s]", speedUpPKParamPath.c_str(),
                      status.ToString().c_str());
        }
    }
    indexlib::config::PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode;
    if (speedUpPrimaryKeyReader || pkIndexConfig->GetPrimaryKeyIndexType() == pk_hash_table) {
        loadMode = indexlib::config::PrimaryKeyLoadStrategyParam::HASH_TABLE;
    } else if (pkIndexConfig->GetPrimaryKeyIndexType() == pk_block_array) {
        loadMode = indexlib::config::PrimaryKeyLoadStrategyParam::BLOCK_VECTOR;
    } else {
        loadMode = indexlib::config::PrimaryKeyLoadStrategyParam::SORTED_VECTOR;
    }

    auto status = pkIndexConfig->SetPrimaryKeyLoadParam(loadMode, speedUpPKParam);
    RETURN_IF_STATUS_ERROR(status, "set pk load param failed for param [%s]", speedUpPKParam.c_str());

    std::string pkParallelLookupPath = "online_index_config.build_config.enable_build_parallel_lookup_pk";
    bool pkParallelLookup = false;

    if (auto status = option->GetFromRawJson(pkParallelLookupPath, &pkParallelLookup); !status.IsOKOrNotFound()) {
        AUTIL_LOG(WARN, "get config from tablet options failed, status[%s]", status.ToString().c_str());
    }
    if (auto status = option->GetFromRawJson(pkParallelLookupPath, &pkParallelLookup); !status.IsOKOrNotFound()) {
        AUTIL_LOG(WARN, "get config from tablet options failed, status[%s]", status.ToString().c_str());
    }
    if (pkParallelLookup) {
        pkIndexConfig->EnableParallelLookupOnBuild();
    }

    std::string enableBloomFilterPath = "online_index_config.build_config.enable_bloom_filter_for_primary_key_reader";
    std::string bloomFilterMultipleNumPath = "online_index_config.build_config.bloom_filter_multiple_num";
    bool enableBloomFilter = false;
    if (auto status = option->GetFromRawJson(enableBloomFilterPath, &enableBloomFilter); !status.IsOKOrNotFound()) {
        AUTIL_LOG(WARN, "get configfrom option failed, status[%s]", status.ToString().c_str());
    }
    if (enableBloomFilter) {
        uint32_t bloomFilterMultipleNum = index::PrimaryKeyIndexConfig::DEFAULT_BLOOM_FILTER_MULTIPLE_NUM;
        if (auto status = option->GetFromRawJson(bloomFilterMultipleNumPath, &bloomFilterMultipleNum);
            !status.IsOKOrNotFound()) {
            AUTIL_LOG(WARN, "get config from tablet options failed, status[%s]", status.ToString().c_str());
        } else {
            if (bloomFilterMultipleNum <= 1 || bloomFilterMultipleNum > 16) {
                AUTIL_LOG(WARN, "invalid bloom_filter_multiple_num [%u], use default [%u].", bloomFilterMultipleNum,
                          index::PrimaryKeyIndexConfig::DEFAULT_BLOOM_FILTER_MULTIPLE_NUM);
                bloomFilterMultipleNum = index::PrimaryKeyIndexConfig::DEFAULT_BLOOM_FILTER_MULTIPLE_NUM;
            }
        }
        if (loadMode != indexlib::config::PrimaryKeyLoadStrategyParam::HASH_TABLE) {
            pkIndexConfig->EnableBloomFilterForPkReader(bloomFilterMultipleNum);
        } else {
            AUTIL_LOG(WARN, "hash table type primary key will not enable bloom filter.");
        }
    }
    return Status::OK();
}

Status NormalSchemaResolver::FillFileCompressToSettings(const indexlib::config::IndexPartitionSchema& legacySchema,
                                                        config::UnresolvedSchema* schema)
{
    const auto& fileCompressSchema = legacySchema.GetFileCompressSchema();
    if (!fileCompressSchema) {
        return Status::OK();
    }
    auto [status, _] =
        schema->GetRuntimeSettings().GetValue<std::shared_ptr<std::vector<config::SingleFileCompressConfig>>>(
            config::SingleFileCompressConfig::FILE_COMPRESS_CONFIG_KEY);
    if (status.IsOK()) {
        return Status::OK();
    }
    if (!status.IsNotFound()) {
        RETURN_IF_STATUS_ERROR(status, "%s", status.ToString().c_str());
    }
    std::vector<std::shared_ptr<config::SingleFileCompressConfig>> configs;
    const auto& legacyConfigs = fileCompressSchema->GetFileCompressConfigs();
    if (legacyConfigs.empty()) {
        return Status::OK();
    }
    for (const auto& legacyConfig : legacyConfigs) {
        std::shared_ptr<config::SingleFileCompressConfig> config = legacyConfig->CreateSingleFileCompressConfig();
        if (!config) {
            RETURN_IF_STATUS_ERROR(Status::InternalError(),
                                   "convert legacy file compress config to v2 single file compress config failed, [%s]",
                                   legacyConfig->GetCompressName().c_str());
        }
        configs.push_back(std::move(config));
    }
    bool ret = schema->SetRuntimeSetting(config::SingleFileCompressConfig::FILE_COMPRESS_CONFIG_KEY, configs, false);
    if (!ret) {
        RETURN_IF_STATUS_ERROR(Status::InternalError(), "schema set setting [%s] failed",
                               config::SingleFileCompressConfig::FILE_COMPRESS_CONFIG_KEY.c_str());
    }
    return Status::OK();
}

Status NormalSchemaResolver::ResolveSummary(config::UnresolvedSchema* schema, config::TabletOptions* options)
{
    const auto& config =
        schema->GetIndexConfig(indexlib::index::SUMMARY_INDEX_TYPE_STR, indexlib::index::SUMMARY_INDEX_NAME);
    if (!config) {
        return Status::OK();
    }
    const auto& summaryConfig = std::dynamic_pointer_cast<config::SummaryIndexConfig>(config);
    assert(summaryConfig);
    // timestamp in summary
    const auto& fieldConfigs = summaryConfig->GetFieldConfigs();
    for (const auto& fieldConfig : fieldConfigs) {
        if (fieldConfig->GetFieldType() != ft_timestamp) {
            continue;
        }
        const auto& fieldName = fieldConfig->GetFieldName();
        if (schema->GetIndexConfig(indexlib::index::ATTRIBUTE_INDEX_TYPE_STR, fieldName)) {
            continue;
        }
        AUTIL_LOG(INFO, "inner add timestamp type attribute [%s] to support storage in summary", fieldName.c_str());
        auto attributeConfig = std::make_shared<index::AttributeConfig>();
        RETURN_IF_STATUS_ERROR(attributeConfig->Init(fieldConfig), "attribute config init failed, field [%s]",
                               fieldName.c_str());
        attributeConfig->SetAttrId(schema->GetIndexConfigs(attributeConfig->GetIndexType()).size());
        RETURN_IF_STATUS_ERROR(schema->AddIndexConfig(attributeConfig), "add summary accompany attribute [%s] failed",
                               fieldName.c_str());
    }
    // disableSummarys
    std::string disableSummarys;
    if (auto status = options->GetFromRawJson("online_index_config.disable_fields.summarys", &disableSummarys);
        !status.IsOKOrNotFound()) {
        AUTIL_LOG(ERROR, "get online_index_config.disable_fields.summarys failed");
        return status;
    }
    if (disableSummarys == "__SUMMARY_FIELD_ALL__") {
        summaryConfig->DisableAllFields();
        return Status::OK();
    }
    // SetNeedStoreSummary
    auto attrFields = schema->GetIndexFieldConfigs(indexlib::index::ATTRIBUTE_INDEX_TYPE_STR);
    const auto& packAttrFields = schema->GetIndexFieldConfigs(indexlib::index::PACK_ATTRIBUTE_INDEX_TYPE_STR);
    attrFields.insert(attrFields.end(), packAttrFields.begin(), packAttrFields.end());
    if (attrFields.empty()) {
        summaryConfig->SetNeedStoreSummary(true);
        return Status::OK();
    }
    std::set<fieldid_t> attrFieldIds;
    for (const auto& attrField : attrFields) {
        attrFieldIds.insert(attrField->GetFieldId());
    }
    summaryConfig->SetNeedStoreSummary(false);
    const auto& summaryFields = summaryConfig->GetFieldConfigs();
    for (const auto& summaryField : summaryFields) {
        auto fieldId = summaryField->GetFieldId();
        if (attrFieldIds.find(fieldId) == attrFieldIds.end()) {
            summaryConfig->SetNeedStoreSummary(fieldId);
        }
    }
    return Status::OK();
}

void NormalSchemaResolver::FillTTLToSettings(const indexlib::config::IndexPartitionSchema& legacySchema,
                                             config::UnresolvedSchema* schema)
{
    if (legacySchema.TTLEnabled()) {
        auto [status, ttlEnable] = schema->GetRuntimeSettings().GetValue<bool>("enable_ttl");
        auto [status1, fieldName] = schema->GetRuntimeSettings().GetValue<std::string>("ttl_field_name");
        auto [status2, defaultTTL] = schema->GetRuntimeSettings().GetValue<int64_t>("default_ttl");
        if (status.IsOK() || status1.IsOK() || status2.IsOK()) {
            // tablet schema setting has ttl config, not use legacy schema
            return;
        }

        bool ret = schema->SetRuntimeSetting("enable_ttl", true, false);
        ret = schema->SetRuntimeSetting("ttl_field_name", legacySchema.GetTTLFieldName(), false) && ret;
        ret = schema->SetRuntimeSetting("default_ttl", legacySchema.GetDefaultTTL(), false) && ret;
        assert(ret);
    }
}

Status NormalSchemaResolver::ResolveSpatialIndex(config::UnresolvedSchema* schema)
{
    for (auto indexConfig : schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR)) {
        if (auto spatialIndexConfig = std::dynamic_pointer_cast<config::SpatialIndexConfig>(indexConfig)) {
            // if (spatialIndexConfig->IsDeleted()) {
            //     continue;
            // }
            auto fieldConfig = spatialIndexConfig->GetFieldConfig();
            if (fieldConfig->GetFieldType() != FieldType::ft_location) {
                continue;
            }
            auto fieldName = fieldConfig->GetFieldName();
            auto attributeConfig = std::dynamic_pointer_cast<index::AttributeConfig>(
                schema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, fieldName));
            if (!attributeConfig) {
                AUTIL_LOG(INFO, "add attribute [%s] to ensure spatial index precision", fieldName.c_str());
                auto attributeConfig = std::make_shared<index::AttributeConfig>();
                RETURN_IF_STATUS_ERROR(attributeConfig->Init(fieldConfig), "attribute [%s] config init failed",
                                       fieldName.c_str());
                attributeConfig->SetFileCompressConfigV2(spatialIndexConfig->GetFileCompressConfigV2());
                attributeConfig->SetAttrId(schema->GetIndexConfigs(attributeConfig->GetIndexType()).size());
                RETURN_IF_STATUS_ERROR(schema->AddIndexConfig(attributeConfig), "add spatial attribute [%s] failed",
                                       fieldName.c_str());
            }
        }
    }
    return Status::OK();
}

Status NormalSchemaResolver::ResolveTTLField(config::UnresolvedSchema* schema)
{
    auto [status, enableTTL] = schema->GetRuntimeSettings().GetValue<bool>("enable_ttl");
    auto ttlAttrConfig = std::make_shared<index::AttributeConfig>();
    if (status.IsOK()) {
        if (!enableTTL) {
            return Status::OK();
        }
        // ttl field resolve
        auto [status, ttlFieldName] = schema->GetRuntimeSettings().GetValue<std::string>("ttl_field_name");
        if (status.IsOK()) {
            auto attributeConfig = std::dynamic_pointer_cast<index::AttributeConfig>(
                schema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, ttlFieldName));
            if (!attributeConfig) {
                RETURN_STATUS_ERROR(ConfigError, "ttl field name [%s] is not found in attributes",
                                    ttlFieldName.c_str());
            }
            if (attributeConfig->GetFieldType() != FieldType::ft_uint32 || attributeConfig->IsMultiValue()) {
                RETURN_STATUS_ERROR(ConfigError,
                                    "ttl field name [%s] in attribute is invalid, should be ft_uint32 and single value",
                                    ttlFieldName.c_str());
            }
            ttlAttrConfig = attributeConfig;
        } else {
            if (status.IsNotFound()) {
                auto fieldConfig = schema->GetFieldConfig(DOC_TIME_TO_LIVE_IN_SECONDS);
                if (fieldConfig) {
                    RETURN_STATUS_ERROR(ConfigError, "ttl field [%s] is in field config but not in attribute config",
                                        DOC_TIME_TO_LIVE_IN_SECONDS.c_str());
                }
                fieldConfig = std::make_shared<config::FieldConfig>(DOC_TIME_TO_LIVE_IN_SECONDS, ft_uint32, false);
                fieldConfig->SetVirtual(true);

                RETURN_IF_STATUS_ERROR(schema->AddFieldConfig(fieldConfig), "add field config [%s] failed",
                                       DOC_TIME_TO_LIVE_IN_SECONDS.c_str());

                if (!schema->SetRuntimeSetting("ttl_field_name", DOC_TIME_TO_LIVE_IN_SECONDS, false)) {
                    RETURN_STATUS_DIRECTLY_IF_ERROR(Status::InternalError(""));
                }
                auto attributeConfig = std::make_shared<index::AttributeConfig>();
                RETURN_IF_STATUS_ERROR(attributeConfig->Init(fieldConfig), "attribute config init failed");
                attributeConfig->SetAttrId(schema->GetIndexConfigs(attributeConfig->GetIndexType()).size());
                RETURN_IF_STATUS_ERROR(schema->AddIndexConfig(attributeConfig), "add attribute config [%s] failed",
                                       DOC_TIME_TO_LIVE_IN_SECONDS.c_str());
                ttlAttrConfig = attributeConfig;
            } else {
                RETURN_IF_STATUS_ERROR(status, "get invaild ttl field name");
            }
        }

        AUTIL_LOG(INFO, "disable update for field [%s] due to ttl field", ttlFieldName.c_str());
        ttlAttrConfig->SetUpdatable(false);
        // default ttl
        auto [status1, defaultTTL] = schema->GetRuntimeSettings().GetValue<int64_t>("default_ttl");
        if (status1.IsNotFound()) {
            bool ret = schema->SetRuntimeSetting("default_ttl", DEFAULT_TIME_TO_LIVE, false);
            if (!ret) {
                RETURN_STATUS_DIRECTLY_IF_ERROR(Status::InternalError(""));
            }
        } else {
            if (status1.IsOK() && defaultTTL < 0) {
                RETURN_STATUS_ERROR(ConfigError, "default ttl [%d] is invalid, should not be negative", defaultTTL);
            }
            RETURN_IF_STATUS_ERROR(status1, "default ttl set is invalid [%ld]", defaultTTL);
        }
    } else if (!status.IsNotFound()) {
        return Status::ConfigError("get enable_ttl failed");
    }
    return Status::OK();
}

Status NormalSchemaResolver::ResolveSortDescriptions(config::UnresolvedSchema* schema,
                                                     const config::TabletOptions* options, const std::string& indexPath)
{
    auto [isExist, _] = schema->GetRuntimeSetting("sort_descriptions");
    // legacy read old partitionMeta to set sortDesc to schema
    // options maybe nullptr when read a resolved schema in index path (offline case)
    // new sort description is store to schema
    if (!isExist) {
        indexlib::legacy::index_base::PartitionMeta partMeta;
        try {
            if (indexlib::legacy::index_base::PartitionMeta::IsExist(indexPath)) {
                partMeta.Load(indexPath);
                if (partMeta.Size() > 0) {
                    schema->SetRuntimeSetting("sort_descriptions",
                                              autil::legacy::ToJson(partMeta.GetSortDescriptions()),
                                              /*overwrite*/ false);
                    AUTIL_LOG(INFO, "set schema sort_descriptions [%s] from partionMeta",
                              autil::legacy::ToJsonString(partMeta.GetSortDescriptions(), true).c_str());
                }
            } else if (options && options->GetBuildOptionConfig().IsSortBuild()) {
                const auto& sortDescriptions = options->GetBuildOptionConfig().GetSortDescriptions();
                if (sortDescriptions.empty()) {
                    RETURN_STATUS_ERROR(ConfigError, "sort build desc cannot be empty when sort_build set true");
                }
                schema->SetRuntimeSetting("sort_descriptions", autil::legacy::ToJson(sortDescriptions),
                                          /*overwrite*/ false);
                AUTIL_LOG(INFO, "set schema sort_descriptions [%s] from build option config",
                          autil::legacy::ToJsonString(sortDescriptions, true).c_str());
            }
        } catch (const autil::legacy::ExceptionBase& e) {
            AUTIL_LOG(ERROR, "resolve schema failed with exception [%s]", e.what());
            return Status::IOError(e.what());
        }
    }
    auto [status, sortDescs] = schema->GetRuntimeSettings().GetValue<config::SortDescriptions>("sort_descriptions");
    if (status.IsNotFound()) {
        return Status::OK();
    }
    RETURN_STATUS_DIRECTLY_IF_ERROR(status);
    if (!sortDescs.empty()) {
        DisableTFBitmap(schema);
    }
    return CheckSortDescs(schema);
}

void NormalSchemaResolver::DisableTFBitmap(config::UnresolvedSchema* schema)
{
    auto indexConfigs = schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR);
    for (auto indexConfig : indexConfigs) {
        auto invertedConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(indexConfig);
        assert(invertedConfig);
        auto optionFlag = invertedConfig->GetOptionFlag();
        if (optionFlag & of_tf_bitmap) {
            AUTIL_LOG(WARN, "tf bitmap disabled, not support sort dump");
            optionFlag &= (~of_tf_bitmap);
            assert(!(optionFlag & of_tf_bitmap));
            invertedConfig->SetOptionFlag(optionFlag);
        }
    }
}

Status NormalSchemaResolver::CheckSortDescs(const config::UnresolvedSchema* schema)
{
    auto [status, sortDescs] = schema->GetRuntimeSettings().GetValue<config::SortDescriptions>("sort_descriptions");
    if (status.IsNotFound()) {
        return Status::OK();
    }
    RETURN_STATUS_DIRECTLY_IF_ERROR(status);

    if (sortDescs.empty()) {
        return Status::OK();
    }

    for (const auto& desc : sortDescs) {
        const std::string& fieldName = desc.GetSortFieldName();
        auto indexConfig = schema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, fieldName);
        if (!indexConfig) {
            RETURN_STATUS_ERROR(ConfigError, "field [%s] is not attribute field, cannot be sort field",
                                fieldName.c_str());
        }
        auto attributeConfig = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(indexConfig);
        if (!attributeConfig) {
            RETURN_STATUS_ERROR(ConfigError, "field [%s] is not attribute field, cannot be sort field",
                                fieldName.c_str());
        }

        auto fieldConfig = attributeConfig->GetFieldConfig();
        if (!fieldConfig || !fieldConfig->SupportSort()) {
            RETURN_STATUS_ERROR(ConfigError, "field [%s] is not illegal sort field", fieldName.c_str());
        }

        indexlib::config::CompressTypeOption compressType = attributeConfig->GetCompressType();
        if (compressType.HasFp16EncodeCompress() || compressType.HasInt8EncodeCompress()) {
            RETURN_STATUS_ERROR(ConfigError, "sort field [%s] not support fp16 or int8 compress", fieldName.c_str());
        }
        AUTIL_LOG(INFO, "disable update for field [%s] due to sort field", fieldName.c_str());
        attributeConfig->SetUpdatable(false);
    }
    return Status::OK();
}

Status NormalSchemaResolver::LoadAdaptiveVocabulary(indexlib::config::IndexPartitionSchema* legacySchema,
                                                    const std::string& indexPath)
{
    if (indexPath.empty()) {
        return Status::OK();
    }
    indexlib::file_system::FileSystemOptions options;
    auto [st, fileSystem] =
        indexlib::file_system::FileSystemCreator::CreateForRead("adaptive-bitmap-tmp", indexPath, options).StatusWith();
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "load adaptive vocabulary failed to create fs [%s]", st.ToString().c_str());
        return st;
    }
    auto rootDir = indexlib::file_system::Directory::Get(fileSystem);
    try {
        indexlib::legacy::index_base::SchemaAdapter::LoadAdaptiveBitmapTerms(rootDir, legacySchema);
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "resolve schema failed. load adaptive vocabulary with exception [%s]", e.what());
        return Status::IOError(e.what());
    }
    return Status::OK();
}

Status NormalSchemaResolver::LoadAdaptiveVocabulary(config::UnresolvedSchema* schema, const std::string& indexPath)
{
    if (indexPath.empty()) {
        return Status::OK();
    }
    indexlib::file_system::FileSystemOptions options;
    auto [st, fileSystem] =
        indexlib::file_system::FileSystemCreator::CreateForRead("adaptive-bitmap-tmp", indexPath, options).StatusWith();
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "load adaptive vocabulary failed to create fs [%s]", st.ToString().c_str());
        return st;
    }
    auto rootDir = indexlib::file_system::Directory::Get(fileSystem)->GetIDirectory();

    auto physicalRootDir = indexlib::file_system::IDirectory::GetPhysicalDirectory(indexPath);
    auto fsResult = physicalRootDir->GetDirectory(ADAPTIVE_DICT_DIR_NAME);
    if (fsResult.Code() != indexlib::file_system::ErrorCode::FSEC_OK) {
        if (fsResult.Code() == indexlib::file_system::ErrorCode::FSEC_NOENT) {
            return Status::OK();
        }
        AUTIL_LOG(ERROR, "get directory [%s] failed, [%s]", ADAPTIVE_DICT_DIR_NAME,
                  indexlib::file_system::toStatus(fsResult.Code()).ToString().c_str());
        return indexlib::file_system::toStatus(fsResult.Code());
    }
    RETURN_IF_STATUS_ERROR(rootDir->GetFileSystem()
                               ->MountDir(/*physicalRoot=*/rootDir->GetPhysicalPath(""),
                                          /*physicalPath=*/ADAPTIVE_DICT_DIR_NAME,
                                          /*logicalPath=*/ADAPTIVE_DICT_DIR_NAME,
                                          /*MountDirOption=*/indexlib::file_system::FSMT_READ_WRITE,
                                          /*enableLazyMount=*/false)
                               .Status(),
                           "mount adaptive bitmap meta dir failed");
    return LoadAdaptiveBitmapTerms(rootDir, schema);
}

Status NormalSchemaResolver::AddBuiltinIndex(config::UnresolvedSchema* schema)
{
    const std::string& tableType = schema->GetTableType();
    if (tableType != indexlib::table::TABLE_TYPE_NORMAL) {
        AUTIL_LOG(INFO, "no need add builtin index for table type [%s]", tableType.c_str());
        return Status::OK();
    }
    auto pkConfigs = schema->GetIndexConfigs(index::PRIMARY_KEY_INDEX_TYPE_STR);
    if (pkConfigs.size() > 0) {
        assert(1 == pkConfigs.size());
        auto pkConfig = std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(pkConfigs[0]);
        assert(pkConfig);
        auto deletionMapConfig = std::dynamic_pointer_cast<indexlibv2::index::DeletionMapConfig>(
            schema->GetIndexConfig(index::DELETION_MAP_INDEX_TYPE_STR, index::DELETION_MAP_INDEX_NAME));
        if (!deletionMapConfig) {
            deletionMapConfig = std::make_shared<index::DeletionMapConfig>();
            RETURN_IF_STATUS_ERROR(schema->AddIndexConfig(deletionMapConfig), "add deletion map config failed");
        }

        if (!schema->GetIndexConfig(indexlib::index::OPERATION_LOG_INDEX_TYPE_STR,
                                    indexlib::index::OPERATION_LOG_INDEX_NAME)) {
            auto operationLogConfig = std::make_shared<indexlib::index::OperationLogConfig>(
                indexlib::index::DEFAULT_MAX_OPERATION_BLOCK_SIZE, true);
            operationLogConfig->AddIndexConfigs(index::PRIMARY_KEY_INDEX_TYPE_STR, {pkConfig});
            operationLogConfig->AddIndexConfigs(index::ATTRIBUTE_INDEX_TYPE_STR,
                                                schema->GetIndexConfigs(index::ATTRIBUTE_INDEX_TYPE_STR));
            // TODO: add inverted index config to oplogconfig
            const auto& fields = schema->GetFieldConfigs();
            operationLogConfig->SetFieldConfigs(fields);
            RETURN_IF_STATUS_ERROR(schema->AddIndexConfig(operationLogConfig), "add operation log config failed");
        }
    }
    auto addBuiltinAttribute = [schema](const std::string fieldName, FieldType fieldType) -> Status {
        if (schema->GetIndexConfig(VIRTUAL_ATTRIBUTE_INDEX_TYPE_STR, fieldName)) {
            return Status::OK();
        }
        auto fieldConfig = schema->GetFieldConfig(fieldName);
        if (fieldConfig && fieldConfig->GetFieldType() != fieldType) {
            AUTIL_LOG(ERROR, "field [%s] already in schema, but type not as expected", fieldName.c_str());
            return Status::InvalidArgs("field schema invalid");
        }
        if (!fieldConfig) {
            fieldConfig = std::make_shared<config::FieldConfig>(fieldName, fieldType, false);
            fieldConfig->SetVirtual(true);
            RETURN_STATUS_DIRECTLY_IF_ERROR(schema->AddFieldConfig(fieldConfig));
        }

        assert(fieldConfig);
        auto attrConfig = std::make_shared<indexlibv2::index::AttributeConfig>();
        RETURN_IF_STATUS_ERROR(attrConfig->Init(fieldConfig), "attribute config [%s] init failed", fieldName.c_str());
        std::shared_ptr<config::IIndexConfig> newConfig(new VirtualAttributeConfig(attrConfig));

        RETURN_IF_STATUS_ERROR(schema->AddIndexConfig(newConfig), "add builtin attribute [%s] failed",
                               fieldName.c_str());
        return Status::OK();
    };
    RETURN_IF_STATUS_ERROR(addBuiltinAttribute(document::DocumentInfoToAttributeRewriter::VIRTUAL_TIMESTAMP_FIELD_NAME,
                                               document::DocumentInfoToAttributeRewriter::VIRTUAL_TIMESTAMP_FIELD_TYPE),
                           "add virtual timestamp field failed");
    RETURN_IF_STATUS_ERROR(addBuiltinAttribute(document::DocumentInfoToAttributeRewriter::VIRTUAL_HASH_ID_FIELD_NAME,
                                               document::DocumentInfoToAttributeRewriter::VIRTUAL_HASH_ID_FIELD_TYPE),
                           "add virtual hashid field failed");
    RETURN_IF_STATUS_ERROR(
        addBuiltinAttribute(document::DocumentInfoToAttributeRewriter::VIRTUAL_CONCURRENT_IDX_FIELD_NAME,
                            document::DocumentInfoToAttributeRewriter::VIRTUAL_CONCURRENT_IDX_FIELD_TYPE),
        "add virtual concurrent idx field failed");
    return Status::OK();
}

std::pair<Status, std::shared_ptr<config::InvertedIndexConfig>>
NormalSchemaResolver::CreateInvertedIndexConfig(const std::shared_ptr<indexlib::config::IndexConfig>& indexConfig)
{
    auto configV2 = indexConfig->ConstructConfigV2();
    if (!configV2) {
        AUTIL_LOG(WARN, "construct index config[%s] from legacy schema failed", indexConfig->GetIndexName().c_str());
        return {Status::OK(), indexConfig};
    }
    auto invertedIndexConfig = dynamic_cast<config::InvertedIndexConfig*>(configV2.release());
    assert(invertedIndexConfig);
    return {Status::OK(), std::shared_ptr<config::InvertedIndexConfig>(invertedIndexConfig)};
}

Status NormalSchemaResolver::ResolveGeneralValue(config::UnresolvedSchema* schema, config::TabletOptions* options)
{
    std::set<std::string> disableAttributes;
    if (auto status = options->GetFromRawJson("online_index_config.disable_fields.attributes", &disableAttributes);
        !status.IsOKOrNotFound()) {
        AUTIL_LOG(ERROR, "get online_index_config.disable_fields.attributes failed");
        return status;
    }
    attrid_t attrId = 0;
    const auto& attrIndexConfigs = schema->GetIndexConfigs(index::ATTRIBUTE_INDEX_TYPE_STR);
    for (const auto& indexConfig : attrIndexConfigs) {
        const auto& attrConfig = std::dynamic_pointer_cast<index::AttributeConfig>(indexConfig);
        assert(attrConfig);
        assert(attrConfig->GetAttrId() != INVALID_ATTRID);
        if (disableAttributes.count(attrConfig->GetIndexName()) > 0) {
            attrConfig->Disable();
        }
        if (!schema->GetIndexConfig(indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR, attrConfig->GetIndexName())) {
            RETURN_IF_STATUS_ERROR(schema->AddIndexConfig(indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR, attrConfig),
                                   "add attribute [%s] into general_value failed", attrConfig->GetIndexName().c_str());
        }
        attrId = std::max(attrConfig->GetAttrId() + 1, attrId);
    }

    std::set<std::string> disablePackAttributes;
    if (auto status =
            options->GetFromRawJson("online_index_config.disable_fields.pack_attributes", &disablePackAttributes);
        !status.IsOKOrNotFound()) {
        AUTIL_LOG(ERROR, "get online_index_config.disable_fields.pack_attributes failed");
        return status;
    }
    const auto& packAttrIndexConfigs = schema->GetIndexConfigs(index::PACK_ATTRIBUTE_INDEX_TYPE_STR);
    for (const auto& indexConfig : packAttrIndexConfigs) {
        if (schema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, indexConfig->GetIndexName())) {
            auto status = Status::ConfigError("pack attribute name [%s] is duplicate with normal attribute",
                                              indexConfig->GetIndexName().c_str());
            AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }
        const auto& packAttrConfig = std::dynamic_pointer_cast<index::PackAttributeConfig>(indexConfig);
        assert(packAttrConfig);
        if (disablePackAttributes.count(packAttrConfig->GetIndexName()) > 0) {
            packAttrConfig->Disable();
        }
        const auto& attrConfigs = packAttrConfig->GetAttributeConfigVec();
        for (const auto& attrConfig : attrConfigs) {
            if (!schema->GetIndexConfig(indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR, attrConfig->GetIndexName())) {
                attrConfig->SetAttrId(attrId++);
                RETURN_IF_STATUS_ERROR(
                    schema->AddIndexConfig(indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR, attrConfig),
                    "add sub attribute [%s] in pack attribute [%s] into general_value failed",
                    attrConfig->GetIndexName().c_str(), packAttrConfig->GetIndexName().c_str());
            }
        }
    }
    return Status::OK();
}

Status NormalSchemaResolver::ResolveGeneralInvertedIndex(config::UnresolvedSchema* schema,
                                                         config::TabletOptions* options)
{
    std::set<std::string> disableIndexes;
    if (auto status = options->GetFromRawJson("online_index_config.disable_fields.indexes", &disableIndexes);
        !status.IsOKOrNotFound()) {
        AUTIL_LOG(ERROR, "get online_index_config.disable_fields.indexes failed");
        return status;
    }
    auto invertedIndexConfigs = schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR);
    for (const auto& indexConfig : invertedIndexConfigs) {
        if (disableIndexes.count(indexConfig->GetIndexName())) {
            const auto& invertedIndexConfig =
                std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
            assert(invertedIndexConfig);
            invertedIndexConfig->Disable();
        }
        if (!schema->GetIndexConfig(indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR, indexConfig->GetIndexName())) {
            RETURN_IF_STATUS_ERROR(
                schema->AddIndexConfig(indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR, indexConfig),
                "add inverted_index [%s] into  general_inverted_index failed", indexConfig->GetIndexName().c_str());
        }
    }

    auto pkIndexConfig = schema->GetPrimaryKeyIndexConfig();
    if (pkIndexConfig &&
        !schema->GetIndexConfig(indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR, pkIndexConfig->GetIndexName())) {
        RETURN_IF_STATUS_ERROR(schema->AddIndexConfig(indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR, pkIndexConfig),
                               "add primary key into general_inverted_index failed");
    }

    auto annIndexConfigs = schema->GetIndexConfigs(indexlibv2::index::ANN_INDEX_TYPE_STR);
    for (const auto& indexConfig : annIndexConfigs) {
        if (disableIndexes.count(indexConfig->GetIndexName())) {
            const auto& invertedIndexConfig =
                std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
            assert(invertedIndexConfig);
            invertedIndexConfig->Disable();
        }
        if (!schema->GetIndexConfig(indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR, indexConfig->GetIndexName())) {
            RETURN_IF_STATUS_ERROR(
                schema->AddIndexConfig(indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR, indexConfig),
                "add ann [%s] into general_inverted_index failed", indexConfig->GetIndexName().c_str());
        }
    }
    return Status::OK();
}

Status NormalSchemaResolver::LoadAdaptiveBitmapTerms(const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir,
                                                     config::UnresolvedSchema* schema)
{
    std::vector<std::shared_ptr<config::InvertedIndexConfig>> adaptiveDictIndexConfigs;
    const auto& configs = schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR);
    for (const auto& config : configs) {
        const auto& invertedConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(config);
        assert(invertedConfig);
        if (invertedConfig->GetShardingIndexConfigs().size() > 0) {
            // if is sharding index, don't read HFV for original index name
            for (const auto& shardingIndexConfig : invertedConfig->GetShardingIndexConfigs()) {
                if (shardingIndexConfig->GetAdaptiveDictionaryConfig()) {
                    adaptiveDictIndexConfigs.push_back(shardingIndexConfig);
                }
            }
            continue;
        }
        if (invertedConfig->GetAdaptiveDictionaryConfig()) {
            adaptiveDictIndexConfigs.push_back(invertedConfig);
        }
    }
    if (adaptiveDictIndexConfigs.empty()) {
        return Status::OK();
    }

    auto fsResult = rootDir->GetDirectory(ADAPTIVE_DICT_DIR_NAME);
    if (fsResult.Code() == indexlib::file_system::FSEC_NOENT) {
        return Status::OK();
    }
    auto status = indexlib::file_system::toStatus(fsResult.Code());
    RETURN_IF_STATUS_ERROR(status, "get directory [%s] failed, [%s]", ADAPTIVE_DICT_DIR_NAME,
                           status.ToString().c_str());
    auto directory = fsResult.Value();
    if (directory == nullptr) {
        AUTIL_LOG(ERROR, "get directory [%s] failed, directory is nullptr", ADAPTIVE_DICT_DIR_NAME);
        return Status::OK();
    }
    return doLoadAdaptiveBitmap(directory, adaptiveDictIndexConfigs);
}

Status NormalSchemaResolver::doLoadAdaptiveBitmap(
    const std::shared_ptr<indexlib::file_system::IDirectory>& adaptiveBitmapDir,
    const std::vector<std::shared_ptr<config::InvertedIndexConfig>>& adaptiveDictIndexConfigs)
{
    // adaptiveBitmapDir might be in archive file or package format.
    const std::string PACKAGE_META_FILE_NAME = std::string(indexlib::file_system::PACKAGE_FILE_PREFIX) +
                                               std::string(indexlib::file_system::PACKAGE_FILE_META_SUFFIX);
    auto [st, isExist] = adaptiveBitmapDir->IsExist(PACKAGE_META_FILE_NAME).StatusWith();
    if (!st.IsOK()) {
        AUTIL_LOG(INFO, "check package meta file failed, [%s]", st.ToString().c_str());
        return st;
    }
    if (isExist) {
        for (size_t i = 0; i < adaptiveDictIndexConfigs.size(); i++) {
            auto indexConfig = adaptiveDictIndexConfigs[i];
            auto [status, vol] = indexlib::config::HighFreqVocabularyCreator::LoadAdaptiveVocabulary(
                                     adaptiveBitmapDir, indexConfig->GetIndexName(),
                                     indexConfig->GetInvertedIndexType(), indexConfig->GetNullTermLiteralString(),
                                     indexConfig->GetDictConfig(), indexConfig->GetDictHashParams())
                                     .StatusWith();
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "load adaptive vocabulary failed, [%s]", status.ToString().c_str());
                return status;
            }
            if (vol != nullptr) {
                indexConfig->SetHighFreqVocabulary(vol);
            }
        }
        return Status::OK();
    }

    auto adaptiveDictFolder = adaptiveBitmapDir->LEGACY_CreateArchiveFolder(false, "");
    for (size_t i = 0; i < adaptiveDictIndexConfigs.size(); i++) {
        auto indexConfig = adaptiveDictIndexConfigs[i];
        auto [status, vol] =
            indexlib::config::HighFreqVocabularyCreator::LoadAdaptiveVocabulary(
                adaptiveDictFolder, indexConfig->GetIndexName(), indexConfig->GetInvertedIndexType(),
                indexConfig->GetNullTermLiteralString(), indexConfig->GetDictConfig(), indexConfig->GetDictHashParams())
                .StatusWith();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "load adaptive vocabulary failed, [%s]", status.ToString().c_str());
            return status;
        }
        if (vol != nullptr) {
            indexConfig->SetHighFreqVocabulary(vol);
        }
    }
    return adaptiveDictFolder->Close().Status();
}

Status NormalSchemaResolver::FillIndexConfigs(const indexlib::config::IndexPartitionSchema& legacySchema,
                                              config::UnresolvedSchema* schema)
{
    assert(schema);
    auto [status, singleFileCompressConfigVec] =
        schema->GetRuntimeSettings().GetValue<std::shared_ptr<std::vector<config::SingleFileCompressConfig>>>(
            config::SingleFileCompressConfig::FILE_COMPRESS_CONFIG_KEY);
    if (!status.IsOK() && !status.IsNotFound()) {
        RETURN_IF_STATUS_ERROR(status, "get file compress config from setting failed");
    }
    if (singleFileCompressConfigVec) {
        RETURN_IF_STATUS_ERROR(config::SingleFileCompressConfig::ValidateConfigs(*singleFileCompressConfigVec),
                               "file compress invalid");
    }
    if (const auto& indexSchema = legacySchema.GetIndexSchema(); indexSchema != nullptr) {
        // INVERTED_INDEX_TYPE_STR
        auto pkConfig = indexSchema->GetPrimaryKeyIndexConfig();
        for (auto it = indexSchema->Begin(); it != indexSchema->End(); it++) {
            auto indexConfig = *it;
            if (indexConfig == pkConfig) {
                continue;
            }
            auto shardingType = indexConfig->GetShardingType();
            if (indexlibv2::config::InvertedIndexConfig::IndexShardingType::IST_IS_SHARDING == shardingType) {
                continue;
            }
            if (schema->GetIndexConfig(indexlib::index::INVERTED_INDEX_TYPE_STR, indexConfig->GetIndexName()) ||
                schema->GetIndexConfig(indexlibv2::index::ANN_INDEX_TYPE_STR, indexConfig->GetIndexName())) {
                // already added
                // TODO: need to be continue, when support new schema
                continue;
            }
            auto [status, invertedIndexConfig] = CreateInvertedIndexConfig(indexConfig);
            RETURN_IF_STATUS_ERROR(status, "create inverted index config fail");
            assert(invertedIndexConfig);
            RETURN_IF_STATUS_ERROR(ResolveFileCompressConfig(singleFileCompressConfigVec, invertedIndexConfig),
                                   "resolve inverted index compress config failed");
            status = schema->AddIndexConfig(invertedIndexConfig);
            RETURN_IF_STATUS_ERROR(status, "add inverted index config fail, indexName[%s]",
                                   invertedIndexConfig->GetIndexName().c_str());
        }
        // PRIMARY_KEY_INDEX_TYPE_STR
        if (schema->GetIndexConfigs(index::PRIMARY_KEY_INDEX_TYPE_STR).empty()) {
            if (indexSchema->HasPrimaryKeyIndex()) {
                if (!schema->GetIndexConfig(index::PRIMARY_KEY_INDEX_TYPE_STR, pkConfig->GetIndexName())) {
                    // already added

                    auto legacyConfig = std::dynamic_pointer_cast<indexlib::config::IndexConfig>(pkConfig);
                    assert(legacyConfig);
                    auto pkConfigV2 = legacyConfig->ConstructConfigV2();
                    if (!pkConfigV2) {
                        RETURN_IF_STATUS_ERROR(Status::InternalError(), "make primary index config v2 failed");
                    }
                    auto pkConfigShared = std::shared_ptr<indexlibv2::config::IIndexConfig>(std::move(pkConfigV2));
                    auto status = schema->AddIndexConfig(pkConfigShared);
                    RETURN_IF_STATUS_ERROR(status, "add primary_key config fail, indexName[%s]",
                                           pkConfig->GetIndexName().c_str());
                }
            }
        }
    }
    if (const auto& attrSchema = legacySchema.GetAttributeSchema(); attrSchema != nullptr) {
        // ATTRIBUTE_INDEX_TYPE_STR
        for (auto it = attrSchema->Begin(); it != attrSchema->End(); it++) {
            auto attrConfig = *it;
            if (schema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, attrConfig->GetIndexName())) {
                // already added
                continue;
            }
            if (attrConfig->GetPackAttributeConfig()) {
                continue;
            }
            RETURN_IF_STATUS_ERROR(ResolveFileCompressConfig(singleFileCompressConfigVec, attrConfig),
                                   "resolve file compress config fail, indexName[%s]",
                                   attrConfig->GetIndexName().c_str());
            auto status = schema->AddIndexConfig(attrConfig);
            RETURN_IF_STATUS_ERROR(status, "add attribute config fail, indexName[%s]",
                                   attrConfig->GetIndexName().c_str());
        }
        // PACK_ATTRIBUTE_INDEX_TYPE_STR
        for (size_t i = 0; i < attrSchema->GetPackAttributeCount(); ++i) {
            const auto& packAttrConfig = attrSchema->GetPackAttributeConfig(i);
            if (schema->GetIndexConfig(index::PACK_ATTRIBUTE_INDEX_TYPE_STR, packAttrConfig->GetIndexName())) {
                // already added
                continue;
            }
            RETURN_IF_STATUS_ERROR(ResolveFileCompressConfig(singleFileCompressConfigVec, packAttrConfig),
                                   "resolve file compress config fail, indexName[%s]",
                                   packAttrConfig->GetIndexName().c_str());
            auto status = schema->AddIndexConfig(packAttrConfig);
            RETURN_IF_STATUS_ERROR(status, "add pack attribute config fail, indexName[%s]",
                                   packAttrConfig->GetIndexName().c_str());
        }
    }
    if (const auto& summarySchema = legacySchema.GetSummarySchema(); nullptr != summarySchema) {
        // SUMMARY_INDEX_TYPE_STR
        std::shared_ptr<indexlibv2::config::SummaryIndexConfig> summaryIndexConfig(
            summarySchema->MakeSummaryIndexConfigV2().release());
        if (schema->GetIndexConfig(index::SUMMARY_INDEX_TYPE_STR, summaryIndexConfig->GetIndexName())) {
            // already added
            return Status::OK();
        }
        RETURN_IF_STATUS_ERROR(ResolveFileCompressConfig(singleFileCompressConfigVec, summaryIndexConfig),
                               "resolve summary compress config failed");
        auto status = schema->AddIndexConfig(summaryIndexConfig);
        RETURN_IF_STATUS_ERROR(status, "add summary index config fail, indexName[%s]",
                               summaryIndexConfig->GetIndexName().c_str());
    }
    return Status::OK();
}

void NormalSchemaResolver::ResolveEmptyProfileNamesForTruncateIndex(
    const std::vector<indexlibv2::config::TruncateProfileConfig>& profileConfigs,
    const std::vector<std::shared_ptr<config::IIndexConfig>>& indexConfigs)
{
    std::vector<std::string> truncateProfileNames;
    for (const auto& profileConfig : profileConfigs) {
        if (profileConfig.GetPayloadConfig().IsInitialized()) {
            continue;
        }
        truncateProfileNames.push_back(profileConfig.GetTruncateProfileName());
    }
    for (const auto& indexConfig : indexConfigs) {
        auto invertedIndexConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(indexConfig);
        if (!invertedIndexConfig->HasTruncate()) {
            continue;
        }
        if (!invertedIndexConfig->GetUseTruncateProfilesStr().empty()) {
            continue;
        }
        invertedIndexConfig->SetUseTruncateProfiles(truncateProfileNames);
    }
}

Status NormalSchemaResolver::ResolveTruncateIndexConfigs(const std::string& indexPath, config::UnresolvedSchema* schema)
{
    auto [isExist, _] = schema->GetRuntimeSetting("truncate_profiles");
    if (!isExist) {
        AUTIL_LOG(DEBUG, "truncate profile not exist.");
        return Status::OK();
    }

    auto st = LoadTruncateTermVocabulary(indexPath, schema);
    RETURN_IF_STATUS_ERROR(st, "load truncate term vocabulary failed.");

    auto [status, truncateProfileConfigs] =
        schema->GetRuntimeSettings().GetValue<std::vector<indexlibv2::config::TruncateProfileConfig>>(
            "truncate_profiles");
    if (!status.IsOK() && !status.IsNotFound()) {
        AUTIL_LOG(ERROR, "get truncate profile config from setting failed.");
        return status;
    }
    auto indexConfigs = schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR);
    ResolveEmptyProfileNamesForTruncateIndex(truncateProfileConfigs, indexConfigs);
    status = AppendTruncateIndexConfig(indexConfigs, truncateProfileConfigs);
    RETURN_IF_STATUS_ERROR(status, "append truncate index config failed");

    for (auto& truncateProfileConfig : truncateProfileConfigs) {
        if (!IsValidTruncateSortParam(truncateProfileConfig.GetTruncateSortParams(), schema)) {
            AUTIL_LOG(ERROR, "invalid sort param for truncate, profileName[%s]",
                      truncateProfileConfig.GetTruncateProfileName().c_str());
            return Status::InvalidArgs("invalid sort param for truncate");
        }
    }
    return Status::OK();
}

Status NormalSchemaResolver::FillTruncateProfile(const indexlib::config::IndexPartitionSchema& legacySchema,
                                                 config::UnresolvedSchema* schema) const
{
    auto [isExist, _] = schema->GetRuntimeSetting("truncate_profiles");
    if (!isExist) {
        std::vector<indexlibv2::config::TruncateProfileConfig> truncateProfileConfigs;
        auto truncateSchema = legacySchema.GetTruncateProfileSchema();
        if (truncateSchema == nullptr) {
            AUTIL_LOG(INFO, "truncate profile schema not exist.");
            return Status::OK();
        }
        for (auto it = truncateSchema->Begin(); it != truncateSchema->End(); ++it) {
            std::string content;
            auto st = indexlib::file_system::JsonUtil::ToString(*(it->second), &content).Status();
            if (!st.IsOK()) {
                AUTIL_LOG(ERROR, "serialize truncate profile config failed.");
                return st;
            }
            indexlibv2::config::TruncateProfileConfig truncateProfileConfig;
            st = indexlib::file_system::JsonUtil::FromString(content, &truncateProfileConfig).Status();
            if (!st.IsOK()) {
                AUTIL_LOG(ERROR, "deserialize truncate profile config failed.");
                return st;
            }
            truncateProfileConfigs.emplace_back(std::move(truncateProfileConfig));
        }
        auto success = schema->SetRuntimeSetting("truncate_profiles", autil::legacy::ToJson(truncateProfileConfigs),
                                                 /*overwrite*/ false);
        if (!success) {
            AUTIL_LOG(ERROR, "set truncate profile config to schema failed.");
            return Status::InvalidArgs("set truncate profile config to schema failed");
        }
    }

    return Status::OK();
}

Status NormalSchemaResolver::LoadTruncateTermVocabulary(const std::string& indexPath,
                                                        config::UnresolvedSchema* schema) const
{
    assert(schema->GetTableType() == indexlib::table::TABLE_TYPE_NORMAL);
    // if option is online & need not rebuild truncate index (for FilteredMultiPartitionMerger)
    indexlib::file_system::FileSystemOptions options;
    auto [st1, fileSystem] =
        indexlib::file_system::FileSystemCreator::CreateForRead("truncate-tmp", indexPath, options).StatusWith();
    if (!st1.IsOK()) {
        AUTIL_LOG(ERROR, "load truncate term failed to create fs [%s]", st1.ToString().c_str());
        return st1;
    }
    auto rootDir = indexlib::file_system::Directory::Get(fileSystem)->GetIDirectory();
    if (rootDir == nullptr) {
        AUTIL_LOG(ERROR, "load truncate term failed to get root dir");
        return Status::InternalError("load truncate term failed to get root dir");
    }
    auto physicalRootDir = indexlib::file_system::IDirectory::GetPhysicalDirectory(indexPath);
    auto fsResult = physicalRootDir->GetDirectory(TRUNCATE_META_DIR_NAME);
    if (fsResult.Code() != indexlib::file_system::ErrorCode::FSEC_OK) {
        if (fsResult.Code() == indexlib::file_system::ErrorCode::FSEC_NOENT) {
            return Status::OK();
        }
        AUTIL_LOG(ERROR, "get directory [%s] failed, [%s]", TRUNCATE_META_DIR_NAME,
                  indexlib::file_system::toStatus(fsResult.Code()).ToString().c_str());
        return indexlib::file_system::toStatus(fsResult.Code());
    }
    RETURN_IF_STATUS_ERROR(rootDir->GetFileSystem()
                               ->MountDir(/*physicalRoot=*/rootDir->GetPhysicalPath(""),
                                          /*physicalPath=*/TRUNCATE_META_DIR_NAME,
                                          /*logicalPath=*/TRUNCATE_META_DIR_NAME,
                                          /*MountDirOption=*/indexlib::file_system::FSMT_READ_WRITE,
                                          /*enableLazyMount=*/false)
                               .Status(),
                           "mount truncate meta dir failed");
    auto pair = rootDir->GetDirectory(std::string(TRUNCATE_META_DIR_NAME));
    indexlib::file_system::ErrorCode ec = pair.Code();
    std::shared_ptr<indexlib::file_system::IDirectory> truncateMetaDir = pair.Value();
    if (ec != indexlib::file_system::ErrorCode::FSEC_OK) {
        AUTIL_LOG(ERROR, "get truncate meta dir failed, [%s]", indexlib::file_system::toStatus(ec).ToString().c_str());
        return indexlib::file_system::toStatus(ec);
    }
    indexlibv2::config::TruncateIndexNameMapper truncMapper(truncateMetaDir);
    auto st = truncMapper.Load();
    RETURN_IF_STATUS_ERROR(st, "load truncate name mapper failed.");

    // truncate meta dir might be in archive file or package format.
    const std::string PACKAGE_META_FILE_NAME = std::string(indexlib::file_system::PACKAGE_FILE_PREFIX) +
                                               std::string(indexlib::file_system::PACKAGE_FILE_META_SUFFIX);
    auto [st2, isExist] = truncateMetaDir->IsExist(PACKAGE_META_FILE_NAME).StatusWith();
    if (!st2.IsOK()) {
        AUTIL_LOG(INFO, "check package meta file failed, [%s]", st2.ToString().c_str());
        return st2;
    }

    std::shared_ptr<indexlib::file_system::ArchiveFolder> archiveFolder = nullptr;
    if (!isExist) {
        archiveFolder = truncateMetaDir->LEGACY_CreateArchiveFolder(false, "");
        if (archiveFolder == nullptr) {
            AUTIL_LOG(ERROR, "create archiveFolder failed, dir[%s]", truncateMetaDir->GetLogicalPath().c_str());
            return Status::InternalError("create archiveFolder failed.");
        }
    }
    auto loadTruncaTermVocabulary =
        [&truncMapper, &truncateMetaDir,
         &archiveFolder](const std::shared_ptr<config::InvertedIndexConfig>& invertedIndexConfig) -> Status {
        const std::string& indexName = invertedIndexConfig->GetIndexName();
        std::vector<std::string> truncNames;
        if (truncMapper.Lookup(indexName, truncNames)) {
            Status st;
            if (archiveFolder != nullptr) {
                st = invertedIndexConfig->LoadTruncateTermVocabulary(archiveFolder, truncNames);
            } else {
                st = invertedIndexConfig->LoadTruncateTermVocabulary(truncateMetaDir, truncNames);
            }
            RETURN_IF_STATUS_ERROR(st, "load truncate term vocabulary failed, index[%s].", indexName.c_str());
        }
        return Status::OK();
    };
    AUTIL_LOG(INFO, "begin load truncate term info");
    auto indexConfigs = schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR);
    for (auto indexConfig : indexConfigs) {
        auto invertedIndexConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(indexConfig);
        assert(invertedIndexConfig != nullptr);
        if (invertedIndexConfig->GetShardingType() ==
            config::InvertedIndexConfig::IndexShardingType::IST_NEED_SHARDING) {
            for (const auto& shardConfig : invertedIndexConfig->GetShardingIndexConfigs()) {
                auto shardInvertedIndexConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(shardConfig);
                assert(shardInvertedIndexConfig != nullptr);
                RETURN_IF_STATUS_ERROR(loadTruncaTermVocabulary(shardInvertedIndexConfig),
                                       "load truncate term volcabulary failed, indexName[%s]",
                                       shardInvertedIndexConfig->GetIndexName().c_str());
            }
        } else {
            assert(invertedIndexConfig->GetShardingType() ==
                   config::InvertedIndexConfig::IndexShardingType::IST_NO_SHARDING);
            RETURN_IF_STATUS_ERROR(loadTruncaTermVocabulary(invertedIndexConfig),
                                   "load truncate term volcabulary failed, indexName[%s]",
                                   invertedIndexConfig->GetIndexName().c_str());
        }
    }
    AUTIL_LOG(INFO, "end load truncate term info");
    if (archiveFolder != nullptr) {
        st = archiveFolder->Close().Status();
        if (!st.IsOK()) {
            AUTIL_LOG(ERROR, "close archive folder failed.");
            return st;
        }
    }
    return Status::OK();
}

Status NormalSchemaResolver::AppendTruncateIndexConfig(
    const std::vector<std::shared_ptr<config::IIndexConfig>>& indexConfigs,
    const std::vector<indexlibv2::config::TruncateProfileConfig>& truncateProfileConfigs)
{
    auto appendTruncateIndexConfig =
        [&truncateProfileConfigs, this](const std::shared_ptr<config::InvertedIndexConfig>& invertedConfig) -> Status {
        for (const auto& truncateProfileConfig : truncateProfileConfigs) {
            const auto truncateProfileName = truncateProfileConfig.GetTruncateProfileName();
            if (invertedConfig->HasTruncateProfile(&truncateProfileConfig)) {
                auto truncateIndexConfig = CreateTruncateIndexConfig(invertedConfig, truncateProfileConfig);
                invertedConfig->AppendTruncateIndexConfig(truncateIndexConfig);
                auto st = UpdateIndexConfigForTruncate(invertedConfig, truncateIndexConfig);
                RETURN_IF_STATUS_ERROR(st, "update index config[%s] for truncate profile[%s] failed",
                                       invertedConfig->GetIndexName().c_str(), truncateProfileName.c_str());
            }
        }
        return Status::OK();
    };
    for (const auto& indexConfig : indexConfigs) {
        auto invertedIndexConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(indexConfig);
        assert(invertedIndexConfig != nullptr);
        if (!invertedIndexConfig->HasTruncate()) {
            continue;
        }
        if (invertedIndexConfig->GetShardingType() == config::InvertedIndexConfig::IST_NEED_SHARDING) {
            for (const auto& shardInvertedConfig : invertedIndexConfig->GetShardingIndexConfigs()) {
                RETURN_IF_STATUS_ERROR(appendTruncateIndexConfig(shardInvertedConfig), "append truncate index failed");
            }
        } else {
            RETURN_IF_STATUS_ERROR(appendTruncateIndexConfig(invertedIndexConfig), "append truncate index failed");
        }
    }
    return Status::OK();
}

// IndexConfig needs to be modified if it has TruncateIndexConfig that uses payload. This is a compatibility change
// for IndexConfig to support payload. Future design of IndexConfig should support paylaod natively.
Status NormalSchemaResolver::UpdateIndexConfigForTruncate(
    const std::shared_ptr<config::InvertedIndexConfig>& indexConfig,
    const std::shared_ptr<config::InvertedIndexConfig>& truncateIndexConfig)
{
    const std::string& existingPayloadName = indexConfig->GetTruncatePayloadConfig().GetName();
    const std::string& incomingPayloadName = truncateIndexConfig->GetTruncatePayloadConfig().GetName();

    if (!indexConfig->GetTruncatePayloadConfig().IsInitialized()) {
        indexConfig->SetTruncatePayloadConfig(truncateIndexConfig->GetTruncatePayloadConfig());
        return Status::OK();
    }
    if (truncateIndexConfig->GetTruncatePayloadConfig().IsInitialized() && existingPayloadName != incomingPayloadName) {
        return Status::InternalError("Index [%s] has different truncate payload [%s] and [%s]",
                                     indexConfig->GetIndexName().c_str(), existingPayloadName.c_str(),
                                     incomingPayloadName.c_str());
    }
    return Status::OK();
}
std::shared_ptr<config::InvertedIndexConfig>
NormalSchemaResolver::CreateTruncateIndexConfig(const std::shared_ptr<config::InvertedIndexConfig>& invertedIndexConfig,
                                                const config::TruncateProfileConfig& truncateProfileConfig) const
{
    const std::string& indexName = invertedIndexConfig->GetIndexName();
    auto truncateIndexConfig = std::shared_ptr<config::InvertedIndexConfig>(invertedIndexConfig->Clone());
    truncateIndexConfig->SetIndexName(config::InvertedIndexConfig::CreateTruncateIndexName(
        indexName, truncateProfileConfig.GetTruncateProfileName()));
    // TODO(xc & lc) 在InvertedIndexReaderImpl open阶段判断是否使用默认值逻辑中, 需要区分当前config是否是truncate
    // index
    truncateIndexConfig->SetNonTruncateIndexName(indexName);
    truncateIndexConfig->SetVirtual(true);
    truncateIndexConfig->SetFileCompressConfig(invertedIndexConfig->GetFileCompressConfig());
    truncateIndexConfig->SetHasTruncateFlag(false);
    truncateIndexConfig->SetTruncatePayloadConfig(truncateProfileConfig.GetPayloadConfig());

    return truncateIndexConfig;
}

bool NormalSchemaResolver::IsValidTruncateSortParam(const std::vector<indexlib::config::SortParam>& sortParams,
                                                    const config::UnresolvedSchema* schema) const
{
    for (auto& sortParam : sortParams) {
        auto sortField = sortParam.GetSortField();
        if (sortField == DOC_PAYLOAD_FIELD_NAME) {
            continue;
        }
        auto indexConfig = schema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, sortField);
        if (indexConfig == nullptr) {
            AUTIL_LOG(ERROR, "sort field in truncate profile must be an attribute, field[%s]", sortField.c_str());
            return false;
        }
        auto attributeConfig = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(indexConfig);
        assert(attributeConfig != nullptr);
        if (attributeConfig->IsMultiValue()) {
            AUTIL_LOG(ERROR, "sort field in truncate profile must be an single-value attribute, field[%s]",
                      sortField.c_str());
            return false;
        }
        // TODO(xc) ft_fp8 ft_fp16?
        auto fieldType = attributeConfig->GetFieldType();
        if (fieldType != ft_integer && fieldType != ft_float && fieldType != ft_long && fieldType != ft_uint8 &&
            fieldType != ft_uint16 && fieldType != ft_uint32 && fieldType != ft_uint64 && fieldType != ft_double &&
            fieldType != ft_int8 && fieldType != ft_int16 && fieldType != ft_int32 && fieldType != ft_int64 &&
            fieldType != ft_time && fieldType != ft_timestamp && fieldType != ft_date) {
            AUTIL_LOG(ERROR, "sort field in truncate profile must be a numeric field, field[%s]", sortField.c_str());

            return false;
        }
    }
    return true;
}
} // namespace indexlibv2::table
