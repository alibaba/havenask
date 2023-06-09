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
    std::shared_ptr<indexlibv2::config::AttributeConfig> attrConfig)
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

Status NormalSchemaResolver::Check(const config::TabletSchema& schema)
{
    RETURN_IF_STATUS_ERROR(CheckStatisticsTermConfig(schema), "check statistics term config failed.");
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
    RETURN_IF_STATUS_ERROR(LoadAdaptiveVocabulary(schema, indexPath), "load adaptive vocabulary failed");
    RETURN_IF_STATUS_ERROR(ResolveTTLField(schema), "resolve ttl field failed");
    RETURN_IF_STATUS_ERROR(ResolveSpatialIndex(schema), "resolve spatial index");
    RETURN_IF_STATUS_ERROR(AddBuiltinIndex(schema), "add built index failed");
    RETURN_IF_STATUS_ERROR(ResolveSortDescriptions(schema, _options, indexPath), "resolve sort descs failed");
    RETURN_IF_STATUS_ERROR(ResolveTruncateIndexConfigs(indexPath, schema), "resolve truncate index configs failed");
    RETURN_IF_STATUS_ERROR(ResolvePKIndexConfig(schema, _options), "resolve pk index config");
    RETURN_IF_STATUS_ERROR(ResolveSummary(schema), "resolve summary failed");
    RETURN_IF_STATUS_ERROR(ResolveAliasTypes(schema), "resolve alias types");
    ResolveGeneralizedValue(schema);
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
    if (pkIndexConfig->GetIndexId() == INVALID_INDEXID) {
        const auto& invertedConfigs = schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR);
        pkIndexConfig->SetIndexId(invertedConfigs.size());
    }

    autil::ScopeGuard done([&schema, &pkIndexConfig]() { schema->SetPrimaryKeyIndexConfig(pkIndexConfig); });

    if (!option) {
        AUTIL_LOG(WARN, "option is NULL, not resolve pk index config");
        return Status::OK();
    }
    std::string speedUpPKReaderPath = "online_index_config.build_config.speedup_primary_key_reader";
    bool speedUpPrimaryKeyReader = false;
    std::string speedUpPKParamPath = "online_index_config.build_config.speedup_primary_key_param";
    std::string speedUpPKParam = "";
    if (option->GetFromRawJson(speedUpPKReaderPath, &speedUpPrimaryKeyReader) && speedUpPrimaryKeyReader) {
        option->GetFromRawJson(speedUpPKParamPath, &speedUpPKParam);
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
    if (option->GetFromRawJson(pkParallelLookupPath, &pkParallelLookup) && pkParallelLookup) {
        pkIndexConfig->EnableParallelLookupOnBuild();
    }

    std::string enableBloomFilterPath = "online_index_config.build_config.enable_bloom_filter_for_primary_key_reader";
    std::string bloomFilterMultipleNumPath = "online_index_config.build_config.bloom_filter_multiple_num";
    bool enableBloomFilter = false;
    if (option->GetFromRawJson(enableBloomFilterPath, &enableBloomFilter) && enableBloomFilter) {
        uint32_t bloomFilterMultipleNum = index::PrimaryKeyIndexConfig::DEFAULT_BLOOM_FILTER_MULTIPLE_NUM;
        if (option->GetFromRawJson(bloomFilterMultipleNumPath, &bloomFilterMultipleNum)) {
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
    auto [status, _] = schema->GetSetting<std::shared_ptr<std::vector<config::SingleFileCompressConfig>>>(
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
    bool ret = schema->SetSetting(config::SingleFileCompressConfig::FILE_COMPRESS_CONFIG_KEY, configs, false);
    if (!ret) {
        RETURN_IF_STATUS_ERROR(Status::InternalError(), "schema set setting [%s] failed",
                               config::SingleFileCompressConfig::FILE_COMPRESS_CONFIG_KEY.c_str());
    }
    return Status::OK();
}

Status NormalSchemaResolver::ResolveSummary(config::UnresolvedSchema* schema)
{
    const auto& fieldConfigs = schema->GetIndexFieldConfigs(indexlib::index::SUMMARY_INDEX_TYPE_STR);
    for (const auto& fieldConfig : fieldConfigs) {
        if (fieldConfig->GetFieldType() != ft_timestamp) {
            continue;
        }
        const auto& fieldName = fieldConfig->GetFieldName();
        if (schema->GetIndexConfig(indexlib::index::ATTRIBUTE_INDEX_TYPE_STR, fieldName)) {
            continue;
        }
        AUTIL_LOG(INFO, "inner add timestamp type attribute [%s] to support storage in summary", fieldName.c_str());
        auto attributeConfig = std::make_shared<config::AttributeConfig>(config::AttributeConfig::ct_summary_accompany);
        attributeConfig->Init(fieldConfig);
        RETURN_IF_STATUS_ERROR(schema->AddIndexConfig(attributeConfig), "add summary accompany attribute [%s] failed",
                               fieldName.c_str());
    }
    SetNeedStoreSummary(schema);
    return Status::OK();
}

void NormalSchemaResolver::SetNeedStoreSummary(config::UnresolvedSchema* schema)
{
    const auto& config =
        schema->GetIndexConfig(indexlib::index::SUMMARY_INDEX_TYPE_STR, indexlib::index::SUMMARY_INDEX_NAME);
    if (!config) {
        return;
    }
    const auto& summaryConfig = std::dynamic_pointer_cast<config::SummaryIndexConfig>(config);
    assert(summaryConfig);
    const auto& attrFields = schema->GetIndexFieldConfigs(indexlib::index::ATTRIBUTE_INDEX_TYPE_STR);
    if (attrFields.empty()) {
        summaryConfig->SetNeedStoreSummary(true);
        return;
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
}

void NormalSchemaResolver::FillTTLToSettings(const indexlib::config::IndexPartitionSchema& legacySchema,
                                             config::UnresolvedSchema* schema)
{
    if (legacySchema.TTLEnabled()) {
        auto [status, ttlEnable] = schema->GetSetting<bool>("enable_ttl");
        auto [status1, fieldName] = schema->GetSetting<std::string>("ttl_field_name");
        auto [status2, defaultTTL] = schema->GetSetting<int64_t>("default_ttl");
        if (status.IsOK() || status1.IsOK() || status2.IsOK()) {
            // tablet schema setting has ttl config, not use legacy schema
            return;
        }

        bool ret = schema->SetSetting("enable_ttl", true, false);
        ret = schema->SetSetting("ttl_field_name", legacySchema.GetTTLFieldName(), false) && ret;
        ret = schema->SetSetting("default_ttl", legacySchema.GetDefaultTTL(), false) && ret;
        assert(ret);
    }
}

Status NormalSchemaResolver::ResolveSpatialIndex(config::UnresolvedSchema* schema)
{
    for (auto indexConfig : schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR)) {
        if (auto spatialIndexConfig = std::dynamic_pointer_cast<config::SpatialIndexConfig>(indexConfig)) {
            if (spatialIndexConfig->IsDeleted()) {
                continue;
            }
            auto fieldConfig = spatialIndexConfig->GetFieldConfig();
            if (fieldConfig->GetFieldType() != FieldType::ft_location) {
                continue;
            }
            auto fieldName = fieldConfig->GetFieldName();
            auto attributeConfig = std::dynamic_pointer_cast<config::AttributeConfig>(
                schema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, fieldName));
            if (!attributeConfig) {
                AUTIL_LOG(INFO, "add attribute [%s] to ensure spatial index precision", fieldName.c_str());
                auto attributeConfig =
                    std::make_shared<config::AttributeConfig>(config::AttributeConfig::ct_index_accompany);
                attributeConfig->Init(fieldConfig);
                attributeConfig->SetFileCompressConfigV2(spatialIndexConfig->GetFileCompressConfigV2());
                RETURN_IF_STATUS_ERROR(schema->AddIndexConfig(attributeConfig), "add spatial attribute [%s] failed",
                                       fieldName.c_str());
            }
        }
    }
    return Status::OK();
}

Status NormalSchemaResolver::ResolveTTLField(config::UnresolvedSchema* schema)
{
    auto [status, enableTTL] = schema->GetSetting<bool>("enable_ttl");
    auto ttlAttrConfig = std::make_shared<config::AttributeConfig>();
    if (status.IsOK()) {
        if (!enableTTL) {
            return Status::OK();
        }
        // ttl field resolve
        auto [status, ttlFieldName] = schema->GetSetting<std::string>("ttl_field_name");
        if (status.IsOK()) {
            auto attributeConfig = std::dynamic_pointer_cast<config::AttributeConfig>(
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
                    RETURN_STATUS_ERROR(ConfigError, "ttl field [%s] is in filed config but not in attribute config",
                                        DOC_TIME_TO_LIVE_IN_SECONDS.c_str());
                }
                fieldConfig = std::make_shared<config::FieldConfig>(DOC_TIME_TO_LIVE_IN_SECONDS, ft_uint32, false);

                RETURN_IF_STATUS_ERROR(schema->AddFieldConfig(fieldConfig), "add field config [%s] failed",
                                       DOC_TIME_TO_LIVE_IN_SECONDS.c_str());
                if (!schema->SetSetting("ttl_field_name", DOC_TIME_TO_LIVE_IN_SECONDS, false)) {
                    RETURN_STATUS_DIRECTLY_IF_ERROR(Status::InternalError(""));
                }
                auto attributeConfig = std::make_shared<config::AttributeConfig>();
                attributeConfig->Init(fieldConfig);
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
        auto [status1, defaultTTL] = schema->GetSetting<int64_t>("default_ttl");
        if (status1.IsNotFound()) {
            bool ret = schema->SetSetting("default_ttl", DEFAULT_TIME_TO_LIVE, false);
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
    auto [isExist, _] = schema->GetSettingConfig("sort_descriptions");
    // legacy read old partitionMeta to set sortDesc to schema
    // options maybe nullptr when read a resolved schema in index path (offline case)
    // new sort description is store to schema
    if (!isExist) {
        indexlib::legacy::index_base::PartitionMeta partMeta;
        try {
            if (indexlib::legacy::index_base::PartitionMeta::IsExist(indexPath)) {
                partMeta.Load(indexPath);
                if (partMeta.Size() > 0) {
                    schema->SetSettingConfig("sort_descriptions", autil::legacy::ToJson(partMeta.GetSortDescriptions()),
                                             /*overwrite*/ false);
                    AUTIL_LOG(INFO, "set schema sort_descriptions [%s] from partionMeta",
                              autil::legacy::ToJsonString(partMeta.GetSortDescriptions(), true).c_str());
                }
            } else if (options && options->GetBuildOptionConfig().IsSortBuild()) {
                const auto& sortDescriptions = options->GetBuildOptionConfig().GetSortDescriptions();
                if (sortDescriptions.empty()) {
                    RETURN_STATUS_ERROR(ConfigError, "sort build desc cannot be empty when sort_build set true");
                }
                schema->SetSettingConfig("sort_descriptions", autil::legacy::ToJson(sortDescriptions),
                                         /*overwrite*/ false);
                AUTIL_LOG(INFO, "set schema sort_descriptions [%s] from build option config",
                          autil::legacy::ToJsonString(sortDescriptions, true).c_str());
            }
        } catch (const autil::legacy::ExceptionBase& e) {
            AUTIL_LOG(ERROR, "resolve schema failed with exception [%s]", e.what());
            return Status::IOError(e.what());
        }
    }
    return CheckSortDescs(schema);
}

Status NormalSchemaResolver::CheckSortDescs(const config::UnresolvedSchema* schema)
{
    auto [status, sortDescs] = schema->GetSetting<config::SortDescriptions>("sort_descriptions");
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
        auto attributeConfig = std::dynamic_pointer_cast<indexlibv2::config::AttributeConfig>(indexConfig);
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
    if (pkConfigs.empty()) {
        AUTIL_LOG(INFO, "pk config is empty, no need rewrite");
        return Status::OK();
    }
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
            RETURN_STATUS_DIRECTLY_IF_ERROR(schema->AddFieldConfig(fieldConfig));
        }

        assert(fieldConfig);
        std::shared_ptr<indexlibv2::config::AttributeConfig> attrConfig(
            new indexlibv2::config::AttributeConfig(indexlibv2::config::AttributeConfig::ct_normal));
        attrConfig->Init(fieldConfig);
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

void NormalSchemaResolver::ResolveGeneralizedValue(config::UnresolvedSchema* schema)
{
    std::vector<std::shared_ptr<config::FieldConfig>> fields;
    const auto& indexConfigs = schema->GetIndexConfigs(index::ATTRIBUTE_INDEX_TYPE_STR);
    for (const auto& indexConfig : indexConfigs) {
        const auto& attrConfig = std::dynamic_pointer_cast<config::AttributeConfig>(indexConfig);
        assert(attrConfig);
        fields.emplace_back(attrConfig->GetFieldConfig());
    }
    schema->SetGeneralizedValueIndexFieldConfigs(fields);
}

Status NormalSchemaResolver::ResolveAliasTypes(config::UnresolvedSchema* schema)
{
    auto pkIndexConfig = schema->GetPrimaryKeyIndexConfig();
    if (pkIndexConfig &&
        !schema->GetIndexConfig(indexlib::index::INVERTED_INDEX_TYPE_STR, pkIndexConfig->GetIndexName())) {
        RETURN_IF_STATUS_ERROR(schema->AddIndexConfig(indexlib::index::INVERTED_INDEX_TYPE_STR, pkIndexConfig),
                               "add pk index config to inverted failed");
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
    std::vector<std::string> indexTypes = {index::PRIMARY_KEY_INDEX_TYPE_STR, indexlib::index::INVERTED_INDEX_TYPE_STR,
                                           index::ATTRIBUTE_INDEX_TYPE_STR, index::SUMMARY_INDEX_TYPE_STR};
    auto [status, singleFileCompressConfigVec] =
        schema->GetSetting<std::shared_ptr<std::vector<config::SingleFileCompressConfig>>>(
            config::SingleFileCompressConfig::FILE_COMPRESS_CONFIG_KEY);
    if (!status.IsOK() && !status.IsNotFound()) {
        RETURN_IF_STATUS_ERROR(status, "get file compress config from setting failed");
    }
    if (singleFileCompressConfigVec) {
        RETURN_IF_STATUS_ERROR(config::SingleFileCompressConfig::ValidateConfigs(*singleFileCompressConfigVec),
                               "file compress invalid");
    }

    for (const auto& indexType : indexTypes) {
        if (indexType == indexlib::index::INVERTED_INDEX_TYPE_STR) {
            const auto& indexSchema = legacySchema.GetIndexSchema();
            if (indexSchema != nullptr) {
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
                    if (schema->GetIndexConfig(indexType, indexConfig->GetIndexName())) {
                        // already added
                        // TODO: need to be continue, when support new schema
                        return Status::OK();
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
            }
        } else if (indexType == index::ATTRIBUTE_INDEX_TYPE_STR) {
            const auto& attrSchema = legacySchema.GetAttributeSchema();
            if (attrSchema != nullptr) {
                for (auto it = attrSchema->Begin(); it != attrSchema->End(); it++) {
                    auto attrConfig = *it;
                    if (schema->GetIndexConfig(indexType, attrConfig->GetIndexName())) {
                        // already added
                        return Status::OK();
                    }
                    RETURN_IF_STATUS_ERROR(ResolveFileCompressConfig(singleFileCompressConfigVec, attrConfig),
                                           "resolve file compress config fail, indexName[%s]",
                                           attrConfig->GetIndexName().c_str());
                    auto status = schema->AddIndexConfig(attrConfig);
                    RETURN_IF_STATUS_ERROR(status, "add attribute config fail, indexName[%s]",
                                           attrConfig->GetIndexName().c_str());
                }
            }
        } else if (indexType == index::PRIMARY_KEY_INDEX_TYPE_STR) {
            auto indexConfigs = schema->GetIndexConfigs(indexType);
            if (indexConfigs.empty()) {
                const auto& indexSchema = legacySchema.GetIndexSchema();
                if (indexSchema != nullptr && indexSchema->HasPrimaryKeyIndex()) {
                    auto pkConfig = indexSchema->GetPrimaryKeyIndexConfig();
                    if (schema->GetIndexConfig(indexType, pkConfig->GetIndexName())) {
                        // already added
                        return Status::OK();
                    }
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
        } else if (indexType == index::SUMMARY_INDEX_TYPE_STR) {
            const auto& summarySchema = legacySchema.GetSummarySchema();

            if (nullptr != summarySchema) {
                std::shared_ptr<indexlibv2::config::SummaryIndexConfig> summaryIndexConfig(
                    summarySchema->MakeSummaryIndexConfigV2().release());
                if (schema->GetIndexConfig(indexType, summaryIndexConfig->GetIndexName())) {
                    // already added
                    return Status::OK();
                }
                RETURN_IF_STATUS_ERROR(ResolveFileCompressConfig(singleFileCompressConfigVec, summaryIndexConfig),
                                       "resolve summary compress config failed");
                auto status = schema->AddIndexConfig(summaryIndexConfig);
                RETURN_IF_STATUS_ERROR(status, "add summary index config fail, indexName[%s]",
                                       summaryIndexConfig->GetIndexName().c_str());
            }
        }
    }
    return Status::OK();
}

Status NormalSchemaResolver::ResolveTruncateIndexConfigs(const std::string& indexPath, config::UnresolvedSchema* schema)
{
    auto [isExist, _] = schema->GetSettingConfig("truncate_profiles");
    if (!isExist) {
        AUTIL_LOG(DEBUG, "truncate profile not exist.");
        return Status::OK();
    }

    auto st = LoadTruncateTermVocabulary(indexPath, schema);
    RETURN_IF_STATUS_ERROR(st, "load truncate term vocabulary failed.");

    auto [status, truncateProfileConfigs] =
        schema->GetSetting<std::vector<indexlibv2::config::TruncateProfileConfig>>("truncate_profiles");
    if (!status.IsOK() && !status.IsNotFound()) {
        AUTIL_LOG(ERROR, "get truncate profile config from setting failed.");
        return status;
    }
    auto indexConfigs = schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR);
    status = AppendTruncateIndexConfig(indexConfigs, truncateProfileConfigs);
    RETURN_IF_STATUS_ERROR(status, "append truncate index config failed");

    for (auto& truncateProfileConfig : truncateProfileConfigs) {
        if (!IsValidTruncateSortParam(truncateProfileConfig.GetTruncateSortParams(), schema)) {
            AUTIL_LOG(ERROR, "invalid sort param for truncate, profileName[%s]",
                      truncateProfileConfig.GetTruncateProfileName().c_str());
            return Status::InvalidArgs("invalid sort param for truncate");
        }
    }

    std::vector<indexlibv2::config::TruncateStrategy> truncateStrategys;
    if (_options != nullptr) {
        truncateStrategys = _options->GetMergeConfig().GetTruncateStrategys();
    }
    if (!truncateStrategys.empty()) {
        auto success = schema->SetRuntimeSetting<std::vector<config::TruncateStrategy>>("truncate.truncate_strategy",
                                                                                        truncateStrategys);
        if (!success) {
            AUTIL_LOG(ERROR, "set truncate strategy failed.");
            return Status::InternalError("set truncate strategy failed.");
        }
    }

    return Status::OK();
}

Status NormalSchemaResolver::FillTruncateProfile(const indexlib::config::IndexPartitionSchema& legacySchema,
                                                 config::UnresolvedSchema* schema) const
{
    auto [isExist, _] = schema->GetSettingConfig("truncate_profiles");
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
        auto success = schema->SetSettingConfig("truncate_profiles", autil::legacy::ToJson(truncateProfileConfigs),
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
        auto attributeConfig = std::dynamic_pointer_cast<indexlibv2::config::AttributeConfig>(indexConfig);
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
