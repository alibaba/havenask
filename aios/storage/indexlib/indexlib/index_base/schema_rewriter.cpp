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
#include "indexlib/index_base/schema_rewriter.h"

#include "indexlib/config/configurator_define.h"
#include "indexlib/config/disable_fields_config.h"
#include "indexlib/config/document_deduper_helper.h"
#include "indexlib/config/index_config_creator.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil::legacy;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::file_system;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, SchemaRewriter);

SchemaRewriter::SchemaRewriter() {}

SchemaRewriter::~SchemaRewriter() {}

void SchemaRewriter::Rewrite(const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options,
                             const DirectoryPtr& rootDir)
{
    // for kv & kkv rewrite levelTopology, enable package file ..., etc.
    IndexPartitionOptions& mutableOptions = const_cast<IndexPartitionOptions&>(options);
    mutableOptions.RewriteForSchema(schema);
    options.Check();

    // update schema standards should be rewrite first
    RewriteForUpdateSchemaStandards(schema, options);
    RewriteForDisableSource(schema, options);
    RewriteForDisableSummary(schema, options);
    RewriteForDisableFields(schema, options);
    RewriteForDisableIndexs(schema, options);
    RewriteForTruncateIndexConfigs(schema, options, rootDir);
    RewriteForSubTable(schema);
    RewriteForPrimaryKey(schema, options);
    RewriteFieldType(schema, options);
    RewriteForEnableBloomFilter(schema, options);

    IndexFormatVersion indexFormatVersion(INDEX_FORMAT_VERSION);
    if (!indexFormatVersion.Load(rootDir, true) && options.IsOnline() && !options.GetOnlineConfig().IsPrimaryNode()) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "[%s] not exist",
                             rootDir->DebugString(INDEX_FORMAT_VERSION_FILE_NAME).c_str());
    }
    RewriteForKV(schema, options, indexFormatVersion);
    schema->Check();
}

void SchemaRewriter::SetPrimaryKeyLoadParam(const IndexPartitionSchemaPtr& schema,
                                            PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode,
                                            const std::string& loadParam, bool needLookupReverse)
{
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    assert(indexSchema);
    const IndexConfigPtr& indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    if (!indexConfig) {
        return;
    }
    const PrimaryKeyIndexConfigPtr& pkIndexConfig = DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, indexConfig);
    assert(pkIndexConfig);
    pkIndexConfig->SetPrimaryKeyLoadParam(loadMode, needLookupReverse, loadParam);
}

void SchemaRewriter::DisablePKLoadParam(const IndexPartitionSchemaPtr& schema)
{
    PrimaryKeyIndexConfigPtr indexConfig =
        DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    if (!indexConfig || indexConfig->GetInvertedIndexType() == it_trie) {
        return;
    }

    PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode;
    string loadParam;
    if (indexConfig->GetPrimaryKeyIndexType() == pk_hash_table) {
        loadMode = PrimaryKeyLoadStrategyParam::HASH_TABLE;
    } else if (indexConfig->GetPrimaryKeyIndexType() == pk_block_array) {
        loadMode = PrimaryKeyLoadStrategyParam::BLOCK_VECTOR;
    } else {
        loadMode = PrimaryKeyLoadStrategyParam::SORTED_VECTOR;
    }
    SetPrimaryKeyLoadParam(schema, loadMode, loadParam, false);
    const IndexPartitionSchemaPtr& subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema) {
        SetPrimaryKeyLoadParam(subSchema, loadMode, loadParam, false);
    }
}

void SchemaRewriter::RewriteForPrimaryKey(const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options)
{
    PrimaryKeyIndexConfigPtr indexConfig =
        DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    if (!indexConfig || indexConfig->GetInvertedIndexType() == it_trie) {
        return;
    }

    PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode;
    string loadParam;
    const BuildConfig& buildConfig = options.GetBuildConfig();
    if (buildConfig.speedUpPrimaryKeyReader || indexConfig->GetPrimaryKeyIndexType() == pk_hash_table) {
        loadMode = PrimaryKeyLoadStrategyParam::HASH_TABLE;
    } else if (indexConfig->GetPrimaryKeyIndexType() == pk_block_array) {
        loadMode = PrimaryKeyLoadStrategyParam::BLOCK_VECTOR;
    } else {
        loadMode = PrimaryKeyLoadStrategyParam::SORTED_VECTOR;
    }

    if (buildConfig.speedUpPrimaryKeyReader) {
        loadParam = buildConfig.speedUpPrimaryKeyReaderParam;
    }

    if (buildConfig.IsPkBuildParallelLookup()) {
        indexConfig->EnableParallelLookupOnBuild();
    }

    uint32_t bloomFilterMultipleNum = 0;
    if (buildConfig.GetBloomFilterParamForPkReader(bloomFilterMultipleNum)) {
        if (indexConfig->GetPrimaryKeyIndexType() != pk_hash_table) {
            indexConfig->EnableBloomFilterForPkReader(bloomFilterMultipleNum);
        } else {
            AUTIL_LOG(WARN, "hash table type primary key will not enable bloom filter.");
        }
    }

    bool needLookupReverse = DocumentDeduperHelper::DelayDedupDocument(options, indexConfig);
    SetPrimaryKeyLoadParam(schema, loadMode, loadParam, needLookupReverse);
    const IndexPartitionSchemaPtr& subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema) {
        SetPrimaryKeyLoadParam(subSchema, loadMode, loadParam, needLookupReverse);
    }
}

void SchemaRewriter::RewriteForKV(const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options,
                                  const IndexFormatVersion& indexFormatVersion)
{
    if (schema->GetTableType() != tt_kv && schema->GetTableType() != tt_kkv) {
        return;
    }

    // TODO: delete later when kkv support disable ttl
    if (schema->GetTableType() == tt_kkv) {
        IE_LOG(INFO, "KKV Table only support enable ttl yet, will enableTTL by default");
        for (regionid_t i = 0; i < (regionid_t)schema->GetRegionCount(); i++) {
            if (!schema->TTLEnabled(i)) {
                schema->SetEnableTTL(true, i);
            }
        }
    }

    for (regionid_t i = 0; i < (regionid_t)schema->GetRegionCount(); i++) {
        RewriteOneRegionForKV(schema, options, indexFormatVersion, i);
    }

    if (schema->GetTableType() == tt_kkv) {
        UnifyKKVBuildParam(schema, indexFormatVersion);
        UnifyKKVIndexPreference(schema);
    }

    if (schema->GetTableType() == tt_kv) {
        UnifyKVIndexConfig(schema, indexFormatVersion);
    }
}

void SchemaRewriter::RewriteOneRegionForKV(const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options,
                                           const IndexFormatVersion& indexFormatVersion, regionid_t id)
{
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema(id);
    if (!indexSchema) {
        IE_LOG(ERROR, "region [%d] not has indexSchema!", id);
        return;
    }

    const SingleFieldIndexConfigPtr& pkConfig = indexSchema->GetPrimaryKeyIndexConfig();
    KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, pkConfig);
    assert(kvConfig);
    int64_t ttl = options.IsOnline() ? options.GetOnlineConfig().ttl : options.GetBuildConfig().ttl;
    if (schema->TTLEnabled(id)) {
        if (ttl < 0) {
            ttl = schema->GetDefaultTTL(id);
        }
        kvConfig->SetTTL(ttl);
    }

    // kv
    if (schema->GetTableType() == tt_kv) {
        if (options.IsOnline() && autil::EnvUtil::hasEnv("KV_SUPPORT_SWAP_MMAP_FILE")) {
            kvConfig->SetUseSwapMmapFile(options.GetOnlineConfig().useSwapMmapFile);
            kvConfig->SetMaxSwapMmapFileSize(options.GetOnlineConfig().maxSwapMmapFileSize);
        }
        return;
    }

    // kkv
    if (schema->GetTableType() == tt_kkv) {
        KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, pkConfig);
        assert(kkvConfig);
        uint32_t buildProtectionThreshold = numeric_limits<uint32_t>::max();
        if (GetRewriteBuildProtectionThreshold(schema, options, buildProtectionThreshold)) {
            IE_LOG(INFO, "rewrite build_protection_threshold from %u to %u",
                   kkvConfig->GetSuffixKeyProtectionThreshold(), buildProtectionThreshold);
            kkvConfig->SetSuffixKeyProtectionThreshold(buildProtectionThreshold);
        }

        if (options.IsOnline()) {
            const KVOnlineConfig& kvOnlineConfig = options.GetOnlineConfig().kvOnlineConfig;
            if (kvOnlineConfig.countLimits < kkvConfig->GetSuffixKeyTruncateLimits()) {
                IE_LOG(INFO, "rewrite SuffixKeyTruncateLimits from %u to %u", kkvConfig->GetSuffixKeyTruncateLimits(),
                       kvOnlineConfig.countLimits);
                kkvConfig->SetSuffixKeyTruncateLimits(kvOnlineConfig.countLimits);
            }
            kkvConfig->SetUseSwapMmapFile(options.GetOnlineConfig().useSwapMmapFile);
            kkvConfig->SetMaxSwapMmapFileSize(options.GetOnlineConfig().maxSwapMmapFileSize);
        }
    }
}

void SchemaRewriter::RewriteForSubTable(const config::IndexPartitionSchemaPtr& schema)
{
    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    if (!subSchema) {
        return;
    }
    AddOfflineJoinVirtualAttribute(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME, schema);
    AddOfflineJoinVirtualAttribute(SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME, subSchema);
}

void SchemaRewriter::AddOfflineJoinVirtualAttribute(const std::string& attrName,
                                                    const config::IndexPartitionSchemaPtr& destSchema)
{
    // TODO: whether exist field, or user defined, check exist
    FieldSchemaPtr fieldSchemaPtr = destSchema->GetFieldSchema();
    if (fieldSchemaPtr && fieldSchemaPtr->IsFieldNameInSchema(attrName)) {
        return;
    }

    auto fieldConfig = destSchema->AddFieldConfig(attrName, ft_int32, false, false);
    fieldConfig->SetBuiltInField(true);
    destSchema->AddAttributeConfig(attrName);
}

void SchemaRewriter::RewriteForRealtimeTimestamp(const IndexPartitionSchemaPtr& schema)
{
    assert(schema);
    assert(schema->GetFieldSchema());
    if (schema->GetFieldSchema()->IsFieldNameInSchema(VIRTUAL_TIMESTAMP_FIELD_NAME)) {
        return;
    }
    schema->AddFieldConfig(VIRTUAL_TIMESTAMP_FIELD_NAME, VIRTUAL_TIMESTAMP_FIELD_TYPE, false, true);

    IndexConfigPtr indexConfig = IndexConfigCreator::CreateSingleFieldIndexConfig(
        VIRTUAL_TIMESTAMP_INDEX_NAME, VIRTUAL_TIMESTAMP_INDEX_TYPE, VIRTUAL_TIMESTAMP_FIELD_NAME, "", true,
        schema->GetFieldSchema());
    schema->AddIndexConfig(indexConfig);
    indexConfig->SetOptionFlag(NO_POSITION_LIST);
}

void SchemaRewriter::RewriteForRemoveTFBitmapFlag(const IndexPartitionSchemaPtr& schema)
{
    DoRewriteForRemoveTFBitmapFlag(schema);
    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema) {
        DoRewriteForRemoveTFBitmapFlag(subSchema);
    }
}

void SchemaRewriter::DoRewriteForRemoveTFBitmapFlag(const IndexPartitionSchemaPtr& schema)
{
    assert(schema);
    assert(schema->GetIndexSchema());
    // TODO remove disable/deleted index?
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    for (IndexSchema::Iterator iter = indexSchema->Begin(); iter != indexSchema->End(); ++iter) {
        optionflag_t flag = (*iter)->GetOptionFlag();
        if (flag & of_tf_bitmap) {
            (*iter)->SetOptionFlag(flag & (~of_tf_bitmap));
        }
    }
}

void SchemaRewriter::RewriteForDisableSource(const IndexPartitionSchemaPtr& schema,
                                             const IndexPartitionOptions& options)
{
    auto sourceSchema = schema->GetSourceSchema();
    if (!sourceSchema) {
        return;
    }

    if (options.GetOnlineConfig().GetDisableFieldsConfig().sources ==
        DisableFieldsConfig::SourceDisableField::CDF_FIELD_ALL) {
        sourceSchema->DisableAllFields();
    }

    if (options.GetOnlineConfig().GetDisableFieldsConfig().sources ==
        DisableFieldsConfig::SourceDisableField::CDF_FIELD_GROUP) {
        for (auto id : options.GetOnlineConfig().GetDisableFieldsConfig().disabledSourceGroupIds) {
            sourceSchema->DisableFieldGroup(id);
        }
    }
}

void SchemaRewriter::RewriteForDisableSummary(const IndexPartitionSchemaPtr& schema,
                                              const IndexPartitionOptions& options)
{
    auto summarySchema = schema->GetSummarySchema();
    if (summarySchema && options.GetOnlineConfig().GetDisableFieldsConfig().summarys ==
                             DisableFieldsConfig::SummaryDisableField::SDF_FIELD_ALL) {
        summarySchema->DisableAllFields();
    }
}

void SchemaRewriter::RewriteForUpdateSchemaStandards(const IndexPartitionSchemaPtr& schema,
                                                     const IndexPartitionOptions& options)
{
    if (schema->GetTableType() != tt_index || options.GetUpdateableSchemaStandards().IsEmpty()) {
        return;
    }
    schema->SetUpdateableSchemaStandards(options.GetUpdateableSchemaStandards());
}

void SchemaRewriter::RewriteForDisableFields(const IndexPartitionSchemaPtr& schema,
                                             const IndexPartitionOptions& options)
{
    if (schema->GetTableType() != tt_index) {
        return;
    }
    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    if (!attrSchema) {
        return;
    }
    AttributeSchemaPtr subAttrSchema;
    const SummarySchemaPtr& summarySchema = schema->GetSummarySchema();
    SummarySchemaPtr subSummarySchema;
    if (schema->GetSubIndexPartitionSchema()) {
        subAttrSchema = schema->GetSubIndexPartitionSchema()->GetAttributeSchema();
        subSummarySchema = schema->GetSubIndexPartitionSchema()->GetSummarySchema();
    }

    for (const string& attribute : options.GetOnlineConfig().GetDisableFieldsConfig().attributes) {
        if ((summarySchema && summarySchema->GetSummaryConfig(attribute)) ||
            (subSummarySchema && subSummarySchema->GetSummaryConfig(attribute))) {
            IE_LOG(WARN, "disable attribute [%s] failed for it is in summary schema", attribute.c_str());
            continue;
        }
        if (!attrSchema->DisableAttribute(attribute) && subAttrSchema && !subAttrSchema->DisableAttribute(attribute)) {
            INDEXLIB_FATAL_ERROR(Schema, "disable attribute [%s] not found in schema", attribute.c_str());
        }
        IE_LOG(INFO, "disable attribute [%s]", attribute.c_str());
    }
    for (const string& packAttribute : options.GetOnlineConfig().GetDisableFieldsConfig().packAttributes) {
        if ((summarySchema && summarySchema->GetSummaryConfig(packAttribute)) ||
            (subSummarySchema && subSummarySchema->GetSummaryConfig(packAttribute))) {
            IE_LOG(WARN, "disable pack attribute [%s] failed for it is in summary schema", packAttribute.c_str());
            continue;
        }
        auto packAttrConfig = attrSchema->GetPackAttributeConfig(packAttribute);
        bool isSubAttributeInSummary = false;
        if (packAttrConfig) {
            for (const auto& attrConfig : packAttrConfig->GetAttributeConfigVec()) {
                const string& attribute = attrConfig->GetAttrName();
                if ((summarySchema && summarySchema->GetSummaryConfig(attribute)) ||
                    (subSummarySchema && subSummarySchema->GetSummaryConfig(attribute))) {
                    IE_LOG(WARN, "disable pack attribute [%s] failed for inner attribute [%s] is in summary schema",
                           packAttribute.c_str(), attribute.c_str());
                    isSubAttributeInSummary = true;
                    break;
                }
            }
        }
        if (isSubAttributeInSummary) {
            continue;
        }
        if (!attrSchema->DisablePackAttribute(packAttribute) && subAttrSchema &&
            !subAttrSchema->DisablePackAttribute(packAttribute)) {
            INDEXLIB_FATAL_ERROR(Schema, "disable pack attribute [%s] not found in schema", packAttribute.c_str());
        }
        IE_LOG(INFO, "disable pack attribute [%s]", packAttribute.c_str());
    }
}

void SchemaRewriter::RewriteForDisableIndexs(const IndexPartitionSchemaPtr& schema,
                                             const IndexPartitionOptions& options)
{
    if (schema->GetTableType() != tt_index) {
        return;
    }
    auto& indexSchema = schema->GetIndexSchema();
    assert(indexSchema);
    IndexSchemaPtr subIndexSchema =
        schema->GetSubIndexPartitionSchema() ? schema->GetSubIndexPartitionSchema()->GetIndexSchema() : nullptr;
    for (const string& index : options.GetOnlineConfig().GetDisableFieldsConfig().indexes) {
        if (indexSchema->GetIndexConfig(index)) {
            try {
                indexSchema->DisableIndex(index);
                IE_LOG(INFO, "disable index [%s]", index.c_str());
            } catch (const util::ExceptionBase& e) {
                IE_LOG(ERROR, "rewrite index[%s] exception:[%s].", index.c_str(), e.what());
            }
        } else if (subIndexSchema && subIndexSchema->GetIndexConfig(index)) {
            try {
                subIndexSchema->DisableIndex(index);
                IE_LOG(INFO, "disable sub index [%s]", index.c_str());
            } catch (const util::ExceptionBase& e) {
                IE_LOG(ERROR, "rewrite sub index[%s] exception:[%s].", index.c_str(), e.what());
            }
        } else {
            IE_LOG(ERROR, "rewrite index[%s] failed, index not exist", index.c_str());
        }
    }
}

void SchemaRewriter::RewriteForEnableBloomFilter(const IndexPartitionSchemaPtr& schema,
                                                 const IndexPartitionOptions& options)
{
    if (schema->GetTableType() != tt_index) {
        return;
    }
    auto& indexSchema = schema->GetIndexSchema();
    assert(indexSchema);
    const vector<IndexDictionaryBloomFilterParam>& params = options.GetOnlineConfig().GetIndexDictBloomFilterParams();
    for (auto& param : params) {
        try {
            indexSchema->EnableBloomFilterForIndex(param.indexName, param.multipleNum);
            IE_LOG(INFO, "Enable bloom filter for index [%s]", param.indexName.c_str());
        } catch (const util::ExceptionBase& e) {
            IE_LOG(ERROR, "rewrite index[%s] exception:[%s].", param.indexName.c_str(), e.what());
        }
    }
}

void SchemaRewriter::RewriteForTruncateIndexConfigs(const IndexPartitionSchemaPtr& schema,
                                                    const IndexPartitionOptions& options, const DirectoryPtr& rootDir)
{
    if (schema->GetTableType() != tt_index) {
        return;
    }

    if (schema->GetRegionCount() > 1) {
        INDEXLIB_FATAL_ERROR(UnSupported, "index table only support default region!");
    }

    if (options.IsOnline() || !options.GetOfflineConfig().NeedRebuildTruncateIndex()) {
        const DirectoryPtr& truncateMetaDir = rootDir->GetDirectory(string(TRUNCATE_META_DIR_NAME), false);
        if (truncateMetaDir) {
            const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
            indexSchema->LoadTruncateTermInfo(truncateMetaDir);
        }
    }

    TruncateOptionConfigPtr truncOptionConfig = options.GetMergeConfig().truncateOptionConfig;
    if (!truncOptionConfig) {
        return;
    }
    truncOptionConfig->Init(schema);
}

void SchemaRewriter::UnifyKVIndexConfig(const IndexPartitionSchemaPtr& schema,
                                        const IndexFormatVersion& indexFormatVersion)
{
    const json::JsonMap& globalJsonMap = schema->GetGlobalRegionIndexPreference();
    if (!globalJsonMap.empty()) {
        Jsonizable::JsonWrapper jsonWrapper(globalJsonMap);
        KVIndexPreference globalPreference;
        globalPreference.Jsonize(jsonWrapper);
        for (regionid_t id = 0; id < (regionid_t)schema->GetRegionCount(); id++) {
            const SingleFieldIndexConfigPtr& pkConfig = schema->GetIndexSchema(id)->GetPrimaryKeyIndexConfig();
            KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, pkConfig);
            assert(kvConfig);
            kvConfig->SetIndexPreference(globalPreference);
        }
    }

    for (regionid_t id = 0; id < (regionid_t)schema->GetRegionCount(); id++) {
        const SingleFieldIndexConfigPtr& pkConfig = schema->GetIndexSchema(id)->GetPrimaryKeyIndexConfig();
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, pkConfig);
        assert(kvConfig);

        ValueConfigPtr valueConfig = kvConfig->GetValueConfig();
        KVIndexPreference& indexPreference = kvConfig->GetIndexPreference();
        KVIndexPreference::HashDictParam dictParam = indexPreference.GetHashDictParam();
        // compatible with legacy format packAttribute
        if (indexFormatVersion <= IndexFormatVersion("2.1.0")) {
            IE_LOG(INFO, "rewrite legacy schema for kv table, "
                         "will not EnableCompactFormat, "
                         "and disable enable_compact_hash_key & enable_shorten_offset");
            valueConfig->EnableCompactFormat(false);
            dictParam.SetEnableCompactHashKey(false);
            dictParam.SetEnableShortenOffset(false);
            indexPreference.SetHashDictParam(dictParam);
        } else {
            valueConfig->EnableCompactFormat(true);
        }
    }
}

void SchemaRewriter::UnifyKKVIndexPreference(const IndexPartitionSchemaPtr& schema)
{
    // rewrite by global index preference
    json::JsonMap globalJsonMap = schema->GetGlobalRegionIndexPreference();
    globalJsonMap.erase(KKV_BUILD_PROTECTION_THRESHOLD);
    if (!globalJsonMap.empty()) {
        Jsonizable::JsonWrapper jsonWrapper(globalJsonMap);
        KKVIndexPreference globalPreference;
        globalPreference.Jsonize(jsonWrapper);
        for (regionid_t id = 0; id < (regionid_t)schema->GetRegionCount(); id++) {
            const SingleFieldIndexConfigPtr& pkConfig = schema->GetIndexSchema(id)->GetPrimaryKeyIndexConfig();
            KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, pkConfig);
            assert(kkvConfig);
            kkvConfig->SetIndexPreference(globalPreference);
        }
        IE_LOG(INFO, "rewrite kkv index preference by global_region_index_prefernce!");
    }
}

void SchemaRewriter::UnifyKKVBuildParam(const IndexPartitionSchemaPtr& schema,
                                        const IndexFormatVersion& indexFormatVersion)
{
    uint32_t buildProtectionThreshold = numeric_limits<uint32_t>::max();
    uint32_t skipListThreshold = numeric_limits<uint32_t>::max();
    for (regionid_t id = 0; id < (regionid_t)schema->GetRegionCount(); id++) {
        const SingleFieldIndexConfigPtr& pkConfig = schema->GetIndexSchema(id)->GetPrimaryKeyIndexConfig();
        KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, pkConfig);
        assert(kkvConfig);
        buildProtectionThreshold = min(buildProtectionThreshold, kkvConfig->GetSuffixKeyProtectionThreshold());
        skipListThreshold = min(skipListThreshold, kkvConfig->GetSuffixKeySkipListThreshold());
    }

    for (regionid_t id = 0; id < (regionid_t)schema->GetRegionCount(); id++) {
        const SingleFieldIndexConfigPtr& pkConfig = schema->GetIndexSchema(id)->GetPrimaryKeyIndexConfig();
        KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, pkConfig);
        assert(kkvConfig);
        kkvConfig->SetSuffixKeyProtectionThreshold(buildProtectionThreshold);
        kkvConfig->SetSuffixKeySkipListThreshold(skipListThreshold);
        ValueConfigPtr valueConfig = kkvConfig->GetValueConfig();
        if (indexFormatVersion <= IndexFormatVersion("2.1.1")) {
            IE_LOG(INFO, "rewrite legacy schema for kkv table, "
                         "will not EnableCompactFormat.");
            valueConfig->EnableCompactFormat(false);
        } else {
            valueConfig->EnableCompactFormat(true);
        }
    }
    IE_LOG(INFO, "kkv table use unified build_protection_threshold [%u]", buildProtectionThreshold);
    IE_LOG(INFO, "kkv table use unified skip_list_threshold [%u]", skipListThreshold);
}

bool SchemaRewriter::GetRewriteBuildProtectionThreshold(const IndexPartitionSchemaPtr& schema,
                                                        const IndexPartitionOptions& options,
                                                        uint32_t& buildProtectThreshold)
{
    if (schema->GetTableType() != tt_kkv) {
        return false;
    }
    if (options.IsOnline()) {
        const KVOnlineConfig& kvOnlineConfig = options.GetOnlineConfig().kvOnlineConfig;
        if (kvOnlineConfig.buildProtectThreshold != KVOnlineConfig::INVALID_COUNT_LIMITS) {
            buildProtectThreshold = kvOnlineConfig.buildProtectThreshold;
            return true;
        }
    }
    const json::JsonMap& globalMap = schema->GetGlobalRegionIndexPreference();
    json::JsonMap::const_iterator iter = globalMap.find(KKV_BUILD_PROTECTION_THRESHOLD);
    if (iter != globalMap.end()) {
        buildProtectThreshold = json::JsonNumberCast<uint32_t>(iter->second);
        return true;
    }
    return false;
}

void SchemaRewriter::RewriteFieldType(const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options)
{
    if (!options.NeedRewriteFieldType()) {
        return;
    }
    const FieldSchemaPtr& fieldSchema = schema->GetFieldSchema();
    assert(fieldSchema);
    fieldSchema->RewriteFieldType();

    const IndexPartitionSchemaPtr& subSchema = schema->GetSubIndexPartitionSchema();
    if (!subSchema) {
        return;
    }
    const FieldSchemaPtr& subFieldSchema = subSchema->GetFieldSchema();
    assert(subFieldSchema);
    subFieldSchema->RewriteFieldType();
}

}} // namespace indexlib::index_base
