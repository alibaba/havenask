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
#include "indexlib/config/impl/region_schema_impl.h"

#include <algorithm>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/attribute_config_creator.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/field_config_loader.h"
#include "indexlib/config/impl/index_partition_schema_impl.h"
#include "indexlib/config/index_config_creator.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/config/spatial_index_config.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil;

using namespace indexlib::common;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, RegionSchemaImpl);

RegionSchemaImpl::RegionSchemaImpl(IndexPartitionSchemaImpl* schema, bool multiRegionFormat)
    : mSchema(schema)
    , mRegionName(DEFAULT_REGIONNAME)
    , mTTLFieldName(DOC_TIME_TO_LIVE_IN_SECONDS)
    , mDefaultTTL(INVALID_TTL)
    , mEnableTemperatureLayer(false)
    , mHashIdFieldName(DEFAULT_HASH_ID_FIELD_NAME)
    , mOwnFieldSchema(false)
    , mMultiRegionFormat(multiRegionFormat)
    , mTTLFromDoc(false)
{
    assert(mSchema);
    if (schema->GetTableType() == tt_rawfile || schema->GetTableType() == tt_linedata) {
        mIndexSchema.reset(new IndexSchema);
        mSummarySchema.reset(new SummarySchema);
    }
    if (schema->GetTableType() == tt_index) {
        mTTLFromDoc = true;
    }
    mFieldSchema = schema->GetFieldSchema();
}

RegionSchemaImpl::~RegionSchemaImpl() {}

void RegionSchemaImpl::AddIndexConfig(const IndexConfigPtr& indexConfig)
{
    if (!mIndexSchema) {
        mIndexSchema.reset(new IndexSchema);
    }
    mIndexSchema->AddIndexConfig(indexConfig);
}

void RegionSchemaImpl::AddAttributeConfig(const AttributeConfigPtr& attrConfig)
{
    if (!mAttributeSchema) {
        mAttributeSchema.reset(new AttributeSchema);
    }
    mAttributeSchema->AddAttributeConfig(attrConfig);
}

void RegionSchemaImpl::AddAttributeConfig(const string& fieldName, const CustomizedConfigVector& customizedConfigs)
{
    AttributeConfigPtr attrConfig = AttributeConfigCreator::Create(mFieldSchema, fieldName, customizedConfigs);
    AddAttributeConfig(attrConfig);
}

void RegionSchemaImpl::AddAttributeConfig(const autil::legacy::Any& any)
{
    AttributeConfigPtr attributeConfig = AttributeConfigCreator::Create(mFieldSchema, mFileCompressSchema, any);
    AddAttributeConfig(attributeConfig);
}

void RegionSchemaImpl::AddVirtualAttributeConfig(const AttributeConfigPtr& virtualAttrConfig)
{
    AttributeConfigPtr attrConfig = CreateVirtualAttributeConfig(virtualAttrConfig->GetFieldConfig(),
                                                                 virtualAttrConfig->GetAttrValueInitializerCreator());
    assert(attrConfig);
    if (!mVirtualAttributeSchema) {
        mVirtualAttributeSchema.reset(new AttributeSchema);
    }
    mVirtualAttributeSchema->AddAttributeConfig(attrConfig);
}

void RegionSchemaImpl::AddPackAttributeConfig(const string& attrName, const vector<string>& subAttrNames,
                                              const string& compressTypeStr, uint64_t defragSlicePercent,
                                              const std::shared_ptr<FileCompressConfig>& fileCompressConfig,
                                              const string& valueFormat)
{
    CompressTypeOption compressOption;
    THROW_IF_STATUS_ERROR(compressOption.Init(compressTypeStr));

    PackAttributeConfigPtr packAttrConfig(
        new PackAttributeConfig(attrName, compressOption, defragSlicePercent, fileCompressConfig));
    for (size_t i = 0; i < subAttrNames.size(); ++i) {
        AttributeConfigPtr attrConfig = CreateAttributeConfig(subAttrNames[i]);
        auto status = packAttrConfig->AddAttributeConfig(attrConfig);
        THROW_IF_STATUS_ERROR(status);
    }

    if (valueFormat == indexlibv2::index::PackAttributeConfig::VALUE_FORMAT_IMPACT) {
        packAttrConfig->EnableImpact();
    } else if (valueFormat == indexlibv2::index::PackAttributeConfig::VALUE_FORMAT_PLAIN) {
        packAttrConfig->EnablePlainFormat();
    }

    if (!mAttributeSchema) {
        mAttributeSchema.reset(new AttributeSchema);
    }
    mAttributeSchema->AddPackAttributeConfig(packAttrConfig);
}

void RegionSchemaImpl::AddSummaryConfig(const string& fieldName, summarygroupid_t summaryGroupId)
{
    FieldConfigPtr fieldConfig = mFieldSchema->GetFieldConfig(fieldName);
    if (!fieldConfig) {
        stringstream msg;
        msg << "No such field defined: fieldName:" << fieldName;
        INDEXLIB_FATAL_ERROR(Schema, "%s", msg.str().c_str());
    }
    if (!mSummarySchema) {
        mSummarySchema.reset(new SummarySchema);
    }
    std::shared_ptr<SummaryConfig> summaryConfig(new SummaryConfig);
    summaryConfig->SetFieldConfig(fieldConfig);
    mSummarySchema->AddSummaryConfig(summaryConfig, summaryGroupId);

    if (fieldConfig->GetFieldType() == ft_timestamp) {
        if (!mAttributeSchema || !mAttributeSchema->GetAttributeConfig(fieldName)) {
            IE_LOG(INFO, "inner add timestamp type attribute [%s] to support storage in summary", fieldName.c_str());
            AddAttributeConfig(fieldName);
            AttributeConfigPtr attrConfig = mAttributeSchema->GetAttributeConfig(fieldName);
            assert(attrConfig);
            attrConfig->SetConfigType(AttributeConfig::ct_summary_accompany);
        }
    }
}

void RegionSchemaImpl::AddSummaryConfig(fieldid_t fieldId, summarygroupid_t summaryGroupId)
{
    FieldConfigPtr fieldConfig = mFieldSchema->GetFieldConfig(fieldId);
    if (!fieldConfig) {
        stringstream msg;
        msg << "No such field defined: fieldId:" << fieldId;
        INDEXLIB_FATAL_ERROR(Schema, "%s", msg.str().c_str());
    }
    if (!mSummarySchema) {
        mSummarySchema.reset(new SummarySchema);
    }
    std::shared_ptr<SummaryConfig> summaryConfig(new SummaryConfig);
    summaryConfig->SetFieldConfig(fieldConfig);
    mSummarySchema->AddSummaryConfig(summaryConfig, summaryGroupId);
}

void RegionSchemaImpl::SetSummaryCompress(bool compress, const string& compressType, summarygroupid_t summaryGroupId)
{
    if (!mSummarySchema) {
        mSummarySchema.reset(new SummarySchema);
    }
    const SummaryGroupConfigPtr summaryGroupConfig = mSummarySchema->GetSummaryGroupConfig(summaryGroupId);
    assert(summaryGroupConfig);
    summaryGroupConfig->SetCompress(compress, compressType);
}

summarygroupid_t RegionSchemaImpl::CreateSummaryGroup(const string& summaryGroupName)
{
    assert(mSummarySchema);
    return mSummarySchema->CreateSummaryGroup(summaryGroupName);
}

AttributeConfigPtr RegionSchemaImpl::CreateAttributeConfig(const string& fieldName,
                                                           const CustomizedConfigVector& customizedConfigs)
{
    FieldConfigPtr fieldConfig = mFieldSchema->GetFieldConfig(fieldName);
    if (!fieldConfig) {
        INDEXLIB_FATAL_ERROR(Schema, "No such field defined: fieldName:%s", fieldName.c_str());
    }
    AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
    attrConfig->Init(fieldConfig, AttributeValueInitializerCreatorPtr(), customizedConfigs);
    return attrConfig;
}

AttributeConfigPtr RegionSchemaImpl::CreateVirtualAttributeConfig(const FieldConfigPtr& fieldConfig,
                                                                  const AttributeValueInitializerCreatorPtr& creator)
{
    FieldConfigPtr cloneFieldConfig(fieldConfig->Clone());
    cloneFieldConfig->SetFieldId(GetFieldIdForVirtualAttribute());
    const string& fieldName = cloneFieldConfig->GetFieldName();
    if (mAttributeSchema && mAttributeSchema->GetAttributeConfig(fieldName)) {
        INDEXLIB_FATAL_ERROR(Schema, "virtual attribute name[%s] already in attributeSchema", fieldName.c_str());
    }

    AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_virtual));
    attrConfig->Init(cloneFieldConfig, creator);
    return attrConfig;
}

fieldid_t RegionSchemaImpl::GetFieldIdForVirtualAttribute() const
{
    fieldid_t fieldId = (fieldid_t)mFieldSchema->GetFieldCount();
    if (mVirtualAttributeSchema) {
        fieldId += mVirtualAttributeSchema->GetAttributeCount();
    }
    return fieldId;
}

void RegionSchemaImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        ToJson(json);
    } else {
        FromJson(json);
        LoadValueConfig();
        InitTruncateIndexConfigs();
        EnsureSpatialIndexWithAttribute();
    }
}

bool RegionSchemaImpl::TTLEnabled() const
{
    TableType tableType = mSchema->GetTableType();
    if (tableType == tt_kkv || tableType == tt_kv) {
        return mDefaultTTL != INVALID_TTL;
    }
    if (mAttributeSchema) {
        AttributeConfigPtr attrConfig = mAttributeSchema->GetAttributeConfig(mTTLFieldName);
        if (attrConfig && attrConfig->GetFieldType() == ft_uint32 && !attrConfig->IsMultiValue()) {
            return true;
        }
    }
    return false;
}

int64_t RegionSchemaImpl::GetDefaultTTL() const { return mDefaultTTL >= 0 ? mDefaultTTL : DEFAULT_TIME_TO_LIVE; }

bool RegionSchemaImpl::HashIdEnabled() const
{
    TableType tableType = mSchema->GetTableType();
    if (tableType == tt_kkv || tableType == tt_kv || tableType == tt_customized) {
        return false;
    }
    if (mAttributeSchema) {
        AttributeConfigPtr attrConfig = mAttributeSchema->GetAttributeConfig(mHashIdFieldName);
        if (attrConfig && attrConfig->GetFieldType() == ft_uint16 && !attrConfig->IsMultiValue()) {
            return true;
        }
    }
    return false;
}

void RegionSchemaImpl::LoadValueConfig()
{
    TableType tableType = mSchema->GetTableType();
    if (tableType != tt_kv && tableType != tt_kkv) {
        return;
    }

    if (!mAttributeSchema || mAttributeSchema->GetPackAttributeCount() > 0) {
        INDEXLIB_FATAL_ERROR(Schema, "%s region not support pack attribute", mRegionName.c_str());
    }

    if (!mIndexSchema) {
        INDEXLIB_FATAL_ERROR(Schema, "no index schema!");
    }
    SingleFieldIndexConfigPtr singleConfig = mIndexSchema->GetPrimaryKeyIndexConfig();
    vector<AttributeConfigPtr> attrConfigs;
    AttributeSchema::Iterator iter = mAttributeSchema->Begin();
    for (; iter != mAttributeSchema->End(); ++iter) {
        AttributeConfigPtr attrConfig = CreateAttributeConfig((*iter)->GetAttrName());
        attrConfig->SetAttrId((*iter)->GetAttrId());
        attrConfigs.push_back(attrConfig);
    }

    ValueConfigPtr valueConfig(new ValueConfig());
    if (tableType == tt_kkv) {
        KKVIndexConfigPtr kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, singleConfig);
        if (!kkvIndexConfig) {
            INDEXLIB_FATAL_ERROR(Schema, "index type [%s] not match with kkv table!",
                                 IndexConfig::InvertedIndexTypeToStr(singleConfig->GetInvertedIndexType()));
        }
        if (kkvIndexConfig->GetSuffixFieldInfo().enableStoreOptimize) {
            OptimizeKKVSKeyStore(kkvIndexConfig, attrConfigs);
        }
        valueConfig->Init(attrConfigs);
        valueConfig->EnableValueImpact(kkvIndexConfig->GetIndexPreference().GetValueParam().IsValueImpact());
        valueConfig->EnablePlainFormat(kkvIndexConfig->GetIndexPreference().GetValueParam().IsPlainFormat());
        kkvIndexConfig->SetValueConfig(valueConfig);
        return;
    }

    KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, singleConfig);
    if (!kvIndexConfig) {
        INDEXLIB_FATAL_ERROR(Schema, "index type [%s] not match with table type!",
                             IndexConfig::InvertedIndexTypeToStr(singleConfig->GetInvertedIndexType()));
    }
    valueConfig->Init(attrConfigs);
    valueConfig->EnableValueImpact(kvIndexConfig->GetIndexPreference().GetValueParam().IsValueImpact());
    valueConfig->EnablePlainFormat(kvIndexConfig->GetIndexPreference().GetValueParam().IsPlainFormat());
    if (mSchema->GetSchemaVersionId() != DEFAULT_SCHEMAID) {
        valueConfig->DisableSimpleValue();
    }
    kvIndexConfig->SetValueConfig(valueConfig);
}

void RegionSchemaImpl::OptimizeKKVSKeyStore(const KKVIndexConfigPtr& kkvIndexConfig,
                                            vector<AttributeConfigPtr>& attrConfigs)
{
    if (kkvIndexConfig->GetSuffixHashFunctionType() == hft_murmur) {
        return;
    }
    const FieldConfigPtr& skeyFieldConfig = kkvIndexConfig->GetSuffixFieldConfig();
    vector<AttributeConfigPtr> optAttrConfigs;
    for (size_t i = 0; i < attrConfigs.size(); i++) {
        if (attrConfigs[i]->GetAttrName() == skeyFieldConfig->GetFieldName()) {
            continue;
        }
        optAttrConfigs.push_back(attrConfigs[i]);
    }
    // TODO: ValueConfig support empty attribute configs
    if (optAttrConfigs.empty()) {
        return;
    }
    attrConfigs.swap(optAttrConfigs);
    kkvIndexConfig->SetOptimizedStoreSKey(true);
}

void RegionSchemaImpl::ResolveEmptyProfileNamesForTruncateIndex()
{
    std::vector<std::string> usingTruncateNames;
    for (auto iter = mTruncateProfileSchema->Begin(); iter != mTruncateProfileSchema->End(); iter++) {
        if (iter->second->GetPayloadConfig().IsInitialized()) {
            continue;
        }
        usingTruncateNames.push_back(iter->first);
    }
    for (auto it = mIndexSchema->Begin(); it != mIndexSchema->End(); ++it) {
        const IndexConfigPtr& indexConfig = *it;
        if (indexConfig->GetShardingType() == IndexConfig::IST_IS_SHARDING || !indexConfig->HasTruncate()) {
            continue;
        }
        auto useTruncateProfiles = indexConfig->GetUseTruncateProfiles();
        if (useTruncateProfiles.size() == 0) {
            indexConfig->SetUseTruncateProfiles(usingTruncateNames);
        }
    }
}

void RegionSchemaImpl::InitTruncateIndexConfigs()
{
    if (!mTruncateProfileSchema || mTruncateProfileSchema->Size() == 0) {
        return;
    }

    if (!mIndexSchema) {
        INDEXLIB_FATAL_ERROR(Schema, "no index schema!");
    }
    ResolveEmptyProfileNamesForTruncateIndex();
    IndexSchema::Iterator it = mIndexSchema->Begin();
    for (; it != mIndexSchema->End(); ++it) {
        const IndexConfigPtr& indexConfig = *it;
        if (indexConfig->GetShardingType() == IndexConfig::IST_IS_SHARDING || !indexConfig->HasTruncate()) {
            continue;
        }

        TruncateProfileSchema::Iterator pt = mTruncateProfileSchema->Begin();
        for (; pt != mTruncateProfileSchema->End(); ++pt) {
            const string& profileName = pt->first;
            const TruncateProfileConfigPtr& truncateProfileConfig =
                mTruncateProfileSchema->GetTruncateProfileConfig(profileName);
            if (indexConfig->HasTruncateProfile(truncateProfileConfig.get())) {
                IndexConfigPtr truncateIndexConfig = CreateTruncateIndexConfig(indexConfig, truncateProfileConfig);
                AddIndexConfig(truncateIndexConfig);
                UpdateIndexConfigForTruncate(indexConfig.get(), truncateIndexConfig.get());
            }
        }
    }
}

// IndexConfig needs to be modified if it has TruncateIndexConfig that uses payload. This is a compatibility change
// for IndexConfig to support payload. Future design of IndexConfig should support paylaod natively.
void RegionSchemaImpl::UpdateIndexConfigForTruncate(IndexConfig* indexConfig, IndexConfig* truncateIndexConfig)
{
    const std::string& existingPayloadName = indexConfig->GetTruncatePayloadConfig().GetName();
    const std::string& incomingPayloadName = truncateIndexConfig->GetTruncatePayloadConfig().GetName();

    if (!indexConfig->GetTruncatePayloadConfig().IsInitialized()) {
        indexConfig->SetTruncatePayloadConfig(truncateIndexConfig->GetTruncatePayloadConfig());
        return;
    }
    if (truncateIndexConfig->GetTruncatePayloadConfig().IsInitialized() and
        existingPayloadName != incomingPayloadName) {
        INDEXLIB_FATAL_ERROR(Schema, "Index [%s] has different truncate payload [%s] and [%s]",
                             indexConfig->GetIndexName().c_str(), existingPayloadName.c_str(),
                             incomingPayloadName.c_str());
    }
}

// For any index config, it might produce multiple TruncateIndexConfig(s) if it has multiple truncate profiles. Each
// TruncateIndexConfig will have its own separate IndexConfig. Such TruncateIndexConfig should have all properties that
// a normal IndexConfig has.
IndexConfigPtr RegionSchemaImpl::CreateTruncateIndexConfig(const IndexConfigPtr& indexConfig,
                                                           const TruncateProfileConfigPtr& truncateProfileConfig)
{
    assert(indexConfig);
    const string& indexName = indexConfig->GetIndexName();
    const string& profileName = truncateProfileConfig->GetTruncateProfileName();
    IndexConfigPtr truncateIndexConfig(indexConfig->Clone());
    std::string newIndexName = IndexConfig::CreateTruncateIndexName(indexName, profileName);
    truncateIndexConfig->SetIndexName(newIndexName);
    truncateIndexConfig->SetVirtual(true);
    truncateIndexConfig->SetNonTruncateIndexName(indexName);
    truncateIndexConfig->SetFileCompressConfig(indexConfig->GetFileCompressConfig());
    truncateIndexConfig->SetHasTruncateFlag(false);
    truncateIndexConfig->SetTruncatePayloadConfig(truncateProfileConfig->GetPayloadConfig());

    if (indexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING) {
        const vector<IndexConfigPtr>& shardingIndexConfigs = indexConfig->GetShardingIndexConfigs();
        for (size_t i = 0; i < shardingIndexConfigs.size(); ++i) {
            IndexConfigPtr truncShardingIndexConfig =
                CreateTruncateIndexConfig(shardingIndexConfigs[i], truncateProfileConfig);
            UpdateIndexConfigForTruncate(shardingIndexConfigs[i].get(), truncShardingIndexConfig.get());
            truncateIndexConfig->AppendShardingIndexConfig(truncShardingIndexConfig);
        }
    }
    return truncateIndexConfig;
}

bool RegionSchemaImpl::IsUsefulField(const string& fieldName) const
{
    fieldid_t fieldId = mFieldSchema->GetFieldId(fieldName);
    if (fieldId == INVALID_FIELDID) {
        return false;
    }
    return (mIndexSchema && mIndexSchema->IsInIndex(fieldId)) ||
           (mSummarySchema && mSummarySchema->IsInSummary(fieldId)) ||
           (mAttributeSchema && mAttributeSchema->IsInAttribute(fieldId));
}

void RegionSchemaImpl::ToJson(Jsonizable::JsonWrapper& json)
{
    if (mOwnFieldSchema) {
        // mFieldSchema
        if (mFieldSchema) {
            Any any = autil::legacy::ToJson(*mFieldSchema);
            JsonMap fieldMap = AnyCast<JsonMap>(any);
            json.Jsonize(FIELDS, fieldMap[FIELDS]);
        }
    }

    // mFileCompressSchema
    if (mFileCompressSchema) {
        Any any = autil::legacy::ToJson(*mFileCompressSchema);
        JsonMap compressMap = AnyCast<JsonMap>(any);
        json.Jsonize(FILE_COMPRESS, compressMap[FILE_COMPRESS]);
    }

    // mIndexSchema
    if (mIndexSchema) {
        Any any = autil::legacy::ToJson(*mIndexSchema);
        JsonMap indexMap = AnyCast<JsonMap>(any);
        json.Jsonize(INDEXS, indexMap[INDEXS]);
    }
    // mAttributeSchema
    if (mAttributeSchema) {
        Any any = autil::legacy::ToJson(*mAttributeSchema);
        JsonMap attributeMap = AnyCast<JsonMap>(any);
        json.Jsonize(ATTRIBUTES, attributeMap[ATTRIBUTES]);
    }
    // mSummarySchema
    if (mSummarySchema) {
        Any any = autil::legacy::ToJson(*mSummarySchema);
        JsonMap summaryMap = AnyCast<JsonMap>(any);
        json.Jsonize(SUMMARYS, summaryMap[SUMMARYS]);
    }

    // mSourceSchema
    if (mSourceSchema) {
        Any any = autil::legacy::ToJson(*mSourceSchema);
        json.Jsonize(SOURCE, any);
    }

    // mTruncateProfileSchema
    if (mTruncateProfileSchema) {
        Any any = autil::legacy::ToJson(*mTruncateProfileSchema);
        JsonMap truncateProfileMap = AnyCast<JsonMap>(any);
        json.Jsonize(TRUNCATE_PROFILES, truncateProfileMap[TRUNCATE_PROFILES]);
    }
    if (TTLEnabled()) {
        bool enableTTL = true;
        json.Jsonize(ENABLE_TTL, enableTTL);
        if (mDefaultTTL != INVALID_TTL) {
            json.Jsonize(DEFAULT_TTL, mDefaultTTL);
        }
        if (mTTLFieldName != DOC_TIME_TO_LIVE_IN_SECONDS) {
            json.Jsonize(TTL_FIELD_NAME, mTTLFieldName);
        }
        json.Jsonize(TTL_FROM_DOC, mTTLFromDoc);
    }

    if (HashIdEnabled()) {
        bool enableHashId = true;
        json.Jsonize(ENABLE_HASH_ID, enableHashId);
        if (mHashIdFieldName != DEFAULT_HASH_ID_FIELD_NAME) {
            json.Jsonize(HASH_ID_FIELD_NAME, mHashIdFieldName);
        }
    }

    if (!mOrderPreservingField.empty()) {
        json.Jsonize(ORDER_PRESERVING_FIELD, mOrderPreservingField);
    }

    if (mRegionName != DEFAULT_REGIONNAME) {
        json.Jsonize(REGION_NAME, mRegionName);
    }

    json.Jsonize(ENABLE_TEMPERATURE_LAYER, mEnableTemperatureLayer, mEnableTemperatureLayer);
    if (mEnableTemperatureLayer) {
        json.Jsonize("temperature_layer_config", mTemperatureLayer);
    }
}

void RegionSchemaImpl::FromJson(Jsonizable::JsonWrapper& json)
{
    std::map<std::string, Any> jsonMap = json.GetMap();

    // parse fieldSchema
    auto iter = jsonMap.find(FIELDS);
    if (iter != jsonMap.end()) {
        LoadFieldSchema(iter->second);
    } else {
        mFieldSchema = mSchema->GetFieldSchema();
    }

    if (!mFieldSchema) {
        if (mSchema->GetTableType() == tt_customized) {
            mFieldSchema.reset(new FieldSchema);
        } else {
            INDEXLIB_FATAL_ERROR(Schema, "no fields section defined");
        }
    }

    iter = jsonMap.find(FILE_COMPRESS);
    if (iter != jsonMap.end()) {
        LoadFileCompressSchema(iter->second);
    }

    // parse truncate_profiles definition
    iter = jsonMap.find(TRUNCATE_PROFILES);
    if (iter != jsonMap.end()) {
        LoadTruncateProfileSchema(jsonMap);
    }

    // parse indexs
    iter = jsonMap.find(INDEXS);
    if (iter != jsonMap.end()) {
        LoadIndexSchema(iter->second);
    } else {
        IE_LOG(DEBUG, "no indexs section defined");
    }

    if (!mIndexSchema) {
        if (mSchema->GetTableType() == tt_kv || mSchema->GetTableType() == tt_kkv) {
            INDEXLIB_FATAL_ERROR(Schema, "no index schema!");
        }
    }

    // parse attribute
    iter = jsonMap.find(ATTRIBUTES);
    if (iter != jsonMap.end()) {
        LoadAttributeSchema(iter->second);
    } else {
        IE_LOG(DEBUG, "no attributes section defined");
    }

    // parse summary
    iter = jsonMap.find(SUMMARYS);
    if (iter != jsonMap.end()) {
        LoadSummarySchema(iter->second);
    }
    SetNeedStoreSummary();

    iter = jsonMap.find(SOURCE);
    if (iter != jsonMap.end()) {
        LoadSourceSchema(iter->second);
    }

    // parse ttl
    json.Jsonize(TTL_FROM_DOC, mTTLFromDoc, mTTLFromDoc);
    json.Jsonize(TTL_FIELD_NAME, mTTLFieldName, mTTLFieldName);
    bool enableTTLSet = false;
    bool enableTTL = false;
    int64_t defaultTTL = INVALID_TTL;
    iter = jsonMap.find(ENABLE_TTL);
    if (iter != jsonMap.end()) {
        enableTTLSet = true;
        enableTTL = AnyCast<bool>(iter->second);
    }
    iter = jsonMap.find(DEFAULT_TTL);
    if (iter != jsonMap.end()) {
        defaultTTL = JsonNumberCast<int64_t>(iter->second);
    }
    bool shouldEnableTTL = enableTTLSet ? enableTTL : (defaultTTL >= 0);
    SetEnableTTL(shouldEnableTTL, mTTLFieldName);
    if (shouldEnableTTL) {
        mDefaultTTL = (defaultTTL >= 0) ? defaultTTL : DEFAULT_TIME_TO_LIVE;
        SetDefaultTTL(mDefaultTTL, mTTLFieldName);
    }
    // parse hash_id
    json.Jsonize(HASH_ID_FIELD_NAME, mHashIdFieldName, mHashIdFieldName);
    iter = jsonMap.find(ENABLE_HASH_ID);
    if (iter != jsonMap.end()) {
        bool enableHashId = AnyCast<bool>(iter->second);
        SetEnableHashId(enableHashId, mHashIdFieldName);
    }

    json.Jsonize(ORDER_PRESERVING_FIELD, mOrderPreservingField, mOrderPreservingField);
    SetEnableOrderPreserving();

    json.Jsonize(REGION_NAME, mRegionName, mRegionName);

    // parse temperature config
    json.Jsonize(ENABLE_TEMPERATURE_LAYER, mEnableTemperatureLayer, mEnableTemperatureLayer);
    if (mEnableTemperatureLayer) {
        iter = jsonMap.find(TEMPERATURE_LAYER_CONFIG);
        if (iter != jsonMap.end()) {
            LoadTemperatureConfig(iter->second);
        }
    }
}

void RegionSchemaImpl::LoadTemperatureConfig(const Any& any)
{
    JsonWrapper wrapper(any);
    mTemperatureLayer.reset(new TemperatureLayerConfig);
    mTemperatureLayer->Jsonize(wrapper);
}

void RegionSchemaImpl::LoadFieldSchema(const Any& any)
{
    if (!mMultiRegionFormat) {
        assert(mFieldSchema);
        return;
    }
    JsonArray fields = AnyCast<JsonArray>(any);
    mFieldSchema.reset(new FieldSchema(fields.size()));
    FieldConfigLoader::Load(any, mFieldSchema);
    mOwnFieldSchema = true;
}

void RegionSchemaImpl::LoadTruncateProfileSchema(const Any& any)
{
    mTruncateProfileSchema.reset(new TruncateProfileSchema);
    Jsonizable::JsonWrapper jsonWrapper(any);
    mTruncateProfileSchema->Jsonize(jsonWrapper);
}

void RegionSchemaImpl::LoadIndexSchema(const autil::legacy::Any& any)
{
    JsonArray indexs = AnyCast<JsonArray>(any);
    for (JsonArray::iterator it = indexs.begin(); it != indexs.end(); ++it) {
        IndexConfigPtr indexConfig =
            IndexConfigCreator::Create(mFieldSchema, mSchema->GetDictSchema(), mSchema->GetAdaptiveDictSchema(),
                                       mFileCompressSchema, *it, mSchema->GetTableType(), mSchema->IsLoadFromIndex());
        AddIndexConfig(indexConfig);
    }
}

void RegionSchemaImpl::LoadFileCompressSchema(const autil::legacy::Any& any)
{
    mFileCompressSchema.reset(FileCompressSchema::FromJson(any));
}

void RegionSchemaImpl::LoadAttributeSchema(const Any& any)
{
    JsonArray attrs = AnyCast<JsonArray>(any);
    for (JsonArray::iterator iter = attrs.begin(); iter != attrs.end(); ++iter) {
        if (iter->GetType() == typeid(JsonMap)) {
            LoadAttributeConfig(*iter);
        } else {
            string fieldName = AnyCast<string>(*iter);
            AddAttributeConfig(fieldName);
        }
    }
}

void RegionSchemaImpl::LoadAttributeConfig(const Any& any)
{
    JsonMap attribute = AnyCast<JsonMap>(any);
    auto packNameIter = attribute.find(indexlibv2::index::PackAttributeConfig::PACK_NAME);
    if (packNameIter != attribute.end()) {
        LoadPackAttributeConfig(any);
    } else {
        AddAttributeConfig(any);
    }
}

void RegionSchemaImpl::LoadPackAttributeConfig(const Any& any)
{
    JsonMap packAttr = AnyCast<JsonMap>(any);
    if (packAttr.find(indexlibv2::index::PackAttributeConfig::PACK_NAME) == packAttr.end()) {
        INDEXLIB_FATAL_ERROR(Schema, "pack attribute name undefined.");
    }
    if (packAttr.find(indexlibv2::index::PackAttributeConfig::SUB_ATTRIBUTES) == packAttr.end()) {
        INDEXLIB_FATAL_ERROR(Schema, "sub attribute names undefined.");
    }
    string packName = AnyCast<string>(packAttr[indexlibv2::index::PackAttributeConfig::PACK_NAME]);
    vector<string> subAttrNames;
    JsonArray subAttrs = AnyCast<JsonArray>(packAttr[indexlibv2::index::PackAttributeConfig::SUB_ATTRIBUTES]);
    for (JsonArray::iterator it = subAttrs.begin(); it != subAttrs.end(); ++it) {
        string subAttrName = AnyCast<string>(*it);
        subAttrNames.push_back(subAttrName);
    }
    if (subAttrNames.empty()) {
        INDEXLIB_FATAL_ERROR(Schema, "sub attribute names undefined.");
    }
    string compressType = "";
    if (packAttr.find(index::COMPRESS_TYPE) != packAttr.end()) {
        compressType = AnyCast<string>(packAttr[index::COMPRESS_TYPE]);
    }

    string valueFormat = "";
    if (packAttr.find(indexlibv2::index::PackAttributeConfig::VALUE_FORMAT) != packAttr.end()) {
        valueFormat = AnyCast<string>(packAttr[indexlibv2::index::PackAttributeConfig::VALUE_FORMAT]);
    }

    uint64_t defragSlicePercent = index::ATTRIBUTE_DEFAULT_DEFRAG_SLICE_PERCENT;
    if (packAttr.find(index::ATTRIBUTE_DEFRAG_SLICE_PERCENT) != packAttr.end()) {
        defragSlicePercent = AnyCast<uint64_t>(packAttr[index::ATTRIBUTE_DEFRAG_SLICE_PERCENT]);
    }
    std::shared_ptr<FileCompressConfig> fileCompressConfig;
    if (packAttr.find(FILE_COMPRESS) != packAttr.end()) {
        string fileCompress = AnyCast<string>(packAttr[FILE_COMPRESS]);
        if (mFileCompressSchema) {
            fileCompressConfig = mFileCompressSchema->GetFileCompressConfig(fileCompress);
        }
        if (!fileCompressConfig) {
            INDEXLIB_FATAL_ERROR(Schema, "undefined file compress[%s] in file compress schema", fileCompress.c_str());
        }
    }
    AddPackAttributeConfig(packName, subAttrNames, compressType, defragSlicePercent, fileCompressConfig, valueFormat);
}

bool RegionSchemaImpl::LoadSummaryGroup(const Any& any, summarygroupid_t summaryGroupId)
{
    JsonMap summary = AnyCast<JsonMap>(any);
    JsonMap::const_iterator it = summary.find(SUMMARY_FIELDS);
    if (it == summary.end()) {
        IE_LOG(WARN, "summary can not be empty");
        return false;
    }

    JsonArray summaryFields = AnyCast<JsonArray>(it->second);
    if (summaryFields.size() < 1) {
        IE_LOG(WARN, "summarys can not be empty");
        return false;
    }
    for (JsonArray::iterator iter = summaryFields.begin(); iter != summaryFields.end(); ++iter) {
        string fieldName = AnyCast<string>(*iter);
        AddSummaryConfig(fieldName, summaryGroupId);
    }

    it = summary.find(SUMMARY_GROUP_PARAMTETER);
    if (it != summary.end()) {
        JsonWrapper wrapper(it->second);
        GroupDataParameter param;
        param.Jsonize(wrapper);
        if (param.NeedSyncFileCompressConfig()) {
            auto status = param.SyncFileCompressConfig(mFileCompressSchema);
            THROW_IF_STATUS_ERROR(status);
        }
        SetSummaryGroupDataParam(param, summaryGroupId);
    }

    // if has summary fields, compress default is false
    it = summary.find(SUMMARY_COMPRESS);
    if (it == summary.end()) {
        SetSummaryCompress(false, "", summaryGroupId);
        IE_LOG(INFO, "compress is not set, default set to : false");
    } else {
        bool useCompress = AnyCast<bool>(it->second);
        string compressType = "";
        if (useCompress) {
            it = summary.find(index::COMPRESS_TYPE);
            if (it != summary.end()) {
                compressType = AnyCast<string>(it->second);
            }
        }
        SetSummaryCompress(useCompress, compressType, summaryGroupId);
    }

    it = summary.find(SUMMARY_ADAPTIVE_OFFSET);
    if (it != summary.end()) {
        bool adaptiveOffset = AnyCast<bool>(it->second);
        SetAdaptiveOffset(adaptiveOffset, summaryGroupId);
    }
    return true;
}

void RegionSchemaImpl::LoadSourceSchema(const Any& any)
{
    JsonWrapper wrapper(any);
    mSourceSchema.reset(new SourceSchema);
    mSourceSchema->Jsonize(wrapper);

    for (index::groupid_t id = 0; id < mSourceSchema->GetSourceGroupCount(); id++) {
        auto groupConfig = mSourceSchema->GetGroupConfig(id);
        if (groupConfig) {
            if (!groupConfig->GetParameter().NeedSyncFileCompressConfig()) {
                continue;
            }
            GroupDataParameter param = groupConfig->GetParameter();
            auto status = param.SyncFileCompressConfig(mFileCompressSchema);
            THROW_IF_STATUS_ERROR(status);
            groupConfig->SetParameter(param);
        }
    }
}

void RegionSchemaImpl::LoadSummarySchema(const Any& any)
{
    if (!LoadSummaryGroup(any, DEFAULT_SUMMARYGROUPID)) {
        return;
    }
    // non-default summary group
    JsonMap summary = AnyCast<JsonMap>(any);
    JsonMap::const_iterator it = summary.find(SUMMARY_GROUPS);
    if (it != summary.end()) {
        JsonArray summaryGroups = AnyCast<JsonArray>(it->second);
        for (JsonArray::iterator iter = summaryGroups.begin(); iter != summaryGroups.end(); ++iter) {
            JsonMap group = AnyCast<JsonMap>(*iter);
            JsonMap::const_iterator groupIt = group.find(SUMMARY_GROUP_NAME);
            if (groupIt == group.end()) {
                INDEXLIB_FATAL_ERROR(Schema, "summary group has no name");
            }
            string groupName = AnyCast<string>(groupIt->second);
            summarygroupid_t groupId = CreateSummaryGroup(groupName);
            if (!LoadSummaryGroup(group, groupId)) {
                INDEXLIB_FATAL_ERROR(Schema, "load summary groupName[%s] failed", groupName.c_str());
            }
        }
    }
}

void RegionSchemaImpl::SetNeedStoreSummary()
{
    if (!mSummarySchema) {
        return;
    }
    if (!mAttributeSchema) {
        mSummarySchema->SetNeedStoreSummary(true);
        return;
    }
    mSummarySchema->SetNeedStoreSummary(false);
    SummarySchema::Iterator it;
    for (it = mSummarySchema->Begin(); it != mSummarySchema->End(); it++) {
        fieldid_t fieldId = (*it)->GetFieldConfig()->GetFieldId();
        if (!mAttributeSchema->IsInAttribute(fieldId)) {
            mSummarySchema->SetNeedStoreSummary(fieldId);
        }
    }
}

void RegionSchemaImpl::SetDefaultTTL(int64_t defaultTTL, const string& fieldName)
{
    if (defaultTTL >= 0) {
        SetEnableTTL(true, fieldName);
        mDefaultTTL = defaultTTL;
    } else {
        SetEnableTTL(false, fieldName);
    }
}

void RegionSchemaImpl::SetEnableOrderPreserving()
{
    if (mOrderPreservingField.empty()) {
        return;
    }

    TableType tableType = mSchema->GetTableType();
    if (tableType != tt_index) {
        IE_LOG(ERROR, "not index table cannot support order preserving");
        INDEXLIB_FATAL_ERROR(Schema, "not index table cannot support order preserving");
        return;
    }

    if (!mAttributeSchema || !mAttributeSchema->GetAttributeConfig(mOrderPreservingField)) {
        IE_LOG(ERROR, "order preserving field [%s] not in attribute config", mOrderPreservingField.c_str());
        INDEXLIB_FATAL_ERROR(Schema, "order preserving field [%s] not in attribute config",
                             mOrderPreservingField.c_str());
        return;
    }

    FieldConfigPtr fieldConfig = mFieldSchema->GetFieldConfig(mOrderPreservingField);
    if (!fieldConfig) {
        IE_LOG(ERROR, "order preserving field [%s] not in field config", mOrderPreservingField.c_str());
        INDEXLIB_FATAL_ERROR(Schema, "order preserving field [%s] not in field config", mOrderPreservingField.c_str());
        return;
    }
    FieldType fieldType = fieldConfig->GetFieldType();
    switch (fieldType) {
#define MACRO(type)                                                                                                    \
    case type: {                                                                                                       \
        return;                                                                                                        \
    }
        NUMBER_FIELD_MACRO_HELPER(MACRO)
#undef MACRO
    default: {
        IE_LOG(ERROR, "order preserving field [%s] should be number type", mOrderPreservingField.c_str());
        INDEXLIB_FATAL_ERROR(Schema, "order preserving field [%s] should be number type",
                             mOrderPreservingField.c_str());
        return;
    }
    }
}

void RegionSchemaImpl::SetEnableTTL(bool enableTTL, const string& fieldName)
{
    if (!enableTTL) {
        mDefaultTTL = INVALID_TTL;
        return;
    }

    if (mDefaultTTL == INVALID_TTL) {
        mDefaultTTL = DEFAULT_TIME_TO_LIVE;
    }
    mTTLFieldName = fieldName;
    TableType tableType = mSchema->GetTableType();

    if (tableType == tt_kv || tableType == tt_kkv) {
        CheckKvKkvPrimaryKeyConfig();

        auto kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, mIndexSchema->GetPrimaryKeyIndexConfig());
        assert(kvConfig);

        // Set kvConfig Default TTL
        kvConfig->SetTTL(mDefaultTTL);
        if (TTLFromDoc() && !fieldName.empty()) {
            kvConfig->EnableStoreExpireTime();
        }
        return;
    }

    // only for index table
    // add doc_time_to_live_in_seconds to attribute
    if (!mAttributeSchema || !mAttributeSchema->GetAttributeConfig(mTTLFieldName)) {
        FieldConfigPtr fieldConfig = mFieldSchema->GetFieldConfig(mTTLFieldName);
        if (!fieldConfig) {
            fieldConfig.reset(new FieldConfig(mTTLFieldName, ft_uint32, false));
            fieldConfig->SetBuiltInField(true);
            mFieldSchema->AddFieldConfig(fieldConfig);
        }
        AddAttributeConfig(mTTLFieldName);
    }

    AttributeConfigPtr attrConfig = mAttributeSchema->GetAttributeConfig(mTTLFieldName);
    if (attrConfig) {
        // check
        if (attrConfig->GetFieldType() != ft_uint32 || attrConfig->IsMultiValue()) {
            INDEXLIB_FATAL_ERROR(Schema, "ttl field config should be ft_uint32 and single value");
        }
        return;
    }
}

void RegionSchemaImpl::SetEnableHashId(bool enableHashId, const string& fieldName)
{
    TableType tableType = mSchema->GetTableType();
    if (tableType == tt_kv || tableType == tt_kkv || tableType == tt_customized) {
        return;
    }
    mHashIdFieldName = fieldName;
    if (!mAttributeSchema || !mAttributeSchema->GetAttributeConfig(mHashIdFieldName)) {
        FieldConfigPtr fieldConfig = mFieldSchema->GetFieldConfig(mHashIdFieldName);
        if (!fieldConfig) {
            fieldConfig.reset(new FieldConfig(mHashIdFieldName, ft_uint16, false));
            fieldConfig->SetBuiltInField(true);
            // set equal compress because:
            // 1、 hash id is in range [0,65535], many fields have the same value
            // 2、 it's friendly for users to sort, example. bs sort build by hashid
            fieldConfig->SetCompressType("equal");
            mFieldSchema->AddFieldConfig(fieldConfig);
        }
        AddAttributeConfig(mHashIdFieldName);
    }
    AttributeConfigPtr attrConfig = mAttributeSchema->GetAttributeConfig(mHashIdFieldName);
    if (attrConfig) {
        // check
        if (attrConfig->GetFieldType() != ft_uint16 || attrConfig->IsMultiValue()) {
            INDEXLIB_FATAL_ERROR(Schema, "hash_id field config should be ft_uint16, single value");
        }
        return;
    }
}

void RegionSchemaImpl::CloneVirtualAttributes(const RegionSchemaImpl& other)
{
    assert(!mVirtualAttributeSchema);
    const AttributeSchemaPtr& virtualAttributeSchema = other.GetVirtualAttributeSchema();
    if (virtualAttributeSchema) {
        AttributeSchema::Iterator iter = virtualAttributeSchema->Begin();
        for (; iter != virtualAttributeSchema->End(); iter++) {
            const AttributeConfigPtr& virAttrConfig = *iter;
            AddVirtualAttributeConfig(virAttrConfig);
        }
    }
}

bool RegionSchemaImpl::AddVirtualAttributeConfigs(const AttributeConfigVector& virtualAttrConfigs)
{
    bool hasNewVirtualAttribute = false;
    for (size_t i = 0; i < virtualAttrConfigs.size(); i++) {
        const AttributeConfigPtr& attrConfig = virtualAttrConfigs[i];
        if (mVirtualAttributeSchema && mVirtualAttributeSchema->IsInAttribute(attrConfig->GetAttrName())) {
            continue;
        }
        AddVirtualAttributeConfig(attrConfig);
        hasNewVirtualAttribute = true;
    }
    return hasNewVirtualAttribute;
}

void RegionSchemaImpl::AssertEqual(const RegionSchemaImpl& other) const
{
    if (mRegionName != other.mRegionName) {
        INDEXLIB_FATAL_ERROR(AssertEqual, "region name is not equal");
    }

#define REGION_ITEM_ASSERT_EQUAL(schemaItemPtr, exceptionMsg)                                                          \
    if (schemaItemPtr.get() != NULL && other.schemaItemPtr.get() != NULL) {                                            \
        schemaItemPtr->AssertEqual(*(other.schemaItemPtr));                                                            \
    } else if (schemaItemPtr.get() != NULL || other.schemaItemPtr.get() != NULL) {                                     \
        INDEXLIB_FATAL_ERROR(AssertEqual, exceptionMsg);                                                               \
    }

    // mFileCompressSchema
    constexpr const char* exceptionMsg = "file compress schema is not equal";
    if (mFileCompressSchema && other.mFileCompressSchema) {
        auto status = mFileCompressSchema->CheckEqual(*(other.mFileCompressSchema));
        THROW_IF_STATUS_ERROR(status);
    } else if (mFileCompressSchema || other.mFileCompressSchema) {
        INDEXLIB_FATAL_ERROR(AssertEqual, exceptionMsg);
    }
    // mFieldSchema
    REGION_ITEM_ASSERT_EQUAL(mFieldSchema, "Field schema is not equal");
    // mIndexSchema
    REGION_ITEM_ASSERT_EQUAL(mIndexSchema, "Index schema is not equal");
    // mAttributeSchema
    REGION_ITEM_ASSERT_EQUAL(mAttributeSchema, "Attribute schema is not equal");
    // mVirtualAttributeSchema
    REGION_ITEM_ASSERT_EQUAL(mVirtualAttributeSchema, "Virtual Attribute schema is not equal");
    // mSummarySchema
    REGION_ITEM_ASSERT_EQUAL(mSummarySchema, "Summary schema is not equal");
    // mTemperatureLayer
    REGION_ITEM_ASSERT_EQUAL(mTemperatureLayer, "Temperature config is not equal");

#undef REGION_ITEM_ASSERT_EQUAL
}

void RegionSchemaImpl::AssertCompatible(const RegionSchemaImpl& other) const
{
    if (mRegionName != other.mRegionName) {
        INDEXLIB_FATAL_ERROR(AssertCompatible, "region name is not compatible");
    }

    // mFieldSchema
    if (mFieldSchema && other.mFieldSchema) {
        mFieldSchema->AssertCompatible(*(other.mFieldSchema));
    } else if (!other.mFieldSchema && mFieldSchema) {
        INDEXLIB_FATAL_ERROR(AssertCompatible, "field schema in region [%s] is not compatible", mRegionName.c_str());
    }

    // mIndexSchema
    if (mIndexSchema && other.mIndexSchema) {
        mIndexSchema->AssertCompatible(*(other.mIndexSchema));
    } else if (!other.mIndexSchema && mIndexSchema) {
        INDEXLIB_FATAL_ERROR(AssertCompatible, "Index schema is not compatible");
    }

    // mAttributeSchema
    if (mAttributeSchema && other.mAttributeSchema) {
        mAttributeSchema->AssertCompatible(*(other.mAttributeSchema));
    } else if (!other.mAttributeSchema && mAttributeSchema) {
        INDEXLIB_FATAL_ERROR(AssertCompatible, "Attribute schema is not compatible");
    }

    // mSummaryinfos
    if (mSummarySchema && other.mSummarySchema) {
        mSummarySchema->AssertCompatible(*(other.mSummarySchema));
    } else if (!other.mSummarySchema && mSummarySchema) {
        INDEXLIB_FATAL_ERROR(AssertCompatible, "Summary schema is not compatible");
    }

    // mTruncateProfileSchema is allways compatible
}

void RegionSchemaImpl::CheckFieldSchema() const
{
    // fixed_multi_value_type is temporarily supported in kv_table only
    FieldSchema::Iterator iter = mFieldSchema->Begin();
    for (; iter != mFieldSchema->End(); ++iter) {
        if ((*iter)->IsMultiValue() && (*iter)->GetFixedMultiValueCount() != -1) {
            if (mSchema->GetTableType() != tt_kv && mSchema->GetTableType() != tt_kkv &&
                mSchema->GetTableType() != tt_index) {
                INDEXLIB_FATAL_ERROR(Schema,
                                     "table type [%d] does not"
                                     " support fixed_multi_value_count",
                                     int(mSchema->GetTableType()));
            }
        }
    }
}

void RegionSchemaImpl::CheckKvKkvPrimaryKeyConfig() const
{
    TableType tableType = mSchema->GetTableType();
    if (tableType == tt_kv &&
        (!mIndexSchema->GetPrimaryKeyIndexConfig() || mIndexSchema->GetPrimaryKeyIndexType() != it_kv)) {
        INDEXLIB_FATAL_ERROR(Schema, "table type [kv] not match with index define");
    }
    if (tableType == tt_kkv &&
        (!mIndexSchema->GetPrimaryKeyIndexConfig() || mIndexSchema->GetPrimaryKeyIndexType() != it_kkv)) {
        INDEXLIB_FATAL_ERROR(Schema, "table type [kkv] not match with index define");
    }
    // kv kkv should not support customization
    if ((tableType == tt_kv || tableType == tt_kkv) &&
        mIndexSchema->GetPrimaryKeyIndexConfig()->GetCustomizedConfigs().size() > 0) {
        INDEXLIB_FATAL_ERROR(Schema, "kv or kkv table does not support index customization");
    }
}

void RegionSchemaImpl::CheckIndexSchema() const
{
    mIndexSchema->Check();
    CheckKvKkvPrimaryKeyConfig();

    // TODO: refine move to index schema check
    uint32_t fieldCount = mFieldSchema->GetFieldCount();

    std::vector<uint32_t> singleFieldIndexCounts(fieldCount, 0);
    IndexSchema::Iterator it = mIndexSchema->Begin();
    std::map<std::string, std::set<std::string>> singleFieldIndexConfigsWithProfileNames;
    for (; it != mIndexSchema->End(); ++it) {
        IndexConfigPtr indexConfig = *it;
        if (indexConfig->IsDeleted()) {
            continue;
        }

        std::vector<std::string> profileNames = indexConfig->GetUseTruncateProfiles();
        std::set<std::string> profileNameSet(profileNames.begin(), profileNames.end());
        if (profileNames.size() != profileNameSet.size()) {
            INDEXLIB_FATAL_ERROR(Schema, "index [%s] has duplicate profile name", indexConfig->GetIndexName().c_str());
        }
        if (indexConfig->HasTruncate()) {
            CheckIndexTruncateProfiles(indexConfig);
        }

        InvertedIndexType indexType = indexConfig->GetInvertedIndexType();
        if (indexType == it_pack || indexType == it_expack || indexType == it_customized) {
            CheckFieldsOrderInPackIndex(indexConfig);
        } else {
            CheckSingleFieldIndex(indexConfig, &singleFieldIndexCounts, &singleFieldIndexConfigsWithProfileNames);
        }

        if (indexType == it_spatial) {
            CheckSpatialIndexConfig(indexConfig);
        }
    }
}

// The check is intended to prevent adding redundant single field inverted index configs.
// However, multiple single field index configs are necessary to support different term payload loads.
// Thus multiple single field index configs with payload names are allowed.
void RegionSchemaImpl::CheckSingleFieldIndex(
    const IndexConfigPtr& indexConfig, std::vector<uint32_t>* singleFieldIndexCounts,
    std::map<std::string, std::set<std::string>>* singleFieldIndexConfigsWithProfileNames) const
{
    FieldSchema::Iterator fieldIt = mFieldSchema->Begin();
    for (; fieldIt != mFieldSchema->End(); fieldIt++) {
        FieldConfigPtr fieldConfig = *fieldIt;
        fieldid_t fieldId = fieldConfig->GetFieldId();
        if (!indexConfig->IsInIndex(fieldId)) {
            continue;
        }

        if (indexConfig->GetShardingType() == IndexConfig::IST_IS_SHARDING) {
            continue;
        }

        (*singleFieldIndexCounts)[fieldId]++;

        std::string fieldName = fieldConfig->GetFieldName();
        std::vector<std::string> profileNames = indexConfig->GetUseTruncateProfiles();
        std::set<std::string> profileNameSet(profileNames.begin(), profileNames.end());
        if (singleFieldIndexConfigsWithProfileNames->find(fieldName) !=
            singleFieldIndexConfigsWithProfileNames->end()) {
            for (const std::string& profileName : profileNameSet) {
                if (singleFieldIndexConfigsWithProfileNames->at(fieldName).find(profileName) !=
                    singleFieldIndexConfigsWithProfileNames->at(fieldName).end()) {
                    stringstream ss;
                    ss << "Single field " << fieldName << " has more than one index with the same profile "
                       << profileName;
                    INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
                } else {
                    singleFieldIndexConfigsWithProfileNames->at(fieldName).insert(profileName);
                }
            }
        } else {
            (*singleFieldIndexConfigsWithProfileNames)[fieldName] = std::set<std::string> {profileNameSet};
        }

        if ((*singleFieldIndexCounts)[fieldId] > 1) {
            const PayloadConfig& payloadConfig = indexConfig->GetTruncatePayloadConfig();
            if (!payloadConfig.IsInitialized()) {
                stringstream ss;
                ss << "Field " << fieldName << " has more than one single field index.";
                INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
            }
        }
    }
}

void RegionSchemaImpl::CheckSpatialIndexConfig(const IndexConfigPtr& indexConfig) const
{
    assert(indexConfig->GetInvertedIndexType() == it_spatial);
    SpatialIndexConfigPtr spatialIndexConf = DYNAMIC_POINTER_CAST(SpatialIndexConfig, indexConfig);
    assert(spatialIndexConf);

    FieldConfigPtr fieldConfig = spatialIndexConf->GetFieldConfig();
    assert(fieldConfig);
    // TODO: support line and polygon
    if (fieldConfig->GetFieldType() != ft_location) {
        return;
    }
    std::string fieldName = fieldConfig->GetFieldName();
    AttributeConfigPtr attrConf;
    if (mAttributeSchema) {
        attrConf = mAttributeSchema->GetAttributeConfig(fieldName);
    }
    if (!attrConf) {
        INDEXLIB_FATAL_ERROR(Schema, "field [%s] should in attributes, because in spatial index [%s]",
                             fieldName.c_str(), spatialIndexConf->GetIndexName().c_str());
    }
    if (attrConf->GetPackAttributeConfig()) {
        INDEXLIB_FATAL_ERROR(Schema, "field [%s] should not in pack attribute, because in spatial index [%s]",
                             fieldName.c_str(), spatialIndexConf->GetIndexName().c_str());
    }
}

void RegionSchemaImpl::CheckFieldsOrderInPackIndex(const IndexConfigPtr& indexConfig) const
{
    PackageIndexConfigPtr packageConfig = dynamic_pointer_cast<PackageIndexConfig>(indexConfig);
    FieldSchema::Iterator fieldIt = mFieldSchema->Begin();
    int32_t lastFieldPosition = -1;
    fieldid_t lastFieldId = -1;
    for (; fieldIt != mFieldSchema->End(); fieldIt++) {
        FieldConfigPtr fieldConfig = *fieldIt;
        fieldid_t fieldId = fieldConfig->GetFieldId();
        if (!packageConfig->IsInIndex(fieldId)) {
            continue;
        }

        int32_t curFieldPosition = packageConfig->GetFieldIdxInPack(fieldId);
        if (curFieldPosition < lastFieldPosition) {
            string beforeFieldName = mFieldSchema->GetFieldConfig(lastFieldId)->GetFieldName();
            string afterFieldName = mFieldSchema->GetFieldConfig(fieldId)->GetFieldName();

            stringstream ss;
            ss << "wrong field order in IndexConfig '" << indexConfig->GetIndexName() << "': expect field '"
               << beforeFieldName << "' before field '" << afterFieldName << "', but found '" << afterFieldName
               << "' before '" << beforeFieldName << "'";
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
        lastFieldPosition = curFieldPosition;
        lastFieldId = fieldId;
    }
}

void RegionSchemaImpl::CheckTruncateSortParams() const
{
    for (TruncateProfileSchema::Iterator it = mTruncateProfileSchema->Begin(); it != mTruncateProfileSchema->End();
         ++it) {
        const SortParams& sortParams = it->second->GetTruncateSortParams();
        for (SortParams::const_iterator sortParamIt = sortParams.begin(); sortParamIt != sortParams.end();
             ++sortParamIt) {
            if (DOC_PAYLOAD_FIELD_NAME == sortParamIt->GetSortField()) {
                continue;
            }
            FieldConfigPtr fieldConfig = mFieldSchema->GetFieldConfig(sortParamIt->GetSortField());
            if (!fieldConfig) {
                INDEXLIB_FATAL_ERROR(Schema,
                                     "truncate sort field [%s] "
                                     "is not in field schema",
                                     sortParamIt->GetSortField().c_str());
            }

            AttributeConfigPtr attrConfig;
            if (mAttributeSchema) {
                attrConfig = mAttributeSchema->GetAttributeConfig(fieldConfig->GetFieldName());
            }
            if (!attrConfig) {
                INDEXLIB_FATAL_ERROR(Schema,
                                     "truncate sort field [%s]"
                                     " has not corresponding attribute config",
                                     fieldConfig->GetFieldName().c_str());
            }

            if (attrConfig->GetPackAttributeConfig() != NULL) {
                INDEXLIB_FATAL_ERROR(Schema,
                                     "truncate sort field [%s] "
                                     "should not be in pack attribute.",
                                     fieldConfig->GetFieldName().c_str());
            }
        }
    }
}

// Check the case that if payload name is used in any of the sort params that uses DOC_PAYLOAD, all other sort params
// that use DOC_PAYLOAD should also specify payload name. It's also valid that non of the sort params that uses
// DOC_PAYLOAD specifies payload name. This is to be backward compatible.
void RegionSchemaImpl::CheckTruncateProfileSchema() const
{
    if (mTruncateProfileSchema == nullptr) {
        return;
    }
    bool payloadNameSpecified = false;
    for (auto iter = mTruncateProfileSchema->Begin(); iter != mTruncateProfileSchema->End(); ++iter) {
        TruncateProfileConfigPtr truncateProfileConfig = iter->second;
        const SortParams& sortParams = truncateProfileConfig->GetTruncateSortParams();
        for (auto sortParamIt = sortParams.begin(); sortParamIt != sortParams.end(); ++sortParamIt) {
            if (DOC_PAYLOAD_FIELD_NAME == sortParamIt->GetSortField()) {
                if (truncateProfileConfig->GetPayloadConfig().IsInitialized()) {
                    payloadNameSpecified = true;
                    break;
                }
            }
        }
    }
    if (!payloadNameSpecified) {
        return;
    }
    for (auto iter = mTruncateProfileSchema->Begin(); iter != mTruncateProfileSchema->End(); ++iter) {
        TruncateProfileConfigPtr truncateProfileConfig = iter->second;
        const SortParams& sortParams = truncateProfileConfig->GetTruncateSortParams();
        for (auto sortParamIt = sortParams.begin(); sortParamIt != sortParams.end(); ++sortParamIt) {
            if (DOC_PAYLOAD_FIELD_NAME == sortParamIt->GetSortField()) {
                if (!truncateProfileConfig->GetPayloadConfig().IsInitialized()) {
                    INDEXLIB_FATAL_ERROR(Schema,
                                         "If payload name is used in any truncate profile, all other truncate profiles "
                                         "that use payload should also specify payload name. Truncate profile [%s] "
                                         "does not specify payload name.",
                                         iter->first.c_str());
                }
            }
        }
    }
}

void RegionSchemaImpl::CheckIndexTruncateProfiles(const IndexConfigPtr& indexConfig) const
{
    if (!mTruncateProfileSchema) {
        INDEXLIB_FATAL_ERROR(Schema, "has no truncate profiles shcema.");
    }
    vector<string> profileNames = indexConfig->GetUseTruncateProfiles();
    for (size_t i = 0; i < profileNames.size(); ++i) {
        const string& profileName = profileNames[i];
        const string& indexName = indexConfig->GetIndexName();
        TruncateProfileConfigPtr profile = mTruncateProfileSchema->GetTruncateProfileConfig(profileName);
        if (!profile) {
            INDEXLIB_FATAL_ERROR(Schema, "has no truncate profile name [%s] of index [%s]", profileName.c_str(),
                                 indexName.c_str());
        }
        // truncate not support single compressed float
        const SortParams& sortParams = profile->GetTruncateSortParams();
        for (auto sortParam : sortParams) {
            string sortField = sortParam.GetSortField();
            FieldConfigPtr fieldConfig = mFieldSchema->GetFieldConfig(sortField);
            if (!fieldConfig) {
                // may be DOC_PAYLOAD
                continue;
            }
            if (fieldConfig->GetFieldType() == FieldType::ft_fp16 || fieldConfig->GetFieldType() == FieldType::ft_fp8) {
                INDEXLIB_FATAL_ERROR(Schema, "invalid field[%s] for truncate profile name [%s] of index [%s]",
                                     fieldConfig->GetFieldName().c_str(), profileName.c_str(), indexName.c_str());
            }
            CompressTypeOption compress = fieldConfig->GetCompressType();
            if (compress.HasFp16EncodeCompress() || compress.HasInt8EncodeCompress()) {
                INDEXLIB_FATAL_ERROR(Schema, "invalid field[%s] for truncate profile name [%s] of index [%s]",
                                     fieldConfig->GetFieldName().c_str(), profileName.c_str(), indexName.c_str());
            }
        }
    }
}

void RegionSchemaImpl::Check() const
{
    TableType tableType = mSchema->GetTableType();
    if (tableType != tt_customized) {
        // non customized table
        if (!mFieldSchema) {
            stringstream ss;
            ss << "IndexPartitionSchema has no fieldSchema" << endl;
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
        CheckFieldSchema();
        if (!mSchema->AllowNoIndex() && !mIndexSchema) {
            stringstream ss;
            ss << "IndexPartitionSchema has no IndexSchema" << endl;
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
        if (mIndexSchema) {
            CheckIndexSchema();
        }
        if (mTruncateProfileSchema) {
            CheckTruncateSortParams();
            CheckTruncateProfileSchema();
        }

        if (mAttributeSchema) {
            CheckAttributeSchema();
        }

        if (mSourceSchema) {
            CheckSourceSchema();
        }

        if (mFileCompressSchema) {
            mFileCompressSchema->Check();
        }

        if (mEnableTemperatureLayer) {
            if (mTemperatureLayer == nullptr) {
                INDEXLIB_FATAL_ERROR(Schema,
                                     "when set enable temperature layer, should configure temperature layer config");
            }
            mTemperatureLayer->Check(mAttributeSchema);
        }
        return;
    }

    // customized table with fieldSchema
    if (mFieldSchema) {
        CheckFieldSchema();
    }
    // with indexSchema
    if (mIndexSchema) {
        CheckIndexSchema();
    }
    // with trunProfile
    if (mTruncateProfileSchema) {
        CheckTruncateSortParams();
    }
    // with attributes
    if (mAttributeSchema) {
        CheckAttributeSchema();
    }
}

void RegionSchemaImpl::CheckAttributeSchema() const
{
    TableType tableType = mSchema->GetTableType();
    for (auto attrConfigIter = mAttributeSchema->Begin(); attrConfigIter != mAttributeSchema->End(); attrConfigIter++) {
        CheckAttributeConfig(tableType, *attrConfigIter);
    }
}

void RegionSchemaImpl::CheckSourceSchema() const
{
    // check every specified field belongs to the field schema
    for (auto iter = mSourceSchema->Begin(); iter != mSourceSchema->End(); ++iter) {
        const SourceGroupConfigPtr& groupConfig = *iter;
        if (groupConfig->GetFieldMode() != SourceGroupConfig::SourceFieldMode::SPECIFIED_FIELD) {
            continue;
        }
        const vector<string>& fields = groupConfig->GetSpecifiedFields();
        for (auto& field : fields) {
            if (!mFieldSchema->IsFieldNameInSchema(field)) {
                INDEXLIB_FATAL_ERROR(Schema,
                                     "source field [%s] in group[%u]"
                                     "is not defined in field shema",
                                     field.c_str(), groupConfig->GetGroupId());
                return;
            }
        }
    }

    // check source schema
    mSourceSchema->Check();
}

void RegionSchemaImpl::CheckAttributeConfig(TableType type, const AttributeConfigPtr& config) const
{
    if (config->GetCustomizedConfigs().size() > 0 && (type == tt_kv || type == tt_kkv)) {
        INDEXLIB_FATAL_ERROR(Schema, "kv or kkv table does not support attribute customization");
    }

    if (config->SupportNull() && config->GetPackAttributeConfig() != NULL) {
        INDEXLIB_FATAL_ERROR(Schema, "attribute [%s] in pack attribute [%s] not support enable null.",
                             config->GetAttrName().c_str(), config->GetPackAttributeConfig()->GetPackName().c_str());
    }

    FieldType ft = config->GetFieldType();
    if ((ft == ft_date || ft == ft_time || ft == ft_timestamp) && config->GetPackAttributeConfig() != NULL) {
        INDEXLIB_FATAL_ERROR(Schema, "attribute [%s] with field type [%s] not support in pack attribute [%s].",
                             config->GetAttrName().c_str(), FieldConfig::FieldTypeToStr(ft),
                             config->GetPackAttributeConfig()->GetPackName().c_str());
    }
}

bool RegionSchemaImpl::SupportAutoUpdate() const
{
    TableType tableType = mSchema->GetTableType();
    if (tableType != tt_index) {
        return false;
    }

    if (NeedStoreSummary()) {
        // has summary field
        return false;
    }

    if (!mIndexSchema || !mIndexSchema->HasPrimaryKeyIndex()) {
        return false;
    }
    if (mIndexSchema->GetIndexCount() > 1) {
        // has index field besides pk
        return false;
    }

    if (mVirtualAttributeSchema) {
        // virtual attribute default value is unknown
        return false;
    }
    if (!mAttributeSchema) {
        return false;
    }

    AttributeConfigIteratorPtr attrConfigs = mAttributeSchema->CreateIterator(IndexStatus::is_normal);
    for (auto iter = attrConfigs->Begin(); iter != attrConfigs->End(); iter++) {
        const AttributeConfigPtr& attrConfig = *iter;
        assert(attrConfig);
        if (attrConfig->GetFieldId() == mIndexSchema->GetPrimaryKeyIndexFieldId()) {
            continue;
        }
        if (!attrConfig->IsAttributeUpdatable()) {
            // has unupdatable attribute field
            return false;
        }
    }
    return true;
}

FieldConfigPtr RegionSchemaImpl::AddFieldConfig(const string& fieldName, FieldType fieldType, bool multiValue,
                                                bool isVirtual, bool isBinary)
{
    return FieldConfigLoader::AddFieldConfig(mFieldSchema, fieldName, fieldType, multiValue, isVirtual, isBinary);
}

EnumFieldConfigPtr RegionSchemaImpl::AddEnumFieldConfig(const string& fieldName, FieldType fieldType,
                                                        vector<string>& validValues, bool multiValue)
{
    return FieldConfigLoader::AddEnumFieldConfig(mFieldSchema, fieldName, fieldType, validValues, multiValue);
}

void RegionSchemaImpl::SetBaseSchemaImmutable()
{
    if (!mFieldSchema) {
        mFieldSchema.reset(new FieldSchema);
    }
    mFieldSchema->SetBaseSchemaImmutable();

    if (!mIndexSchema) {
        mIndexSchema.reset(new IndexSchema);
    }
    mIndexSchema->SetBaseSchemaImmutable();

    if (!mAttributeSchema) {
        mAttributeSchema.reset(new AttributeSchema);
    }
    mAttributeSchema->SetBaseSchemaImmutable();
}

void RegionSchemaImpl::SetModifySchemaImmutable()
{
    if (!mFieldSchema) {
        mFieldSchema.reset(new FieldSchema);
    }
    mFieldSchema->SetModifySchemaImmutable();

    if (!mIndexSchema) {
        mIndexSchema.reset(new IndexSchema);
    }
    mIndexSchema->SetModifySchemaImmutable();

    if (!mAttributeSchema) {
        mAttributeSchema.reset(new AttributeSchema);
    }
    mAttributeSchema->SetModifySchemaImmutable();
}

void RegionSchemaImpl::SetModifySchemaMutable()
{
    if (mFieldSchema) {
        mFieldSchema->SetModifySchemaMutable();
    }

    if (mIndexSchema) {
        mIndexSchema->SetModifySchemaMutable();
    }

    if (mAttributeSchema) {
        mAttributeSchema->SetModifySchemaMutable();
    }
}

vector<AttributeConfigPtr> RegionSchemaImpl::EnsureSpatialIndexWithAttribute()
{
    vector<AttributeConfigPtr> ret;
    if (mSchema->GetTableType() == tt_customized || mSchema->GetTableType() == tt_orc) {
        // orc does not support spatial index
        return ret;
    }

    if (!mIndexSchema) {
        INDEXLIB_FATAL_ERROR(Schema, "no index schema!");
    }
    IndexSchema::Iterator it = mIndexSchema->Begin();
    for (; it != mIndexSchema->End(); ++it) {
        IndexConfigPtr indexConfig = *it;
        if (indexConfig->IsDeleted()) {
            continue;
        }

        InvertedIndexType indexType = indexConfig->GetInvertedIndexType();
        if (indexType == it_spatial) {
            SpatialIndexConfigPtr spatialIndexConf = DYNAMIC_POINTER_CAST(SpatialIndexConfig, indexConfig);
            assert(spatialIndexConf);

            FieldConfigPtr fieldConfig = spatialIndexConf->GetFieldConfig();
            if (fieldConfig->GetFieldType() != ft_location) {
                continue;
                // TODO: support polygon and line attribute
            }
            assert(fieldConfig);
            std::string fieldName = fieldConfig->GetFieldName();
            if (!mAttributeSchema || !mAttributeSchema->GetAttributeConfig(fieldName)) {
                IE_LOG(INFO, "inner add attribute [%s] to ensure spatial index precision", fieldName.c_str());
                AddAttributeConfig(fieldName);
                AttributeConfigPtr attrConfig = mAttributeSchema->GetAttributeConfig(fieldName);
                assert(attrConfig);
                attrConfig->SetConfigType(AttributeConfig::ct_index_accompany);
                attrConfig->SetFileCompressConfig(indexConfig->GetFileCompressConfig());
                ret.push_back(attrConfig);
            }
        }
    }
    return ret;
}

void RegionSchemaImpl::SetSummaryGroupDataParam(const GroupDataParameter& param, summarygroupid_t summaryGroupId)
{
    if (!mSummarySchema) {
        mSummarySchema.reset(new SummarySchema);
    }
    const SummaryGroupConfigPtr summaryGroupConfig = mSummarySchema->GetSummaryGroupConfig(summaryGroupId);
    assert(summaryGroupConfig);
    summaryGroupConfig->SetSummaryGroupDataParam(param);
}

void RegionSchemaImpl::SetAdaptiveOffset(bool adaptiveOffset, summarygroupid_t summaryGroupId)
{
    if (!mSummarySchema) {
        mSummarySchema.reset(new SummarySchema);
    }
    const SummaryGroupConfigPtr summaryGroupConfig = mSummarySchema->GetSummaryGroupConfig(summaryGroupId);
    assert(summaryGroupConfig);
    summaryGroupConfig->SetEnableAdaptiveOffset(adaptiveOffset);
}
}} // namespace indexlib::config
