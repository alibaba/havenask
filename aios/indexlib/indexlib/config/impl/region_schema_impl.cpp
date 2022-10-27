#include <algorithm>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include "indexlib/config/impl/region_schema_impl.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/impl/index_partition_schema_impl.h"
#include "indexlib/config/index_config_creator.h"
#include "indexlib/config/field_config_loader.h"
#include "indexlib/config/spatial_index_config.h"
#include "indexlib/config/impl/spatial_index_config_impl.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil;

IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, RegionSchemaImpl);

RegionSchemaImpl::RegionSchemaImpl(IndexPartitionSchemaImpl* schema, bool multiRegionFormat)
    : mSchema(schema)
    , mRegionName(DEFAULT_REGIONNAME)
    , mTTLFieldName(DOC_TIME_TO_LIVE_IN_SECONDS)
    , mDefaultTTL(INVALID_TTL)
    , mOwnFieldSchema(false)
    , mMultiRegionFormat(multiRegionFormat)
{
    assert(mSchema);
    if (schema->GetTableType() == tt_rawfile || schema->GetTableType() == tt_linedata)
    {
        mIndexSchema.reset(new IndexSchema);
        mSummarySchema.reset(new SummarySchema);
    }
    mFieldSchema = schema->GetFieldSchema();
}

RegionSchemaImpl::~RegionSchemaImpl()
{}

void RegionSchemaImpl::AddIndexConfig(const IndexConfigPtr& indexConfig)
{
    if (!mIndexSchema)
    {
        mIndexSchema.reset(new IndexSchema);
    }
    mIndexSchema->AddIndexConfig(indexConfig);
}

void RegionSchemaImpl::AddAttributeConfig(const AttributeConfigPtr& attrConfig)
{
    if (!mAttributeSchema)
    {
        mAttributeSchema.reset(new AttributeSchema);
    }
    mAttributeSchema->AddAttributeConfig(attrConfig);
}

void RegionSchemaImpl::AddAttributeConfig(const string& fieldName,
                                          const CustomizedConfigVector& customizedConfigs)
{
    AttributeConfigPtr attrConfig = CreateAttributeConfig(fieldName, customizedConfigs);
    assert(attrConfig);
    if (!mAttributeSchema)
    {
        mAttributeSchema.reset(new AttributeSchema);
    }
    mAttributeSchema->AddAttributeConfig(attrConfig);
}

void RegionSchemaImpl::AddVirtualAttributeConfig(const AttributeConfigPtr& virtualAttrConfig)
{
    AttributeConfigPtr attrConfig = CreateVirtualAttributeConfig(
            virtualAttrConfig->GetFieldConfig(),
            virtualAttrConfig->GetAttrValueInitializerCreator());
    assert(attrConfig);
    if (!mVirtualAttributeSchema)
    {
        mVirtualAttributeSchema.reset(new AttributeSchema);
    }
    mVirtualAttributeSchema->AddAttributeConfig(attrConfig);
}

void RegionSchemaImpl::AddPackAttributeConfig(
        const string& attrName, const vector<string>& subAttrNames,
        const string& compressTypeStr)
{
    AddPackAttributeConfig(attrName, subAttrNames,
                           compressTypeStr, FIELD_DEFAULT_DEFRAG_SLICE_PERCENT);
}
    
void RegionSchemaImpl::AddPackAttributeConfig(
        const string& attrName, const vector<string>& subAttrNames,
        const string& compressTypeStr, uint64_t defragSlicePercent)
{
    CompressTypeOption compressOption;
    compressOption.Init(compressTypeStr);

    PackAttributeConfigPtr packAttrConfig(new PackAttributeConfig(
                    attrName, compressOption, defragSlicePercent));
    for (size_t i = 0; i < subAttrNames.size(); ++i)
    {
        AttributeConfigPtr attrConfig = CreateAttributeConfig(subAttrNames[i]);
        packAttrConfig->AddAttributeConfig(attrConfig);
    }
    if (!mAttributeSchema)
    {
        mAttributeSchema.reset(new AttributeSchema);
    }
    mAttributeSchema->AddPackAttributeConfig(packAttrConfig);
}

void RegionSchemaImpl::AddSummaryConfig(const string& fieldName, summarygroupid_t summaryGroupId)
{
    FieldConfigPtr fieldConfig = mFieldSchema->GetFieldConfig(fieldName);
    if (!fieldConfig)
    {
        stringstream msg;
        msg << "No such field defined: fieldName:" << fieldName;
        INDEXLIB_FATAL_ERROR(Schema, "%s", msg.str().c_str());
    }
    if (!mSummarySchema)
    {
        mSummarySchema.reset(new SummarySchema);
    }
    SummaryConfigPtr summaryConfig(new SummaryConfig);
    summaryConfig->SetFieldConfig(fieldConfig);
    mSummarySchema->AddSummaryConfig(summaryConfig, summaryGroupId);
}

void RegionSchemaImpl::AddSummaryConfig(fieldid_t fieldId,
                                        summarygroupid_t summaryGroupId)
{
    FieldConfigPtr fieldConfig = mFieldSchema->GetFieldConfig(fieldId);
    if (!fieldConfig)
    {
        stringstream msg;
        msg << "No such field defined: fieldId:" << fieldId;
        INDEXLIB_FATAL_ERROR(Schema, "%s", msg.str().c_str());
    }
    if (!mSummarySchema)
    {
        mSummarySchema.reset(new SummarySchema);
    }
    SummaryConfigPtr summaryConfig(new SummaryConfig);
    summaryConfig->SetFieldConfig(fieldConfig);
    mSummarySchema->AddSummaryConfig(summaryConfig, summaryGroupId);
}

void RegionSchemaImpl::SetSummaryCompress(bool compress, const string& compressType,
                                          summarygroupid_t summaryGroupId)
{
    if (!mSummarySchema)
    {
        mSummarySchema.reset(new SummarySchema);
    }
    const SummaryGroupConfigPtr summaryGroupConfig =
        mSummarySchema->GetSummaryGroupConfig(summaryGroupId);
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
    if (!fieldConfig)
    {
        INDEXLIB_FATAL_ERROR(Schema, "No such field defined: fieldName:%s",
                       fieldName.c_str());
    }
    AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
    attrConfig->Init(fieldConfig, AttributeValueInitializerCreatorPtr(), customizedConfigs);
    return attrConfig;
}

AttributeConfigPtr RegionSchemaImpl::CreateVirtualAttributeConfig(
        const FieldConfigPtr& fieldConfig,
        const AttributeValueInitializerCreatorPtr& creator)
{
    FieldConfigPtr cloneFieldConfig(fieldConfig->Clone());
    cloneFieldConfig->SetFieldId(GetFieldIdForVirtualAttribute());
    const string& fieldName = cloneFieldConfig->GetFieldName();
    if (mAttributeSchema && mAttributeSchema->GetAttributeConfig(fieldName))
    {
        INDEXLIB_FATAL_ERROR(Schema,
                       "virtual attribute name[%s] already in attributeSchema",
                       fieldName.c_str());
    }

    AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_virtual));
    attrConfig->Init(cloneFieldConfig, creator);
    return attrConfig;
}

fieldid_t RegionSchemaImpl::GetFieldIdForVirtualAttribute() const
{
    fieldid_t fieldId = (fieldid_t)mFieldSchema->GetFieldCount();
    if (mVirtualAttributeSchema)
    {
        fieldId += mVirtualAttributeSchema->GetAttributeCount();
    }
    return fieldId;
}

void RegionSchemaImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON)
    {
        ToJson(json);
    }
    else
    {
        FromJson(json);
        LoadValueConfig();
        InitTruncateIndexConfigs();
        EnsureSpatialIndexWithAttribute();
    }
}

bool RegionSchemaImpl::TTLEnabled() const
{
    TableType tableType = mSchema->GetTableType();
    if (tableType == tt_kkv || tableType == tt_kv)
    {
        return mDefaultTTL != INVALID_TTL;
    }
    if (mAttributeSchema)
    {
        AttributeConfigPtr attrConfig =
            mAttributeSchema->GetAttributeConfig(mTTLFieldName);
        if (attrConfig && attrConfig->GetFieldType() == ft_uint32 && !attrConfig->IsMultiValue())
        {
            return true;
        }
    }
    return false;
}

int64_t RegionSchemaImpl::GetDefaultTTL() const
{
    return mDefaultTTL >=0 ? mDefaultTTL : DEFAULT_TIME_TO_LIVE;
}

void RegionSchemaImpl::LoadValueConfig()
{
    TableType tableType = mSchema->GetTableType();
    if (tableType != tt_kv && tableType != tt_kkv)
    {
        return;
    }
    
    if (!mAttributeSchema || mAttributeSchema->GetPackAttributeCount() > 0)
    {
        INDEXLIB_FATAL_ERROR(Schema, "%s region not support pack attribute", mRegionName.c_str());
    }

    if (!mIndexSchema)
    {
        INDEXLIB_FATAL_ERROR(Schema, "no index schema!");
    }
    SingleFieldIndexConfigPtr singleConfig = mIndexSchema->GetPrimaryKeyIndexConfig();
    vector<AttributeConfigPtr> attrConfigs;
    AttributeSchema::Iterator iter = mAttributeSchema->Begin();
    for (; iter != mAttributeSchema->End(); ++iter)
    {
        AttributeConfigPtr attrConfig = CreateAttributeConfig((*iter)->GetAttrName());
        attrConfig->SetAttrId((*iter)->GetAttrId());
        attrConfigs.push_back(attrConfig);
    }

    if (tableType == tt_kkv)
    {
        KKVIndexConfigPtr kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, singleConfig);
        if (!kkvIndexConfig)
        {
            INDEXLIB_FATAL_ERROR(Schema, "index type [%s] not match with kkv table!",
                    IndexConfig::IndexTypeToStr(singleConfig->GetIndexType()));
        }
        
        if (kkvIndexConfig->GetSuffixFieldInfo().enableStoreOptimize)
        {
            OptimizeKKVSKeyStore(kkvIndexConfig, attrConfigs);
        }
    }
    ValueConfigPtr valueConfig(new ValueConfig());
    valueConfig->Init(attrConfigs);
    KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, singleConfig);
    if (!kvIndexConfig)
    {
        INDEXLIB_FATAL_ERROR(Schema, "index type [%s] not match with table type!",
                             IndexConfig::IndexTypeToStr(singleConfig->GetIndexType()));
    }
    kvIndexConfig->SetValueConfig(valueConfig);
}

void RegionSchemaImpl::OptimizeKKVSKeyStore(
        const KKVIndexConfigPtr& kkvIndexConfig, vector<AttributeConfigPtr>& attrConfigs)
{
    if (kkvIndexConfig->GetSuffixHashFunctionType() == hft_murmur)
    {
        return;
    }
    const FieldConfigPtr& skeyFieldConfig = kkvIndexConfig->GetSuffixFieldConfig();
    vector<AttributeConfigPtr> optAttrConfigs;
    for (size_t i = 0; i < attrConfigs.size(); i++)
    {
        if (attrConfigs[i]->GetAttrName() == skeyFieldConfig->GetFieldName())
        {
            continue;
        }
        optAttrConfigs.push_back(attrConfigs[i]);
    }
    // TODO: ValueConfig support empty attribute configs
    if (optAttrConfigs.empty())
    {
        return;
    }
    attrConfigs.swap(optAttrConfigs);
    kkvIndexConfig->SetOptimizedStoreSKey(true);
}

void RegionSchemaImpl::InitTruncateIndexConfigs()
{
    if (!mTruncateProfileSchema || mTruncateProfileSchema->Size() == 0)
    {
        return;
    }

    if (!mIndexSchema)
    {
        INDEXLIB_FATAL_ERROR(Schema, "no index schema!");
    }
    IndexSchema::Iterator it = mIndexSchema->Begin();
    for (; it != mIndexSchema->End(); ++it)
    {
        const IndexConfigPtr& indexConfig = *it;
        if (indexConfig->GetShardingType() == IndexConfig::IST_IS_SHARDING
            || !indexConfig->HasTruncate())
        {
            continue;
        }

        TruncateProfileSchema::Iterator pt = mTruncateProfileSchema->Begin();
        for (; pt != mTruncateProfileSchema->End(); ++pt)
        {
            const string& profileName = pt->first;
            if (indexConfig->HasTruncateProfile(profileName))
            {
                IndexConfigPtr truncateIndexConfig =
                    CreateTruncateIndexConfig(indexConfig, profileName);
                AddIndexConfig(truncateIndexConfig);
            }
        }
    }
}

IndexConfigPtr RegionSchemaImpl::CreateTruncateIndexConfig(
        const IndexConfigPtr& indexConfig, const string& profileName)
{
    assert(indexConfig);
    const string& indexName = indexConfig->GetIndexName();
    IndexConfigPtr truncateIndexConfig(indexConfig->Clone());
    truncateIndexConfig->SetIndexName(
            IndexConfig::CreateTruncateIndexName(indexName, profileName));
    truncateIndexConfig->SetVirtual(true);
    truncateIndexConfig->SetNonTruncateIndexName(indexName);
    truncateIndexConfig->SetHasTruncateFlag(false);

    if (indexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING)
    {
        const vector<IndexConfigPtr>& shardingIndexConfigs =
            indexConfig->GetShardingIndexConfigs();
        for (size_t i = 0; i < shardingIndexConfigs.size(); ++i)
        {
            IndexConfigPtr truncShardingIndexConfig =
                CreateTruncateIndexConfig(shardingIndexConfigs[i], profileName);
            truncateIndexConfig->AppendShardingIndexConfig(truncShardingIndexConfig);
        }
    }
    return truncateIndexConfig;
}

bool RegionSchemaImpl::IsUsefulField(const string& fieldName) const
{
    fieldid_t fieldId = mFieldSchema->GetFieldId(fieldName);
    if (fieldId == INVALID_FIELDID)
    {
        return false;
    }
    return (mIndexSchema && mIndexSchema->IsInIndex(fieldId))
        || (mSummarySchema && mSummarySchema->IsInSummary(fieldId))
        || (mAttributeSchema && mAttributeSchema->IsInAttribute(fieldId));
}

void RegionSchemaImpl::ToJson(Jsonizable::JsonWrapper& json)
{
    if (mOwnFieldSchema)
    {
        // mFieldSchema
        if (mFieldSchema) 
        {
            Any any = autil::legacy::ToJson(*mFieldSchema);
            JsonMap fieldMap = AnyCast<JsonMap>(any);
            json.Jsonize(FIELDS, fieldMap[FIELDS]);
        }
    }
    
    // mIndexSchema
    if (mIndexSchema)
    {
        Any any = autil::legacy::ToJson(*mIndexSchema);
        JsonMap indexMap = AnyCast<JsonMap>(any);
        json.Jsonize(INDEXS, indexMap[INDEXS]);
    }
    // mAttributeSchema
    if (mAttributeSchema)
    {
        Any any = autil::legacy::ToJson(*mAttributeSchema);
        JsonMap attributeMap = AnyCast<JsonMap>(any);
        json.Jsonize(ATTRIBUTES, attributeMap[ATTRIBUTES]);
    }
    // mSummarySchema
    if (mSummarySchema)
    {
        Any any = autil::legacy::ToJson(*mSummarySchema);
        JsonMap summaryMap = AnyCast<JsonMap>(any);
        json.Jsonize(SUMMARYS, summaryMap[SUMMARYS]);
    }
    // mTruncateProfileSchema
    if (mTruncateProfileSchema)
    {
        Any any = autil::legacy::ToJson(*mTruncateProfileSchema);
        JsonMap truncateProfileMap = AnyCast<JsonMap>(any);
        json.Jsonize(TRUNCATE_PROFILES, truncateProfileMap[TRUNCATE_PROFILES]);
    }
    if (TTLEnabled())
    {
        bool enableTTL = true;
        json.Jsonize(ENABLE_TTL, enableTTL);
        if (mDefaultTTL != INVALID_TTL)
        {
            json.Jsonize(DEFAULT_TTL, mDefaultTTL);            
        }
        if (mTTLFieldName != DOC_TIME_TO_LIVE_IN_SECONDS)
        {
            json.Jsonize(TTL_FIELD_NAME, mTTLFieldName);
        }
    }

    if (mRegionName != DEFAULT_REGIONNAME)
    {
        json.Jsonize(REGION_NAME, mRegionName);
    }
}

void RegionSchemaImpl::FromJson(Jsonizable::JsonWrapper& json)
{
    std::map<std::string, Any> jsonMap = json.GetMap();

    // parse fieldSchema
    auto iter = jsonMap.find(FIELDS);
    if (iter != jsonMap.end())
    {
        LoadFieldSchema(iter->second);
    }
    else
    {
        mFieldSchema = mSchema->GetFieldSchema();
    }

    if (!mFieldSchema)
    {
        if (mSchema->GetTableType() == tt_customized)
        {
            mFieldSchema.reset(new FieldSchema);
        }
        else
        {
            INDEXLIB_FATAL_ERROR(Schema, "no fields section defined");
        }
    }

    //parse truncate_profiles definition
    iter = jsonMap.find(TRUNCATE_PROFILES);
    if (iter != jsonMap.end())
    {
        LoadTruncateProfileSchema(jsonMap);
    }

    //parse indexs
    iter = jsonMap.find(INDEXS);
    if (iter != jsonMap.end())
    {
        LoadIndexSchema(iter->second);
    }
    else
    {
        IE_LOG(WARN, "no indexs section defined");
    }
        
    // parse attribute
    iter = jsonMap.find(ATTRIBUTES);
    if (iter != jsonMap.end())
    {
        LoadAttributeSchema(iter->second);
    }
    else 
    {
        IE_LOG(WARN, "no attributes section defined");
    }

    // parse summary
    iter = jsonMap.find(SUMMARYS);
    if (iter != jsonMap.end())
    {
        LoadSummarySchema(iter->second);
    }
    SetNeedStoreSummary();

    // parse ttl
    json.Jsonize(TTL_FIELD_NAME, mTTLFieldName, mTTLFieldName);
    iter = jsonMap.find(DEFAULT_TTL);
    if (iter != jsonMap.end())
    {
        int64_t defaultTTL = JsonNumberCast<int64_t>(iter->second);
        SetDefaultTTL(defaultTTL, mTTLFieldName);
    }
    iter = jsonMap.find(ENABLE_TTL);
    if (iter != jsonMap.end())
    {
        bool enableTTL = AnyCast<bool>(iter->second);
        SetEnableTTL(enableTTL, mTTLFieldName);
    }
    json.Jsonize(REGION_NAME, mRegionName, mRegionName);
}

void RegionSchemaImpl::LoadFieldSchema(const Any& any)
{
    if (!mMultiRegionFormat)
    {
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
    for (JsonArray::iterator it = indexs.begin(); it != indexs.end(); ++it) 
    {
        IndexConfigPtr indexConfig = IndexConfigCreator::Create(
                mFieldSchema, mSchema->GetDictSchema(), 
                mSchema->GetAdaptiveDictSchema(), *it, mSchema->GetTableType());
        AddIndexConfig(indexConfig);
    }
}

void RegionSchemaImpl::LoadAttributeSchema(const Any& any) 
{
    JsonArray attrs = AnyCast<JsonArray>(any);
    for (JsonArray::iterator iter = attrs.begin(); iter != attrs.end(); ++iter)
    {
        if (iter->GetType() == typeid(JsonMap))
        {
            LoadAttributeConfig(*iter);
        }
        else
        {
            string fieldName = AnyCast<string>(*iter);
            AddAttributeConfig(fieldName);
        }
    }
}

void RegionSchemaImpl::LoadAttributeConfig(const Any& any)
{
    JsonMap attribute = AnyCast<JsonMap>(any);
    auto fieldNameIter = attribute.find(FIELD_NAME);
    auto customizedConfigIter = attribute.find(CUSTOMIZED_CONFIG);
    if (fieldNameIter == attribute.end() ||
        customizedConfigIter == attribute.end())
    {
        LoadPackAttributeConfig(any);        
    }
    else
    {
        // load as customized attribute
        string fieldName = AnyCast<string>(fieldNameIter->second);
        JsonArray customizedConfigVec = AnyCast<JsonArray>(customizedConfigIter->second);
        CustomizedConfigVector customizedConfigs;
        for (JsonArray::iterator configIter = customizedConfigVec.begin(); 
             configIter != customizedConfigVec.end(); ++configIter)
        {
            JsonMap customizedConfigMap = AnyCast<JsonMap>(*configIter);
            Jsonizable::JsonWrapper jsonWrapper(customizedConfigMap);
            CustomizedConfigPtr customizedConfig(new CustomizedConfig());
            customizedConfig->Jsonize(jsonWrapper);
            customizedConfigs.push_back(customizedConfig);
        }
        AddAttributeConfig(fieldName, customizedConfigs);        
    }
}

void RegionSchemaImpl::LoadPackAttributeConfig(const Any& any)
{
    JsonMap packAttr = AnyCast<JsonMap>(any);
    if (packAttr.find(PACK_ATTR_NAME_FIELD) == packAttr.end())
    {
        INDEXLIB_FATAL_ERROR(Schema, "pack attribute name undefined.");
    }
    if (packAttr.find(PACK_ATTR_SUB_ATTR_FIELD) == packAttr.end())
    {
        INDEXLIB_FATAL_ERROR(Schema, "sub attribute names undefined.");
    }
    string packName = AnyCast<string>(packAttr[PACK_ATTR_NAME_FIELD]);
    vector<string> subAttrNames;
    JsonArray subAttrs = AnyCast<JsonArray>(packAttr[PACK_ATTR_SUB_ATTR_FIELD]);
    for (JsonArray::iterator it = subAttrs.begin(); it != subAttrs.end(); ++it)
    {
        string subAttrName = AnyCast<string>(*it);
        subAttrNames.push_back(subAttrName);
    }
    if (subAttrNames.empty())
    {
        INDEXLIB_FATAL_ERROR(Schema, "sub attribute names undefined.");
    }
    string compressType = "";
    if (packAttr.find(PACK_ATTR_COMPRESS_TYPE_FIELD) != packAttr.end())
    {
        compressType = AnyCast<string>(packAttr[PACK_ATTR_COMPRESS_TYPE_FIELD]);
    }
    uint64_t defragSlicePercent = FIELD_DEFAULT_DEFRAG_SLICE_PERCENT;
    if (packAttr.find(FIELD_DEFRAG_SLICE_PERCENT) != packAttr.end())
    {
        defragSlicePercent = AnyCast<uint64_t>(packAttr[FIELD_DEFRAG_SLICE_PERCENT]);
    }
    AddPackAttributeConfig(packName, subAttrNames, compressType, defragSlicePercent);
}

bool RegionSchemaImpl::LoadSummaryGroup(const Any& any, summarygroupid_t summaryGroupId)
{
    JsonMap summary = AnyCast<JsonMap>(any);
    JsonMap::const_iterator it = summary.find(SUMMARY_FIELDS);
    if (it == summary.end())
    {
        IE_LOG(WARN, "summary can not be empty");
        return false;
    }

    JsonArray summaryFields = AnyCast<JsonArray>(it->second);
    if (summaryFields.size() < 1)
    {
        IE_LOG(WARN, "summarys can not be empty");
        return false;
    }
    for (JsonArray::iterator iter = summaryFields.begin(); 
         iter != summaryFields.end(); ++iter)
    {
        string fieldName = AnyCast<string>(*iter);        
        AddSummaryConfig(fieldName, summaryGroupId);
    }
    
    //if has summary fields, compress default is false
    it = summary.find(SUMMARY_COMPRESS);
    if(it == summary.end())
    {
        SetSummaryCompress(false, "", summaryGroupId);
        IE_LOG(INFO, "compress is not set, default set to : false");
    }
    else
    {
        bool useCompress = AnyCast<bool>(it->second);
        string compressType = "";
        if (useCompress)
        {
            it = summary.find(SUMMARY_COMPRESS_TYPE);
            if (it != summary.end())
            {
                compressType = AnyCast<string>(it->second);
            }
        }
        SetSummaryCompress(useCompress, compressType, summaryGroupId);
    }
    return true;
}

void RegionSchemaImpl::LoadSummarySchema(const Any& any)
{
    if (!LoadSummaryGroup(any, DEFAULT_SUMMARYGROUPID))
    {
        return;
    }
    // non-default summary group
    JsonMap summary = AnyCast<JsonMap>(any);
    JsonMap::const_iterator it = summary.find(SUMMARY_GROUPS);
    if (it != summary.end())
    {
        JsonArray summaryGroups = AnyCast<JsonArray>(it->second);
        for (JsonArray::iterator iter = summaryGroups.begin(); 
             iter != summaryGroups.end(); ++iter)
        {
            JsonMap group = AnyCast<JsonMap>(*iter);
            JsonMap::const_iterator groupIt = group.find(SUMMARY_GROUP_NAME);
            if (groupIt == group.end())
            {
                INDEXLIB_FATAL_ERROR(Schema, "summary group has no name");
            }
            string groupName = AnyCast<string>(groupIt->second);
            summarygroupid_t groupId = CreateSummaryGroup(groupName);
            if (!LoadSummaryGroup(group, groupId))
            {
                INDEXLIB_FATAL_ERROR(Schema, "load summary groupName[%s] failed",
                        groupName.c_str());
            }
        }
    }
}

void RegionSchemaImpl::SetNeedStoreSummary()
{
    if (!mSummarySchema)
    {
        return;
    }
    if (!mAttributeSchema)
    {
        mSummarySchema->SetNeedStoreSummary(true);
        return;
    }
    mSummarySchema->SetNeedStoreSummary(false);
    SummarySchema::Iterator it;
    for (it = mSummarySchema->Begin(); it != mSummarySchema->End(); it++)
    {
        fieldid_t fieldId = (*it)->GetFieldConfig()->GetFieldId();
        if (!mAttributeSchema->IsInAttribute(fieldId))
        {
            mSummarySchema->SetNeedStoreSummary(fieldId);
        }
    }
}

void RegionSchemaImpl::SetDefaultTTL(int64_t defaultTTL, const string& fieldName)
{
    if (defaultTTL >= 0)
    {
        SetEnableTTL(true, fieldName);
        mDefaultTTL = defaultTTL;
    }
    else
    {
        SetEnableTTL(false, fieldName);
    }
}

void RegionSchemaImpl::SetEnableTTL(bool enableTTL, const string& fieldName)
{
    if (!enableTTL)
    {
        mDefaultTTL = INVALID_TTL;
        return;
    }
    
    if (mDefaultTTL == INVALID_TTL)
    {
        mDefaultTTL = DEFAULT_TIME_TO_LIVE;
    }

    TableType tableType = mSchema->GetTableType();
    if (tableType == tt_kv || tableType == tt_kkv)
    {
        return;
    }
    // only for index table
    // add doc_time_to_live_in_seconds to attribute
    mTTLFieldName = fieldName;
    if (!mAttributeSchema || ! mAttributeSchema->GetAttributeConfig(mTTLFieldName))
    {
        FieldConfigPtr fieldConfig = mFieldSchema->GetFieldConfig(mTTLFieldName);
        if (!fieldConfig)
        {
            fieldConfig.reset(new FieldConfig(mTTLFieldName, ft_uint32, false));
            mFieldSchema->AddFieldConfig(fieldConfig);
        }
        AttributeConfigPtr attrConfig(new AttributeConfig);
        attrConfig->Init(fieldConfig);
        if (!mAttributeSchema)
        {
            mAttributeSchema.reset(new AttributeSchema);
        }
        mAttributeSchema->AddAttributeConfig(attrConfig);
    }

    AttributeConfigPtr attrConfig =
        mAttributeSchema->GetAttributeConfig(mTTLFieldName);
    if (attrConfig)
    {
        // check
        if (attrConfig->GetFieldType() != ft_uint32 || attrConfig->IsMultiValue())
        {
            INDEXLIB_FATAL_ERROR(Schema,
                           "ttl field config should be ft_uint32 and single value");
        }
        return;
    }
}

void RegionSchemaImpl::CloneVirtualAttributes(const RegionSchemaImpl& other)
{
    assert(!mVirtualAttributeSchema);
    const AttributeSchemaPtr& virtualAttributeSchema = other.GetVirtualAttributeSchema();
    if (virtualAttributeSchema)
    {
        AttributeSchema::Iterator iter = virtualAttributeSchema->Begin();
        for (; iter != virtualAttributeSchema->End(); iter++)
        {
            const AttributeConfigPtr& virAttrConfig = *iter;
            AddVirtualAttributeConfig(virAttrConfig);
        }
    }
}

bool RegionSchemaImpl::AddVirtualAttributeConfigs(const AttributeConfigVector& virtualAttrConfigs)
{
    bool hasNewVirtualAttribute = false;
    for (size_t i = 0; i < virtualAttrConfigs.size(); i++)
    {
        const AttributeConfigPtr& attrConfig = virtualAttrConfigs[i];
        if (mVirtualAttributeSchema &&
            mVirtualAttributeSchema->IsInAttribute(attrConfig->GetAttrName()))
        {
            continue;
        }
        AddVirtualAttributeConfig(attrConfig);
        hasNewVirtualAttribute = true;
    }
    return hasNewVirtualAttribute;
}

void RegionSchemaImpl::AssertEqual(const RegionSchemaImpl& other) const
{
    if (mRegionName != other.mRegionName)
    {
        INDEXLIB_FATAL_ERROR(AssertEqual, "region name is not equal");
    }

#define REGION_ITEM_ASSERT_EQUAL(schemaItemPtr, exceptionMsg)           \
    if (schemaItemPtr.get() != NULL && other.schemaItemPtr.get() != NULL) \
    {                                                                   \
        schemaItemPtr->AssertEqual(*(other.schemaItemPtr));             \
    }                                                                   \
    else if (schemaItemPtr.get() != NULL || other.schemaItemPtr.get() != NULL) \
    {                                                                   \
        INDEXLIB_FATAL_ERROR(AssertEqual, exceptionMsg);       \
    }                                                                   \

    // mFieldSchema
    REGION_ITEM_ASSERT_EQUAL(mFieldSchema,
                             "Field schema is not equal");
    // mIndexSchema
    REGION_ITEM_ASSERT_EQUAL(mIndexSchema,
                             "Index schema is not equal");
    // mAttributeSchema
    REGION_ITEM_ASSERT_EQUAL(mAttributeSchema,
                             "Attribute schema is not equal");
    // mVirtualAttributeSchema
    REGION_ITEM_ASSERT_EQUAL(mVirtualAttributeSchema,
                             "Virtual Attribute schema is not equal");
    // mSummarySchema
    REGION_ITEM_ASSERT_EQUAL(mSummarySchema,
                             "Summary schema is not equal");

#undef REGION_ITEM_ASSERT_EQUAL

}

void RegionSchemaImpl::AssertCompatible(const RegionSchemaImpl& other) const
{
    if (mRegionName != other.mRegionName)
    {
        INDEXLIB_FATAL_ERROR(AssertCompatible, "region name is not compatible");
    }

    // mFieldSchema
    if (mFieldSchema  && other.mFieldSchema)
    {
        mFieldSchema->AssertCompatible(*(other.mFieldSchema));
    }
    else if (!other.mFieldSchema && mFieldSchema)
    {
        INDEXLIB_FATAL_ERROR(AssertCompatible,
                       "field schema in region [%s] is not compatible",
                       mRegionName.c_str());
    }
    
    // mIndexSchema
    if (mIndexSchema  && other.mIndexSchema)
    {
        mIndexSchema->AssertCompatible(*(other.mIndexSchema));
    }
    else if (!other.mIndexSchema && mIndexSchema)
    {
        INDEXLIB_FATAL_ERROR(AssertCompatible, "Index schema is not compatible");
    }

    // mAttributeSchema
    if (mAttributeSchema  && other.mAttributeSchema )
    {
        mAttributeSchema->AssertCompatible(*(other.mAttributeSchema));
    }
    else if (!other.mAttributeSchema && mAttributeSchema)
    {
        INDEXLIB_FATAL_ERROR(AssertCompatible, "Attribute schema is not compatible");
    }

    // mSummaryinfos
    if (mSummarySchema  && other.mSummarySchema)
    {
        mSummarySchema->AssertCompatible(*(other.mSummarySchema));
    }
    else if (!other.mSummarySchema && mSummarySchema)
    {
        INDEXLIB_FATAL_ERROR(AssertCompatible, "Summary schema is not compatible");
    }

    // mTruncateProfileSchema is allways compatible
}

void RegionSchemaImpl::CheckFieldSchema() const
{
    // fixed_multi_value_type is temporarily supported in kv_table only
    FieldSchema::Iterator iter = mFieldSchema->Begin();
    for (; iter != mFieldSchema->End();  ++iter)
    {
        if ((*iter)->IsMultiValue() && (*iter)->GetFixedMultiValueCount() != -1)
        {
            if (mSchema->GetTableType() != tt_kv &&
                mSchema->GetTableType() != tt_kkv &&
                mSchema->GetTableType() != tt_index)
            {
                INDEXLIB_FATAL_ERROR(Schema, "table type [%d] does not"
                        " support fixed_multi_value_count", int(mSchema->GetTableType()));
            }
        }
    }
}

void RegionSchemaImpl::CheckIndexSchema() const
{
    mIndexSchema->Check();
    TableType tableType = mSchema->GetTableType();
    if (tableType == tt_kv &&
        (!mIndexSchema->GetPrimaryKeyIndexConfig() || mIndexSchema->GetPrimaryKeyIndexType() != it_kv))
    {
        INDEXLIB_FATAL_ERROR(Schema, "table type [kv] not match with index define");
    }
    if (tableType == tt_kkv &&
        (!mIndexSchema->GetPrimaryKeyIndexConfig() || mIndexSchema->GetPrimaryKeyIndexType() != it_kkv))
    {
        INDEXLIB_FATAL_ERROR(Schema, "table type [kkv] not match with index define");
    }
    //kv kkv should not support customization
    if ((tableType == tt_kv || tableType == tt_kkv) &&
        mIndexSchema->GetPrimaryKeyIndexConfig()->GetCustomizedConfigs().size() > 0)
    {
        INDEXLIB_FATAL_ERROR(Schema, "kv or kkv table does not support index customization");
    }

    //TODO: refine move to index schema check
    uint32_t fieldCount = mFieldSchema->GetFieldCount();

    vector<uint32_t> singleFieldIndexCounts(fieldCount, 0);
    IndexSchema::Iterator it = mIndexSchema->Begin();
    for (; it != mIndexSchema->End(); ++it)
    {
        IndexConfigPtr indexConfig = *it;
        if (indexConfig->IsDeleted())
        {
            continue;
        }
        
        IndexType indexType = indexConfig->GetIndexType();
        if (indexType == it_pack || indexType == it_expack
            || indexType == it_customized)
        {
            CheckFieldsOrderInPackIndex(indexConfig);
        }
        else
        {
            CheckSingleFieldIndex(indexConfig, singleFieldIndexCounts);
        }
        if (indexConfig->HasTruncate())
        {
            CheckIndexTruncateProfiles(indexConfig);
        }

        if (indexType == it_spatial)
        {
            CheckSpatialIndexConfig(indexConfig);
        }
    }
}

void RegionSchemaImpl::CheckSingleFieldIndex(const IndexConfigPtr& indexConfig,
        vector<uint32_t>& singleFieldIndexCounts) const
{
    FieldSchema::Iterator fieldIt = mFieldSchema->Begin();
    for (; fieldIt != mFieldSchema->End(); fieldIt++)
    {
        FieldConfigPtr fieldConfig = *fieldIt;
        fieldid_t fieldId = fieldConfig->GetFieldId();
        if (!indexConfig->IsInIndex(fieldId))
        {
            continue;
        }

        if (indexConfig->GetShardingType() == IndexConfig::IST_IS_SHARDING)
        {
            continue;
        }

        singleFieldIndexCounts[fieldId]++;
        if(singleFieldIndexCounts[fieldId] > 1)
        {
            stringstream ss;
            ss << "Field "
               << fieldConfig->GetFieldName()
               << " has more than one single field index.";
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
    }
}

void RegionSchemaImpl::CheckSpatialIndexConfig(const IndexConfigPtr& indexConfig) const
{
    assert(indexConfig->GetIndexType() == it_spatial);
    SpatialIndexConfigPtr spatialIndexConf = DYNAMIC_POINTER_CAST(
            SpatialIndexConfig, indexConfig);
    assert(spatialIndexConf);

    FieldConfigPtr fieldConfig = spatialIndexConf->GetFieldConfig();
    assert(fieldConfig);
    //TODO: support line and polygon
    if (fieldConfig->GetFieldType() != ft_location)
    {
        return;
    }
    std::string fieldName = fieldConfig->GetFieldName();
    AttributeConfigPtr attrConf;
    if (mAttributeSchema)
    {
        attrConf = mAttributeSchema->GetAttributeConfig(fieldName);
    }
    if (!attrConf)
    {
        INDEXLIB_FATAL_ERROR(Schema,
                             "field [%s] should in attributes, because in spatial index [%s]",
                             fieldName.c_str(), spatialIndexConf->GetIndexName().c_str());
    }
    if (attrConf->GetPackAttributeConfig())
    {
        INDEXLIB_FATAL_ERROR(Schema,
                             "field [%s] should not in pack attribute, because in spatial index [%s]",
                             fieldName.c_str(), spatialIndexConf->GetIndexName().c_str());
    }
}

void RegionSchemaImpl::CheckFieldsOrderInPackIndex(const IndexConfigPtr& indexConfig) const
{
    PackageIndexConfigPtr packageConfig =
        dynamic_pointer_cast<PackageIndexConfig>(indexConfig);
    FieldSchema::Iterator fieldIt = mFieldSchema->Begin();
    int32_t lastFieldPosition = -1;
    fieldid_t lastFieldId = -1;
    for (; fieldIt != mFieldSchema->End(); fieldIt++)
    {
        FieldConfigPtr fieldConfig = *fieldIt;
        fieldid_t fieldId = fieldConfig->GetFieldId();
        if (!packageConfig->IsInIndex(fieldId))
        {
            continue;
        }

        int32_t curFieldPosition = packageConfig->GetFieldIdxInPack(fieldId);
        if (curFieldPosition < lastFieldPosition)
        {
            string beforeFieldName = mFieldSchema->GetFieldConfig(lastFieldId)->GetFieldName();
            string afterFieldName = mFieldSchema->GetFieldConfig(fieldId)->GetFieldName();

            stringstream ss;
            ss << "wrong field order in IndexConfig '"
               << indexConfig->GetIndexName()
               << "': expect field '" << beforeFieldName
               << "' before field '" << afterFieldName
               << "', but found '" << afterFieldName
               << "' before '" << beforeFieldName << "'";
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
        lastFieldPosition = curFieldPosition;
        lastFieldId = fieldId;
    }
}

void RegionSchemaImpl::CheckTruncateSortParams() const
{
    for (TruncateProfileSchema::Iterator it = mTruncateProfileSchema->Begin();
         it != mTruncateProfileSchema->End(); ++it)
    {
        const SortParams& sortParams = it->second->GetTruncateSortParams();
        for (SortParams::const_iterator sortParamIt = sortParams.begin();
             sortParamIt != sortParams.end(); ++sortParamIt)
        {
            if (DOC_PAYLOAD_FIELD_NAME == sortParamIt->GetSortField())
            {
                continue;
            }
            FieldConfigPtr fieldConfig = mFieldSchema->GetFieldConfig(sortParamIt->GetSortField());
            if (!fieldConfig)
            {
                INDEXLIB_FATAL_ERROR(Schema, "truncate sort field [%s] "
                        "is not in field schema",
                        sortParamIt->GetSortField().c_str());
            }

            AttributeConfigPtr attrConfig;
            if (mAttributeSchema)
            {
                attrConfig = mAttributeSchema->GetAttributeConfig(fieldConfig->GetFieldName());
            }
            if (!attrConfig)
            {
                INDEXLIB_FATAL_ERROR(Schema, "truncate sort field [%s]"
                        " has not corresponding attribute config",
                        fieldConfig->GetFieldName().c_str());
            }

            if (attrConfig->GetPackAttributeConfig() != NULL)
            {
                INDEXLIB_FATAL_ERROR(Schema, "truncate sort field [%s] "
                        "should not be in pack attribute.",
                        fieldConfig->GetFieldName().c_str());
            }
        }
    }
}

void RegionSchemaImpl::CheckIndexTruncateProfiles(const IndexConfigPtr& indexConfig) const
{
    if (!mTruncateProfileSchema)
    {
        INDEXLIB_FATAL_ERROR(Schema, "has no truncate profiles shcema.");
    }
    vector<string> profileNames = indexConfig->GetUseTruncateProfiles();
    for (size_t i = 0; i < profileNames.size(); ++i)
    {
        const string& profileName = profileNames[i];
        const string& indexName = indexConfig->GetIndexName();
        TruncateProfileConfigPtr profile = mTruncateProfileSchema->GetTruncateProfileConfig(profileName);
        if (!profile)
        {
            INDEXLIB_FATAL_ERROR(Schema,
                           "has no truncate profile name [%s] of index [%s]",
                           profileName.c_str(), indexName.c_str());
        }
        // truncate not support single compressed float
        const SortParams& sortParams = profile->GetTruncateSortParams();
        for (auto sortParam : sortParams)
        {
            string sortField = sortParam.GetSortField();
            FieldConfigPtr fieldConfig = mFieldSchema->GetFieldConfig(sortField);
            if (!fieldConfig) {
                // may be DOC_PAYLOAD
                return;
            }
            if (fieldConfig->GetFieldType() == FieldType::ft_fp16 ||
                fieldConfig->GetFieldType() == FieldType::ft_fp8)
            {
                INDEXLIB_FATAL_ERROR(Schema,
                                     "invalid field[%s] for truncate profile name [%s] of index [%s]",
                                     fieldConfig->GetFieldName().c_str(),
                                     profileName.c_str(), indexName.c_str());
            }
            CompressTypeOption compress = fieldConfig->GetCompressType();
            if (compress.HasFp16EncodeCompress() || compress.HasInt8EncodeCompress())
            {
                INDEXLIB_FATAL_ERROR(Schema,
                                     "invalid field[%s] for  truncate profile name [%s] of index [%s]",
                                     fieldConfig->GetFieldName().c_str(),
                                     profileName.c_str(), indexName.c_str());
            }
        }
    }
}

void RegionSchemaImpl::Check() const
{
    TableType tableType = mSchema->GetTableType();
    if (tableType != tt_customized)
    {
        // non customized table
        if(!mFieldSchema)
        {
            stringstream ss;
            ss << "IndexPartitionSchema has no fieldSchema" << endl;
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
        CheckFieldSchema();
        if(!mIndexSchema)
        {
            stringstream ss;
            ss << "IndexPartitionSchema has no IndexSchema" << endl;
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
        CheckIndexSchema();
        if (mTruncateProfileSchema)
        {
            CheckTruncateSortParams();
        }

        if (mAttributeSchema)
        {
            CheckAttributeSchema();
        }
        return;
    }

    // customized table with fieldSchema
    if (mFieldSchema)
    {
        CheckFieldSchema();
    }
    // with indexSchema
    if (mIndexSchema)
    {
        CheckIndexSchema();
    }
    // with trunProfile
    if (mTruncateProfileSchema)
    {
        CheckTruncateSortParams();
    }
    // with attributes
    if (mAttributeSchema)
    {
        CheckAttributeSchema();
    }
}

void RegionSchemaImpl::CheckAttributeSchema() const
{
    TableType tableType = mSchema->GetTableType();
    // kv kkv should not support field-level customization
    if (tableType == tt_kv || tableType == tt_kkv)
    {
        auto attrConfigIter = mAttributeSchema->Begin();
        while (attrConfigIter != mAttributeSchema->End())
        {
            if ((*attrConfigIter)->GetCustomizedConfigs().size() > 0)
            {
                INDEXLIB_FATAL_ERROR(Schema,
                               "kv or kkv table does not support attribute customization");
            }
            attrConfigIter++;
        }
    }
}

bool RegionSchemaImpl::SupportAutoUpdate() const
{
    TableType tableType = mSchema->GetTableType();
    if (tableType != tt_index)
    {
        return false;
    }
    
    if (NeedStoreSummary())
    {
        // has summary field
        return false;
    }

    if (!mIndexSchema || !mIndexSchema->HasPrimaryKeyIndex())
    {
        return false;
    }
    if (mIndexSchema->GetIndexCount() > 1)
    {
        // has index field besides pk
        return false;
    }

    if (mVirtualAttributeSchema)
    {
        // virtual attribute default value is unknown
        return false;
    }
    if (!mAttributeSchema)
    {
        return false;
    }

    AttributeConfigIteratorPtr attrConfigs = mAttributeSchema->CreateIterator(ConfigIteratorType::CIT_NORMAL);
    for (auto iter = attrConfigs->Begin(); iter != attrConfigs->End(); iter++)
    {
        const AttributeConfigPtr& attrConfig = *iter;
        assert(attrConfig);
        if (attrConfig->GetFieldId() == mIndexSchema->GetPrimaryKeyIndexFieldId())
        {
            continue;
        }
        if (!attrConfig->IsAttributeUpdatable())
        {
            // has unupdatable attribute field
            return false;
        }
    }
    return true;
}

FieldConfigPtr RegionSchemaImpl::AddFieldConfig(const string& fieldName, 
        FieldType fieldType, bool multiValue, bool isVirtual, bool isBinary)
{
    return FieldConfigLoader::AddFieldConfig(mFieldSchema, fieldName,
            fieldType, multiValue, isVirtual, isBinary);
}
    
EnumFieldConfigPtr RegionSchemaImpl::AddEnumFieldConfig(const string& fieldName, 
        FieldType fieldType, vector<string>& validValues, bool multiValue)
{
    return FieldConfigLoader::AddEnumFieldConfig(mFieldSchema, fieldName,
            fieldType, validValues, multiValue);
}

void RegionSchemaImpl::SetBaseSchemaImmutable()
{
    if (!mFieldSchema)
    {
        mFieldSchema.reset(new FieldSchema);
    }
    mFieldSchema->SetBaseSchemaImmutable();

    if (!mIndexSchema)
    {
        mIndexSchema.reset(new IndexSchema);
    }
    mIndexSchema->SetBaseSchemaImmutable();

    if (!mAttributeSchema)
    {
        mAttributeSchema.reset(new AttributeSchema);
    }
    mAttributeSchema->SetBaseSchemaImmutable();
}

void RegionSchemaImpl::SetModifySchemaImmutable()
{
    if (!mFieldSchema)
    {
        mFieldSchema.reset(new FieldSchema);
    }
    mFieldSchema->SetModifySchemaImmutable();

    if (!mIndexSchema)
    {
        mIndexSchema.reset(new IndexSchema);
    }
    mIndexSchema->SetModifySchemaImmutable();

    if (!mAttributeSchema)
    {
        mAttributeSchema.reset(new AttributeSchema);
    }
    mAttributeSchema->SetModifySchemaImmutable();
}

void RegionSchemaImpl::SetModifySchemaMutable()
{
    if (mFieldSchema)
    {
        mFieldSchema->SetModifySchemaMutable();        
    }

    if (mIndexSchema)
    {
        mIndexSchema->SetModifySchemaMutable();        
    }

    if (mAttributeSchema)
    {
        mAttributeSchema->SetModifySchemaMutable();        
    }
}

vector<AttributeConfigPtr> RegionSchemaImpl::EnsureSpatialIndexWithAttribute()
{
    vector<AttributeConfigPtr> ret;
    if (mSchema->GetTableType() == tt_customized)
    {
        return ret;
    }
    
    if (!mIndexSchema)
    {
        INDEXLIB_FATAL_ERROR(Schema, "no index schema!");
    }
    IndexSchema::Iterator it = mIndexSchema->Begin();
    for (; it != mIndexSchema->End(); ++it)
    {
        IndexConfigPtr indexConfig = *it;
        if (indexConfig->IsDeleted())
        {
            continue;
        }
        
        IndexType indexType = indexConfig->GetIndexType();
        if (indexType == it_spatial)
        {
            SpatialIndexConfigPtr spatialIndexConf = DYNAMIC_POINTER_CAST(
                    SpatialIndexConfig, indexConfig);
            assert(spatialIndexConf);

            FieldConfigPtr fieldConfig = spatialIndexConf->GetFieldConfig();
            if (fieldConfig->GetFieldType() != ft_location) {
                continue;
                //TODO: support polygon and line attribute
            }
            assert(fieldConfig);
            std::string fieldName = fieldConfig->GetFieldName();
            if (!mAttributeSchema || !mAttributeSchema->GetAttributeConfig(fieldName))
            {
                IE_LOG(INFO, "inner add attribute [%s] to ensure spatial index precision",
                       fieldName.c_str());
                AddAttributeConfig(fieldName);
                AttributeConfigPtr attrConfig = mAttributeSchema->GetAttributeConfig(fieldName);
                assert(attrConfig);
                attrConfig->SetConfigType(AttributeConfig::ct_index_accompany);
                ret.push_back(attrConfig);
            }
        }
    }
    return ret;
}

IE_NAMESPACE_END(config);
