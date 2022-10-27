#include <sstream>
#include <assert.h>
#include "indexlib/config/impl/index_partition_schema_impl.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/config/sort_param.h"
#include "indexlib/config/truncate_option_config.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/impl/kv_index_config_impl.h"
#include "indexlib/config/field_config_loader.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace std::tr1;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, IndexPartitionSchemaImpl);

IndexPartitionSchemaImpl::IndexPartitionSchemaImpl(const string& schemaName)
    : mSchemaName(schemaName)
    , mMaxModifyOperationCount(std::numeric_limits<uint32_t>::max())
    , mAutoUpdatePreference(true)
    , mThrowAssertCompatibleException(false)
    , mTableType(tt_index)
    , mDefaultRegionId(DEFAULT_REGIONID)
    , mSchemaId(DEFAULT_SCHEMAID)
{}

IndexPartitionSchemaImpl::~IndexPartitionSchemaImpl()
{}

DictionaryConfigPtr IndexPartitionSchemaImpl::AddDictionaryConfig(
        const string& dictName, const string& content)
{
    DictionaryConfigPtr dictConfig(new DictionaryConfig(dictName, content));
    if (mDictSchema.get() == NULL)
    {
        mDictSchema.reset(new DictionarySchema);
    }
    mDictSchema->AddDictionaryConfig(dictConfig);
    return dictConfig;
}

FieldConfigPtr IndexPartitionSchemaImpl::AddFieldConfig(
    const string& fieldName, FieldType fieldType,
    bool multiValue, bool isVirtual, bool isBinary)
{
    if (GetRegionCount() > 1)
    {
        IE_LOG(ERROR, "not support multi region schema");
        return FieldConfigPtr();
    }
    
    if (isBinary)
    {
        if (!multiValue || fieldType != ft_string)
        {
            IE_LOG(ERROR, "Only MultiString field support isBinary = true");
            isBinary = false;
        }
    }

    FieldConfigPtr fieldConfig(new FieldConfig(fieldName,
                    fieldType, multiValue, isVirtual, isBinary));
    if (mFieldSchema.get() == NULL)
    {
        mFieldSchema.reset(new FieldSchema);
    }
    mFieldSchema->AddFieldConfig(fieldConfig);
    return fieldConfig;
}

EnumFieldConfigPtr IndexPartitionSchemaImpl::AddEnumFieldConfig(const string& fieldName,
        FieldType fieldType, vector<string>& validValues, bool multiValue)
{
    if (GetRegionCount() > 1)
    {
        IE_LOG(ERROR, "not support multi region schema");
        return EnumFieldConfigPtr();
    }

    EnumFieldConfigPtr enumField(new EnumFieldConfig(fieldName, false));
    if (mFieldSchema.get() == NULL)
    {
        mFieldSchema.reset(new FieldSchema);
    }
    mFieldSchema->AddFieldConfig(static_pointer_cast<FieldConfig>(enumField));
    enumField->AddValidValue(validValues);
    return enumField;
}

void IndexPartitionSchemaImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON)
    {
        // mSchemaName
        string tableName = mSchemaName.empty() ? string("noname") : mSchemaName;
        json.Jsonize(TABLE_NAME, tableName);

        // mTableType
        string tableTypeStr = TableType2Str(mTableType);
        json.Jsonize(TABLE_TYPE, tableTypeStr);

        // customized config for table
        if (mCustomizedTableConfig)
        {
            json.Jsonize(CUSTOMIZED_TABLE_CONFIG, mCustomizedTableConfig);
        }
        if (mCustomizedDocumentConfigs.size() > 0)
        {
            vector<Any> anyVec;
            for (size_t i = 0; i < mCustomizedDocumentConfigs.size(); i++)
            {
                anyVec.push_back(ToJson(*mCustomizedDocumentConfigs[i]));
            }
            json.Jsonize(CUSTOMIZED_DOCUMENT_CONFIG, anyVec);            
        }

        // mUserDefinedParam
        if (!mUserDefinedParam.empty()) {
            json.Jsonize(TABLE_USER_DEFINED_PARAM, mUserDefinedParam);
        }

        // mGlobalRegionIndexPreference
        if (!mGlobalRegionIndexPreference.empty())
        {
            json.Jsonize(GLOBAL_REGION_INDEX_PREFERENCE, mGlobalRegionIndexPreference);
        }
        
        if (mAdaptiveDictSchema) // mAdaptiveDictSchema
        {
            Any any = ToJson(*mAdaptiveDictSchema);
            JsonMap jsonMap = AnyCast<JsonMap>(any);
            json.Jsonize(ADAPTIVE_DICTIONARIES, jsonMap[ADAPTIVE_DICTIONARIES]);
        }
        if (mDictSchema) //mDictSchema
        {
            Any any = ToJson(*mDictSchema);
            JsonMap jsonMap = AnyCast<JsonMap>(any);
            json.Jsonize(DICTIONARIES, jsonMap[DICTIONARIES]);
        }
        if (mFieldSchema) // mFieldSchema
        {
            Any any = ToJson(*mFieldSchema);
            JsonMap jsonMap = AnyCast<JsonMap>(any);
            json.Jsonize(FIELDS, jsonMap[FIELDS]);
        }

        if (mRegionVector.size() == 1)
        {
            mRegionVector[0]->Jsonize(json); // legacy format
        }
        else
        {
            vector<Any> anyVec;
            for (size_t i = 0; i < mRegionVector.size(); i++)
            {
                anyVec.push_back(ToJson(*mRegionVector[i]));
            }
            json.Jsonize(REGIONS, anyVec);
        }
        // mSubIndexPartitionSchema
        if (mSubIndexPartitionSchema.get() != NULL)
        {
            json.Jsonize(SUB_SCHEMA, *mSubIndexPartitionSchema);
        }
        if (!mAutoUpdatePreference)
        {
            json.Jsonize(AUTO_UPDATE_PREFERENCE, mAutoUpdatePreference);
        }

        if (mSchemaId != DEFAULT_SCHEMAID && mModifyOperations.empty())
        {
            json.Jsonize(SCHEMA_VERSION_ID, mSchemaId);
        }

        if (!mModifyOperations.empty())
        {
            json.Jsonize(SCHEMA_MODIFY_OPERATIONS, mModifyOperations);
        }

        if (mMaxModifyOperationCount != numeric_limits<uint32_t>::max())
        {
            json.Jsonize(MAX_SCHEMA_MODIFY_OPERATION_COUNT, mMaxModifyOperationCount);
        }
    }
    else
    {
        SchemaConfigurator configurator;
        JsonMap jsonMap = json.GetMap();
        configurator.Configurate(jsonMap, *this, false);
        
        JsonMap::iterator iter = jsonMap.find(SUB_SCHEMA);
        if (iter != jsonMap.end())
        {
            mSubIndexPartitionSchema.reset(new IndexPartitionSchema(""));
            configurator.Configurate(
                iter->second, *(mSubIndexPartitionSchema->GetImpl().get()), true);
            IE_LOG(INFO, "auto_update_preference will be set false"
                   " for main & sub schema scene");
            mAutoUpdatePreference = false;
            mSubIndexPartitionSchema->SetAutoUpdatePreference(false);
        }
        Check();
    }
}

void IndexPartitionSchemaImpl::AssertEqual(const IndexPartitionSchemaImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mSchemaName, other.mSchemaName, "Schema name not equal");
    IE_CONFIG_ASSERT_EQUAL(mSchemaId, other.mSchemaId, "Schema id not equal");

#define SCHEMA_ITEM_ASSERT_EQUAL(schemaItemPtr, exceptionMsg)           \
    if (schemaItemPtr.get() != NULL && other.schemaItemPtr.get() != NULL) \
    {                                                                   \
        schemaItemPtr->AssertEqual(*(other.schemaItemPtr));             \
    }                                                                   \
    else if (schemaItemPtr.get() != NULL || other.schemaItemPtr.get() != NULL) \
    {                                                                   \
        INDEXLIB_FATAL_ERROR(AssertEqual, exceptionMsg);                \
    }                                                                   \


    IE_CONFIG_ASSERT_EQUAL(mTableType, other.mTableType, "table type not equal");

    // mCustomizedTableConfig
    SCHEMA_ITEM_ASSERT_EQUAL(mCustomizedTableConfig,
                             "Customized Table Config is not equal");

    // mCustomizedDocumentConfigs
    for (size_t i = 0; i < mCustomizedDocumentConfigs.size(); i++)
    { 
        SCHEMA_ITEM_ASSERT_EQUAL(mCustomizedDocumentConfigs[i],
                                 "Customized Document Config is not equal");
    }

    // mAdaptiveDictSchema
    SCHEMA_ITEM_ASSERT_EQUAL(mAdaptiveDictSchema,
                             "Adaptive Dictionary schema is not equal");
    // mDictSchema
    SCHEMA_ITEM_ASSERT_EQUAL(mDictSchema,
                             "Dictionary schema is not equal");
    // mFieldSchema
    SCHEMA_ITEM_ASSERT_EQUAL(mFieldSchema,
                             "Field schema is not equal");

    // mSubIndexPartitionSchema
    SCHEMA_ITEM_ASSERT_EQUAL(mSubIndexPartitionSchema,
                             "sub schema is not equal");

    if (mRegionVector.size() != other.mRegionVector.size())
    {
        INDEXLIB_FATAL_ERROR(AssertEqual,
                       "region count is not equal");
    }
    for (size_t i = 0; i < mRegionVector.size(); i++)
    {
        SCHEMA_ITEM_ASSERT_EQUAL(mRegionVector[i], "inner region is not equal");
    }
    
    if (mAutoUpdatePreference != other.mAutoUpdatePreference)
    {
        INDEXLIB_FATAL_ERROR(AssertEqual,
                       "auto_update_preference is not equal");
    }

    if (mModifyOperations.size() != other.mModifyOperations.size())
    {
        INDEXLIB_FATAL_ERROR(AssertEqual, "modify_operations's size not equal");
    }

    for (size_t i = 0; i < mModifyOperations.size(); i++)
    {
        SCHEMA_ITEM_ASSERT_EQUAL(mModifyOperations[i], "modify_operations is not equal");
    }

#undef SCHEMA_ITEM_ASSERT_EQUAL
}

void IndexPartitionSchemaImpl::AssertCompatible(const IndexPartitionSchemaImpl& other) const
{
    try
    {
        DoAssertCompatible(other);
    }
    catch (const AssertCompatibleException& e)
    {
        IE_LOG(ERROR, "Schema is not compatible: %s", e.GetMessage().c_str());
        if (mThrowAssertCompatibleException)
        {
            throw e;
        }
    }
    catch (const ExceptionBase& e)
    {
        IE_LOG(ERROR, "Exception: %s", e.GetMessage().c_str());
        if (mThrowAssertCompatibleException)
        {
            throw e;
        }
    }
}

void IndexPartitionSchemaImpl::DoAssertCompatible(const IndexPartitionSchemaImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mSchemaName, other.mSchemaName, "Schema name not compatible");
    if (mModifyOperations.empty() && other.mModifyOperations.empty())
    {
        IE_CONFIG_ASSERT_EQUAL(mSchemaId, other.mSchemaId, "Schema id not compatible");
    }

    IE_CONFIG_ASSERT(mModifyOperations.size() <= other.mModifyOperations.size(),
                     "mModifyOperations's size not compatible");
    for (size_t i = 0; i < mModifyOperations.size(); i++)
    {
        mModifyOperations[i]->AssertEqual(*other.mModifyOperations[i]);
    }

    if (other.mModifyOperations.size() != mModifyOperations.size()) {
        IndexPartitionSchemaImplPtr base1(CreateBaseSchemaWithoutModifyOperation());
        IndexPartitionSchemaImplPtr base2(other.CreateBaseSchemaWithoutModifyOperation());
        try
        {
            base1->AssertEqual(*base2);
        }
        catch (const ExceptionBase& e)
        {
            IE_LOG(ERROR, "base schema not equal for modify operation, ExceptionBase: %s",
                   e.GetMessage().c_str());
            INDEXLIB_FATAL_ERROR(AssertCompatible, "base schema without modify opertion not equal");
        }
        return;
    }

#define SCHEMA_ITEM_ASSERT_COMPATIBLE(schemaItemPtr, exceptionMsg)      \
    if (schemaItemPtr && other.schemaItemPtr)                           \
    {                                                                   \
        schemaItemPtr->AssertCompatible(*(other.schemaItemPtr));        \
    }                                                                   \
    else if (schemaItemPtr && !other.schemaItemPtr)                     \
    {                                                                   \
        INDEXLIB_FATAL_ERROR(AssertCompatible, exceptionMsg);  \
    }                                                                   \

    // mAdaptiveDictSchema
    SCHEMA_ITEM_ASSERT_COMPATIBLE(mAdaptiveDictSchema,
                                  "Adaptive Dictionary schema is not compatible");
    // mDictSchema
    SCHEMA_ITEM_ASSERT_COMPATIBLE(mDictSchema,
                                  "Dictionary schema is not compatible");

    // mFieldSchema
    SCHEMA_ITEM_ASSERT_COMPATIBLE(mFieldSchema,
                                  "Field schema is not compatible");

    // mSubIndexPartitionSchema
    SCHEMA_ITEM_ASSERT_COMPATIBLE(mSubIndexPartitionSchema,
                                  "sub schema is not compatible");

    if (mRegionVector.size() != other.mRegionVector.size())
    {
        INDEXLIB_FATAL_ERROR(AssertCompatible,
                       "region count is not compatible");
    }
    for (size_t i = 0; i < mRegionVector.size(); i++)
    {
        SCHEMA_ITEM_ASSERT_COMPATIBLE(mRegionVector[i], "inner region is not compatible");
    }

#undef SCHEMA_ITEM_ASSERT_COMPATIBLE
    // mTruncateProfileSchema is allways compatible
}

void IndexPartitionSchemaImpl::CheckIndexNameNotSummary(const IndexConfigPtr& indexConfig) const
{
    string indexName = indexConfig->GetIndexName();
    if (indexName == "summary")
    {
        stringstream ss;
        ss << "One index's name is \"summary\", it is forbidden." << endl;
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }
}


void IndexPartitionSchemaImpl::CheckSubSchema() const
{
    if (!mSubIndexPartitionSchema)
    {
        return;
    }

    if (mTableType != tt_index)
    {
        INDEXLIB_FATAL_ERROR(Schema, "sub table only support tt_index table");
    }

    if (mSubIndexPartitionSchema->HasModifyOperations())
    {
        INDEXLIB_FATAL_ERROR(Schema, "sub table not support has modify operations");
    }
    
    if (GetRegionCount() != 1 || mSubIndexPartitionSchema->GetRegionCount() != 1)
    {
        INDEXLIB_FATAL_ERROR(Schema, "sub table only support index table with default region");
    }
    
    mSubIndexPartitionSchema->Check();

    // TODO: sub schema not support summary right now
    if (mSubIndexPartitionSchema->GetSummarySchema())
    {
        INDEXLIB_FATAL_ERROR(Schema, "sub schema not support summary");
    }

    //check no duplicate field name in fieldschema
    FieldSchemaPtr subFieldSchema = mSubIndexPartitionSchema->GetFieldSchema();
    for (FieldSchema::Iterator iter = mFieldSchema->Begin();
         iter != mFieldSchema->End(); iter++)
    {
        string fieldName = (*iter)->GetFieldName();
        if (subFieldSchema->IsFieldNameInSchema(fieldName))
        {
            INDEXLIB_FATAL_ERROR(Schema, "duplicate field name:"
                           "[%s] in subschema", fieldName.c_str());
        }
    }

    //check no duplicate index name in index schema
    IndexSchemaPtr mainIndexSchema = GetIndexSchema();
    IndexSchemaPtr subIndexSchema = mSubIndexPartitionSchema->GetIndexSchema();
    for (IndexSchema::Iterator iter = mainIndexSchema->Begin();
         iter != mainIndexSchema->End(); iter++)
    {
        string indexName = (*iter)->GetIndexName();
        if (subIndexSchema->GetIndexConfig(indexName))
        {
            INDEXLIB_FATAL_ERROR(Schema, "duplicate index name:"
                           "[%s] in subschema", indexName.c_str());
        }
    }

    //check no duplicate pack attribute name in attribute schema
    AttributeSchemaPtr mainAttrSchema = GetAttributeSchema();
    AttributeSchemaPtr subAttrSchema = mSubIndexPartitionSchema->GetAttributeSchema();
    if (mainAttrSchema && subAttrSchema && subAttrSchema->GetPackAttributeCount() > 0)
    {
        size_t packCount = subAttrSchema->GetPackAttributeCount();
        for (packattrid_t packId = 0; packId < packCount; ++packId)
        {
            const PackAttributeConfigPtr& subPackConfig =
                subAttrSchema->GetPackAttributeConfig(packId);
            assert(subPackConfig);
            const string& subPackName = subPackConfig->GetAttrName();
            // duplicate pack name in main schema
            if (mainAttrSchema->GetPackAttributeConfig(subPackName))
            {
                INDEXLIB_FATAL_ERROR(Schema, "duplicate pack attribute name [%s] in sub schema",
                                     subPackName.c_str());
            }
        }
    }

    //check schema and subschema must has pk index
    if (!mainIndexSchema->HasPrimaryKeyIndex() || !subIndexSchema->HasPrimaryKeyIndex())
    {
        INDEXLIB_FATAL_ERROR(Schema, "main schema or sub schema has no pk index");
    }
}

void IndexPartitionSchemaImpl::CheckFieldSchema() const
{
    // fixed_multi_value_type is temporarily supported in kv_table only
    FieldSchema::Iterator iter = mFieldSchema->Begin();
    for (; iter != mFieldSchema->End();  ++iter)
    {
        if ((*iter)->IsMultiValue() && (*iter)->GetFixedMultiValueCount() != -1)
        {
            if (mTableType != tt_kv && mTableType != tt_index && mTableType != tt_kkv)
            {
                INDEXLIB_FATAL_ERROR(Schema, "table type [%d] does not"
                        " support fixed_multi_value_count", int(mTableType));
            }
        }
    }
}

void IndexPartitionSchemaImpl::Check() const
{
    if (mFieldSchema)
    {
        CheckFieldSchema();
    }
    
    if (mRegionVector.empty())
    {
        INDEXLIB_FATAL_ERROR(Schema, "no valid region schema!");
    }

    if (mRegionVector.size() > MAX_REGION_COUNT)
    {
        INDEXLIB_FATAL_ERROR(Schema, "region count over limit [%u]", MAX_REGION_COUNT);
    }

    if (mTableType != tt_kv && mTableType != tt_kkv && mRegionVector.size() > 1)
    {
        INDEXLIB_FATAL_ERROR(Schema, "only kv & kkv table support multi region!");
    }

    for (size_t i = 0; i < mRegionVector.size(); i++)
    {
        if (!mRegionVector[i])
        {
            INDEXLIB_FATAL_ERROR(Schema, "region schema [%lu] is NULL!", i);
        }
        mRegionVector[i]->Check();
    }
    for (auto &op : mModifyOperations)
    {
        op->Validate();
    }
    CheckSubSchema();
}

IndexPartitionSchemaImpl* IndexPartitionSchemaImpl::Clone() const
{
    unique_ptr<IndexPartitionSchemaImpl> ptr;
    IndexPartitionSchemaImpl *schema = new IndexPartitionSchemaImpl(this->GetSchemaName());
    ptr.reset(schema);

    autil::legacy::Any any = autil::legacy::ToJson(*this);
    FromJson(*schema, any);

    // not support clone virtual index & attribute
    return ptr.release();
}

IndexPartitionSchemaImpl* IndexPartitionSchemaImpl::CloneWithDefaultRegion(regionid_t regionId) const
{ 
    unique_ptr<IndexPartitionSchemaImpl> ptr;
    ptr.reset(new IndexPartitionSchemaImpl(*this));
    ptr->SetDefaultRegionId(regionId);

    if (mSubIndexPartitionSchema)
    {
        IndexPartitionSchemaPtr cloneSubSchema(
                mSubIndexPartitionSchema->CloneWithDefaultRegion(regionId));
        ptr->SetSubIndexPartitionSchema(cloneSubSchema);
    }
    return ptr.release();
}

void IndexPartitionSchemaImpl::CloneVirtualAttributes(
        const IndexPartitionSchemaImpl* other)
{
    if (GetRegionCount() != other->GetRegionCount())
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "region count not equal!");
    }
    for (regionid_t i = 0; i < (regionid_t)GetRegionCount(); i++)
    {
        const RegionSchemaPtr& region = GetRegionSchema(i);
        region->CloneVirtualAttributes(*other->GetRegionSchema(i));
    }

    if (mSubIndexPartitionSchema && other->GetSubIndexPartitionSchema())
    {
        const IndexPartitionSchemaPtr &subSchema = other->GetSubIndexPartitionSchema();
        mSubIndexPartitionSchema->CloneVirtualAttributes(*(subSchema.get()));
    }
}

bool IndexPartitionSchemaImpl::AddVirtualAttributeConfigs(
        const AttributeConfigVector& virtualAttrConfigs, regionid_t id)
{
    const RegionSchemaPtr& region = GetRegionSchema(id);
    return region ? region->AddVirtualAttributeConfigs(virtualAttrConfigs) : false;
}

void IndexPartitionSchemaImpl::SetDefaultTTL(
        int64_t defaultTTL, regionid_t id, const string& ttlFieldName)
{
    const RegionSchemaPtr& region = GetRegionSchema(id);
    if (region)
    {
        region->SetDefaultTTL(defaultTTL, ttlFieldName);
    }
}

int64_t IndexPartitionSchemaImpl::GetDefaultTTL(regionid_t id) const
{
    const RegionSchemaPtr& region = GetRegionSchema(id);
    return region ? region->GetDefaultTTL() : DEFAULT_TIME_TO_LIVE;
}

void IndexPartitionSchemaImpl::SetEnableTTL(
        bool enableTTL, regionid_t id, const string& ttlFieldName)
{
    const RegionSchemaPtr& region = GetRegionSchema(id);
    if (region)
    {
        region->SetEnableTTL(enableTTL, ttlFieldName);
    }
}

const string& IndexPartitionSchemaImpl::GetTTLFieldName(regionid_t id)
{
    const RegionSchemaPtr& region = GetRegionSchema(id);
    if (region)
    {
        return region->GetTTLFieldName();
    }
    assert(false);
    static string emptyString;
    return emptyString;
}


bool IndexPartitionSchemaImpl::TTLEnabled(regionid_t id) const {
    const RegionSchemaPtr& region = GetRegionSchema(id);
    return region ? region->TTLEnabled() : false;
}

bool IndexPartitionSchemaImpl::IsUsefulField(const string& fieldName) const
{
    for (size_t i = 0; i < mRegionVector.size(); i++)
    {
        if (mRegionVector[i]->IsUsefulField(fieldName))
        {
            return true;
        }
    }
    return mSubIndexPartitionSchema && mSubIndexPartitionSchema->IsUsefulField(fieldName);
}

bool IndexPartitionSchemaImpl::SupportAutoUpdate() const
{
    if (!mAutoUpdatePreference)
    {
        return false;
    }

    for (regionid_t i = 0; i < (regionid_t)GetRegionCount(); i++)
    {
        if (!GetRegionSchema(i)->SupportAutoUpdate())
        {
            return false;
        }
    }
    if (mSubIndexPartitionSchema)
    {
        return mSubIndexPartitionSchema->SupportAutoUpdate();
    }
    return true;
}

TableType IndexPartitionSchemaImpl::GetTableType() const {
    return mTableType;
}

string IndexPartitionSchemaImpl::TableType2Str(TableType tableType) {
#define CONVERT_TABLE_TYPE(enum, enum_str)      \
    if (enum == tableType) {                    \
        return enum_str;                        \
    }
    CONVERT_TABLE_TYPE(tt_index, TABLE_TYPE_INDEX);
    CONVERT_TABLE_TYPE(tt_linedata, TABLE_TYPE_LINEDATA);
    CONVERT_TABLE_TYPE(tt_rawfile, TABLE_TYPE_RAWFILE);
    CONVERT_TABLE_TYPE(tt_kv, TABLE_TYPE_KV);
    CONVERT_TABLE_TYPE(tt_kkv, TABLE_TYPE_KKV);
    CONVERT_TABLE_TYPE(tt_customized, TABLE_TYPE_CUSTOMIZED);
    return TABLE_TYPE_INDEX;
}

void IndexPartitionSchemaImpl::SetTableType(const string &str) {
#define SET_TABLE_TYPE(enum, enum_str)          \
    if (str == enum_str) {                      \
        mTableType = enum;                      \
        return;                                 \
    }
    SET_TABLE_TYPE(tt_index, "");
    SET_TABLE_TYPE(tt_index, TABLE_TYPE_INDEX);
    if (TABLE_TYPE_RAWFILE == str) {
        mTableType = tt_rawfile;
        mFieldSchema.reset(new FieldSchema);
        mRegionIdMap.clear();
        mRegionVector.clear();
        RegionSchemaPtr region(new RegionSchema(this));
        AddRegionSchema(region);
        return;
    }
    if (TABLE_TYPE_LINEDATA == str) {
        mTableType = tt_linedata;
        mFieldSchema.reset(new FieldSchema);
        mRegionIdMap.clear();
        mRegionVector.clear();
        RegionSchemaPtr region(new RegionSchema(this));
        AddRegionSchema(region);
        return;
    }
    SET_TABLE_TYPE(tt_kv, TABLE_TYPE_KV);
    SET_TABLE_TYPE(tt_kkv, TABLE_TYPE_KKV);
    SET_TABLE_TYPE(tt_customized, TABLE_TYPE_CUSTOMIZED);
    INDEXLIB_FATAL_ERROR(Schema, "unexpected table type [%s]", str.c_str());
}

bool IndexPartitionSchemaImpl::GetValueFromUserDefinedParam(
        const string &key, string &value) const
{
    auto jsonMap = GetUserDefinedParam();
    auto it = jsonMap.find(key);
    if (jsonMap.end() == it) {
        IE_LOG(INFO, "[%s] not found in schema", key.c_str());
        return false;
    }
    try {
        value = AnyCast<string>(it->second);
    } catch (const exception &e) {
        IE_LOG(ERROR, "cast [%s] failed, exception happened [%s]",
               key.c_str(), e.what());
        return false;
    }
    return true;
}

void IndexPartitionSchemaImpl::AddRegionSchema(const RegionSchemaPtr& regionSchema)
{
    assert(regionSchema);
    const std::string& regionName = regionSchema->GetRegionName();
    RegionIdMap::const_iterator iter = mRegionIdMap.find(regionName);
    if (iter != mRegionIdMap.end())
    {
        INDEXLIB_FATAL_ERROR(Schema, "duplicate region_name [%s] in schema",
                             regionName.c_str());
    }
    regionid_t regionId = (regionid_t)mRegionVector.size();
    mRegionVector.push_back(regionSchema);
    mRegionIdMap[regionName] = regionId;
}

regionid_t IndexPartitionSchemaImpl::GetRegionId(const string& regionName) const
{
    RegionIdMap::const_iterator iter = mRegionIdMap.find(regionName);
    if (iter == mRegionIdMap.end())
    {
        return INVALID_REGIONID;
    }
    return iter->second;
}

const RegionSchemaPtr& IndexPartitionSchemaImpl::GetRegionSchema(const string& regionName) const
{
    regionid_t regionId = GetRegionId(regionName);
    if (regionId == INVALID_REGIONID)
    {
        static RegionSchemaPtr tmp;
        return tmp;
    }
    return GetRegionSchema(regionId);
}

const RegionSchemaPtr& IndexPartitionSchemaImpl::GetRegionSchema(regionid_t regionId) const
{
    if (regionId >= (regionid_t)mRegionVector.size())
    {
        static RegionSchemaPtr tmp;
        return tmp;
    }
    return mRegionVector[regionId];
}

const FieldSchemaPtr& IndexPartitionSchemaImpl::GetFieldSchema(regionid_t regionId) const
{
    const RegionSchemaPtr& region = GetRegionSchema(regionId);
    if (!region)
    {
        return mFieldSchema;
    }
    return region->GetFieldSchema();
}

const IndexSchemaPtr& IndexPartitionSchemaImpl::GetIndexSchema(regionid_t regionId) const
{
    const RegionSchemaPtr& region = GetRegionSchema(regionId);
    if (!region)
    {
        static IndexSchemaPtr tmp;
        return tmp;
    }
    return region->GetIndexSchema();
}
    
const AttributeSchemaPtr& IndexPartitionSchemaImpl::GetAttributeSchema(regionid_t regionId) const
{
    const RegionSchemaPtr& region = GetRegionSchema(regionId);
    if (!region)
    {
        static AttributeSchemaPtr tmp;
        return tmp;
    }
    return region->GetAttributeSchema();
}

const AttributeSchemaPtr& IndexPartitionSchemaImpl::GetVirtualAttributeSchema(regionid_t regionId) const
{
    const RegionSchemaPtr& region = GetRegionSchema(regionId);
    if (!region)
    {
        static AttributeSchemaPtr tmp;
        return tmp;
    }
    return region->GetVirtualAttributeSchema();
}

const SummarySchemaPtr& IndexPartitionSchemaImpl::GetSummarySchema(regionid_t regionId) const
{
    const RegionSchemaPtr& region = GetRegionSchema(regionId);
    if (!region)
    {
        static SummarySchemaPtr tmp;
        return tmp;
    }
    return region->GetSummarySchema();
}

const TruncateProfileSchemaPtr& IndexPartitionSchemaImpl::GetTruncateProfileSchema(
        regionid_t regionId) const
{
    const RegionSchemaPtr& region = GetRegionSchema(regionId);
    if (!region)
    {
        static TruncateProfileSchemaPtr tmp;
        return tmp;
    }
    return region->GetTruncateProfileSchema();
}

bool IndexPartitionSchemaImpl::NeedStoreSummary() const
{
    for (regionid_t i = 0; i < (regionid_t)GetRegionCount(); i++)
    {
        if (GetRegionSchema(i)->NeedStoreSummary())
        {
            return true;
        }
    }
    return false;
}

#define ADD_ITEM_IN_REGION_SCHEMA(Func, funcParam, id) \
    RegionSchemaPtr region = GetRegionSchema(id);      \
    if (!region && id == DEFAULT_REGIONID)             \
    {                                                  \
        region.reset(new RegionSchema(this));          \
        AddRegionSchema(region);                       \
    }                                                  \
    if (region)                                        \
    {                                                  \
        region->Func(funcParam);                       \
    }                                                  \
    
void IndexPartitionSchemaImpl::AddIndexConfig(const IndexConfigPtr& indexConfig, regionid_t id)
{
    ADD_ITEM_IN_REGION_SCHEMA(AddIndexConfig, indexConfig, id)
}

void IndexPartitionSchemaImpl::AddAttributeConfig(const string& fieldName, regionid_t id)
{
    ADD_ITEM_IN_REGION_SCHEMA(AddAttributeConfig, fieldName, id)
}

void IndexPartitionSchemaImpl::AddVirtualAttributeConfig(
        const AttributeConfigPtr& virtualAttrConfig, regionid_t id)
{
    ADD_ITEM_IN_REGION_SCHEMA(AddVirtualAttributeConfig, virtualAttrConfig, id)
}

void IndexPartitionSchemaImpl::SetTruncateProfileSchema(
        const TruncateProfileSchemaPtr& truncateProfileSchema, regionid_t id)
{
    ADD_ITEM_IN_REGION_SCHEMA(SetTruncateProfileSchema, truncateProfileSchema, id)
}

void IndexPartitionSchemaImpl::AddSummaryConfig(
        const string& fieldName, summarygroupid_t summaryGroupId, regionid_t id)
{
    RegionSchemaPtr region = GetRegionSchema(id);
    if (!region && id == DEFAULT_REGIONID)
    {
        region.reset(new RegionSchema(this));
        AddRegionSchema(region);
    }
    if (region)
    {
        region->AddSummaryConfig(fieldName, summaryGroupId);
    }
}

void IndexPartitionSchemaImpl::AddPackAttributeConfig(
        const string& attrName, const vector<string>& subAttrNames,
        const string& compressTypeStr, regionid_t id)
{
    RegionSchemaPtr region = GetRegionSchema(id);
    if (!region && id == DEFAULT_REGIONID)
    {
        region.reset(new RegionSchema(this));
        AddRegionSchema(region);
    }
    if (region)
    {
        region->AddPackAttributeConfig(attrName, subAttrNames, compressTypeStr);
    }
}

void IndexPartitionSchemaImpl::LoadValueConfig(regionid_t id)
{
    const RegionSchemaPtr& region = GetRegionSchema(id);
    if (region)
    {
        region->LoadValueConfig();
    }
}

void IndexPartitionSchemaImpl::ResetRegions()
{
    mRegionVector.clear();
    mRegionIdMap.clear();
}

void IndexPartitionSchemaImpl::LoadFieldSchema(const autil::legacy::Any& any)
{
    FieldConfigLoader::Load(any, mFieldSchema);
}

void IndexPartitionSchemaImpl::LoadFieldSchema(const autil::legacy::Any& any,
        std::vector<FieldConfigPtr>& fields)
{
    FieldConfigLoader::Load(any, mFieldSchema, fields);
}

void IndexPartitionSchemaImpl::SetDefaultRegionId(regionid_t regionId)
{
    if (regionId < 0 || regionId >= (regionid_t)mRegionVector.size())
    {
        IE_LOG(ERROR, "regionId [%d] out of range!", regionId);
        return;
    }
    mDefaultRegionId = regionId;
    if (mSubIndexPartitionSchema)
    {
        mSubIndexPartitionSchema->SetDefaultRegionId(regionId);
    }
}

bool IndexPartitionSchemaImpl::HasModifyOperations() const
{
    return !mModifyOperations.empty();
}

bool IndexPartitionSchemaImpl::MarkOngoingModifyOperation(schema_opid_t opId)
{
    SchemaModifyOperationPtr op = GetSchemaModifyOperation(opId);
    if (!op)
    {
        IE_LOG(ERROR, "get schema modify operation [id:%d] fail!", opId);
        return false;
    }
    op->MarkNotReady();
    return true;    
}

const SchemaModifyOperationPtr& IndexPartitionSchemaImpl::GetSchemaModifyOperation(
        schema_opid_t opId) const
{
    if (opId >= 1 && opId <= (schema_opid_t)mModifyOperations.size())
    {
        return mModifyOperations[opId - 1];
    }
    
    static SchemaModifyOperationPtr tmp;
    return tmp;
}

void IndexPartitionSchemaImpl::AddSchemaModifyOperation(const SchemaModifyOperationPtr& op)
{
    assert(op);
    mModifyOperations.push_back(op);
    op->SetOpId((schema_opid_t)mModifyOperations.size());
    SetSchemaVersionId(mModifyOperations.size());
}

size_t IndexPartitionSchemaImpl::GetModifyOperationCount() const
{
    return mModifyOperations.size();
}

string IndexPartitionSchemaImpl::GetEffectiveIndexInfo(bool modifiedIndexOnly) const
{
    ModifyItemVector indexs;
    ModifyItemVector attrs;
    if (!modifiedIndexOnly)
    {
        const auto& indexSchema = GetIndexSchema();
        if (indexSchema)
        {
            indexSchema->CollectBaseVersionIndexInfo(indexs);
        }
        const auto& attrSchema = GetAttributeSchema();
        if (attrSchema)
        {
            attrSchema->CollectBaseVersionAttrInfo(attrs);
        }
    }
    
    for (auto &op : mModifyOperations)
    {
        assert(op);
        op->CollectEffectiveModifyItem(indexs, attrs);
    }

    JsonMap ret;
    if (!indexs.empty())
    {
        ret[INDEXS] = autil::legacy::ToJson(indexs);
    }

    if (!attrs.empty())
    {
        ret[ATTRIBUTES] = autil::legacy::ToJson(attrs);
    }
    return autil::legacy::ToJsonString(ret);
}

void IndexPartitionSchemaImpl::GetModifyOperationIds(
        vector<schema_opid_t> &ids) const
{
    assert(ids.empty());
    for (auto &op : mModifyOperations)
    {
        ids.push_back(op->GetOpId());
    }
}

void IndexPartitionSchemaImpl::GetNotReadyModifyOperationIds(
        vector<schema_opid_t> &ids) const
{
    assert(ids.empty());
    for (auto &op : mModifyOperations)
    {
        if (op->IsNotReady())
        {
            ids.push_back(op->GetOpId());
        }
    }
}

void IndexPartitionSchemaImpl::SetBaseSchemaImmutable()
{
    RegionSchemaPtr regionSchema = GetRegionSchema(DEFAULT_REGIONID);
    if (regionSchema)
    {
        regionSchema->SetBaseSchemaImmutable();
    }
}

void IndexPartitionSchemaImpl::SetModifySchemaImmutable()
{
    RegionSchemaPtr regionSchema = GetRegionSchema(DEFAULT_REGIONID);
    if (regionSchema)
    {
        regionSchema->SetModifySchemaImmutable();
    }
}

void IndexPartitionSchemaImpl::SetModifySchemaMutable()
{
    RegionSchemaPtr regionSchema = GetRegionSchema(DEFAULT_REGIONID);
    if (regionSchema)
    {
        regionSchema->SetModifySchemaMutable();
    }
}

IndexPartitionSchemaImpl* IndexPartitionSchemaImpl::CreateBaseSchemaWithoutModifyOperation() const
{
    IndexPartitionSchemaImplPtr tmp(Clone());
    tmp->SetMaxModifyOperationCount(std::numeric_limits<uint32_t>::max());
    Any any = ToJson(*tmp);
    unique_ptr<IndexPartitionSchemaImpl> newSchema(new IndexPartitionSchemaImpl);
    newSchema->SetMaxModifyOperationCount(0);
    FromJson(*newSchema, any);
    newSchema->SetSchemaVersionId(0);
    return newSchema.release();
}

IE_NAMESPACE_END(config);
