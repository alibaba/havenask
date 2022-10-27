#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/impl/index_partition_schema_impl.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace tr1;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, IndexPartitionSchema);

IndexPartitionSchema::IndexPartitionSchema(const string& schemaName)
{
    mImpl.reset(new IndexPartitionSchemaImpl(schemaName));
}

DictionaryConfigPtr IndexPartitionSchema::AddDictionaryConfig(
    const string& dictName, const string& content)
{
    return mImpl->AddDictionaryConfig(dictName, content);
}

FieldConfigPtr IndexPartitionSchema::AddFieldConfig(
        const string& fieldName, FieldType fieldType, 
        bool multiValue, bool isVirtual, bool isBinary)
{
    return mImpl->AddFieldConfig(fieldName, fieldType ,multiValue, isVirtual, isBinary);
}
    
EnumFieldConfigPtr IndexPartitionSchema::AddEnumFieldConfig(const string& fieldName, 
                                                            FieldType fieldType,
                                                            vector<string>& validValues, 
                                                            bool multiValue)
{
    return mImpl->AddEnumFieldConfig(fieldName, fieldType, validValues, multiValue);
}

void IndexPartitionSchema::AddRegionSchema(const RegionSchemaPtr& regionSchema)
{
    mImpl->AddRegionSchema(regionSchema);
}

size_t IndexPartitionSchema::GetRegionCount() const
{
    return mImpl->GetRegionCount();
}

const RegionSchemaPtr& IndexPartitionSchema::GetRegionSchema(const string& regionName) const
{
    return mImpl->GetRegionSchema(regionName);
}

const RegionSchemaPtr& IndexPartitionSchema::GetRegionSchema(regionid_t regionId) const
{
    return mImpl->GetRegionSchema(regionId);
}
    
regionid_t IndexPartitionSchema::GetRegionId(const string& regionName) const
{
    return mImpl->GetRegionId(regionName);
}
    
void IndexPartitionSchema::SetSchemaName(const string& schemaName)
{
    mImpl->SetSchemaName(schemaName);
}
    
const string& IndexPartitionSchema::GetSchemaName() const
{
    return mImpl->GetSchemaName();
}

TableType IndexPartitionSchema::GetTableType() const
{
    return mImpl->GetTableType();
}

void IndexPartitionSchema::SetTableType(const string &str)
{
    mImpl->SetTableType(str);
}

const JsonMap& IndexPartitionSchema::GetUserDefinedParam() const
{
    return mImpl->GetUserDefinedParam();
}
    
JsonMap& IndexPartitionSchema::GetUserDefinedParam()
{
    return mImpl->GetUserDefinedParam();
}
    
void IndexPartitionSchema::SetUserDefinedParam(const JsonMap &jsonMap)
{
    mImpl->SetUserDefinedParam(jsonMap);
}
    
bool IndexPartitionSchema::GetValueFromUserDefinedParam(const string &key,
                                                        string &value) const
{
    return mImpl->GetValueFromUserDefinedParam(key, value);
}

const JsonMap& IndexPartitionSchema::GetGlobalRegionIndexPreference() const
{
    return mImpl->GetGlobalRegionIndexPreference();
}
    
JsonMap& IndexPartitionSchema::GetGlobalRegionIndexPreference()
{
    return mImpl->GetGlobalRegionIndexPreference();
}
    
void IndexPartitionSchema::SetGlobalRegionIndexPreference(const JsonMap &jsonMap)
{
    mImpl->SetGlobalRegionIndexPreference(jsonMap);
}

void IndexPartitionSchema::SetAdaptiveDictSchema(const AdaptiveDictionarySchemaPtr& adaptiveDictSchema)
{
    mImpl->SetAdaptiveDictSchema(adaptiveDictSchema);
}
    
const AdaptiveDictionarySchemaPtr& IndexPartitionSchema::GetAdaptiveDictSchema() const
{
    return mImpl->GetAdaptiveDictSchema();
}
    
const DictionarySchemaPtr& IndexPartitionSchema::GetDictSchema() const
{
    return mImpl->GetDictSchema();
}
    
const FieldSchemaPtr& IndexPartitionSchema::GetFieldSchema(regionid_t regionId) const
{
    return mImpl->GetFieldSchema(regionId);
}
    
const IndexSchemaPtr& IndexPartitionSchema::GetIndexSchema(regionid_t regionId) const
{
    return mImpl->GetIndexSchema(regionId);
}
    
const AttributeSchemaPtr& IndexPartitionSchema::GetAttributeSchema(regionid_t regionId) const
{
    return mImpl->GetAttributeSchema(regionId);
}
    
const AttributeSchemaPtr& IndexPartitionSchema::GetVirtualAttributeSchema(regionid_t regionId) const
{
    return mImpl->GetVirtualAttributeSchema(regionId);
}
    
const SummarySchemaPtr& IndexPartitionSchema::GetSummarySchema(regionid_t regionId) const
{
    return mImpl->GetSummarySchema(regionId);
}
    
const TruncateProfileSchemaPtr& IndexPartitionSchema::GetTruncateProfileSchema(regionid_t regionId) const
{
    return mImpl->GetTruncateProfileSchema(regionId);
}
    
const FieldSchemaPtr& IndexPartitionSchema::GetFieldSchema()const
{
    return mImpl->GetFieldSchema();
}

const IndexSchemaPtr& IndexPartitionSchema::GetIndexSchema() const
{
    return mImpl->GetIndexSchema();
}

const AttributeSchemaPtr& IndexPartitionSchema::GetAttributeSchema() const
{
    return mImpl->GetAttributeSchema();
}

const AttributeSchemaPtr& IndexPartitionSchema::GetVirtualAttributeSchema() const
{
    return mImpl->GetVirtualAttributeSchema();
}

const SummarySchemaPtr& IndexPartitionSchema::GetSummarySchema() const
{
    return mImpl->GetSummarySchema();
}

const TruncateProfileSchemaPtr& IndexPartitionSchema::GetTruncateProfileSchema() const
{
    return mImpl->GetTruncateProfileSchema();
}
    
const IndexPartitionSchemaPtr& IndexPartitionSchema::GetSubIndexPartitionSchema() const
{
    return mImpl->GetSubIndexPartitionSchema();
}

void IndexPartitionSchema::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}

void IndexPartitionSchema::AssertEqual(const IndexPartitionSchema& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}

// only literal clone (rewrite data not cloned)
IndexPartitionSchema* IndexPartitionSchema::Clone() const
{
    unique_ptr<IndexPartitionSchema> ptr;
    IndexPartitionSchema *cloneSchema =
        new IndexPartitionSchema(mImpl->GetSchemaName());
    ptr.reset(cloneSchema);
    
    cloneSchema->mImpl.reset(mImpl->Clone());
    return ptr.release();
}

const string& IndexPartitionSchema::GetTTLFieldName(regionid_t id)
{
    return mImpl->GetTTLFieldName(id);
}

// shallow clone 
IndexPartitionSchema* IndexPartitionSchema::CloneWithDefaultRegion(regionid_t regionId) const
{
    IndexPartitionSchema *cloneSchema =
        new IndexPartitionSchema(mImpl->GetSchemaName());
    cloneSchema->mImpl.reset(mImpl->CloneWithDefaultRegion(regionId));
    return cloneSchema;
}
    
void IndexPartitionSchema::CloneVirtualAttributes(const IndexPartitionSchema& other)
{
    mImpl->CloneVirtualAttributes(other.mImpl.get());
}
    
bool IndexPartitionSchema::AddVirtualAttributeConfigs(
    const AttributeConfigVector& virtualAttrConfigs,
    regionid_t id)
{
    return mImpl->AddVirtualAttributeConfigs(virtualAttrConfigs, id);
}

void IndexPartitionSchema::AssertCompatible(const IndexPartitionSchema& other) const
{
    mImpl->AssertCompatible(*(other.mImpl.get()));
}

void IndexPartitionSchema::Check() const
{
    mImpl->Check();
}

bool IndexPartitionSchema::NeedStoreSummary() const
{
    return mImpl->NeedStoreSummary();
}

bool IndexPartitionSchema::IsUsefulField(const string& fieldName) const
{
    return mImpl->IsUsefulField(fieldName);
}

void IndexPartitionSchema::SetSubIndexPartitionSchema(const IndexPartitionSchemaPtr& schema)
{
    mImpl->SetSubIndexPartitionSchema(schema);
}

void IndexPartitionSchema::SetEnableTTL(bool enableTTL, regionid_t id, const string& ttlFieldName)
{
    mImpl->SetEnableTTL(enableTTL, id, ttlFieldName);
}
    
bool IndexPartitionSchema::TTLEnabled(regionid_t id) const
{
    return mImpl->TTLEnabled(id);
}

void IndexPartitionSchema::SetDefaultTTL(
        int64_t defaultTTL, regionid_t id, const string& ttlFieldName)
{
    mImpl->SetDefaultTTL(defaultTTL, id, ttlFieldName);
}
    
int64_t IndexPartitionSchema::GetDefaultTTL(regionid_t id) const
{
    return mImpl->GetDefaultTTL(id);
}

void IndexPartitionSchema::SetAutoUpdatePreference(bool autoUpdate)
{
    mImpl->SetAutoUpdatePreference(autoUpdate);
}

bool IndexPartitionSchema::GetAutoUpdatePreference() const
{
    return mImpl->GetAutoUpdatePreference();
}

bool IndexPartitionSchema::SupportAutoUpdate() const
{
    return mImpl->SupportAutoUpdate();
}
    
void IndexPartitionSchema::LoadFieldSchema(const autil::legacy::Any& any)
{
    mImpl->LoadFieldSchema(any);
}

void IndexPartitionSchema::SetDefaultRegionId(regionid_t regionId)
{
    mImpl->SetDefaultRegionId(regionId);
}

regionid_t IndexPartitionSchema::GetDefaultRegionId() const
{
    return mImpl->GetDefaultRegionId();
}
    
void IndexPartitionSchema::SetCustomizedTableConfig(const CustomizedTableConfigPtr& tableConfig)
{
    mImpl->SetCustomizedTableConfig(tableConfig);
}

const CustomizedTableConfigPtr& IndexPartitionSchema::GetCustomizedTableConfig() const
{
    return mImpl->GetCustomizedTableConfig();
}

void IndexPartitionSchema::SetCustomizedDocumentConfig(const CustomizedConfigVector& documentConfigs)
{
    mImpl->SetCustomizedDocumentConfig(documentConfigs);
}

const CustomizedConfigVector& IndexPartitionSchema::GetCustomizedDocumentConfigs() const
{
    return mImpl->GetCustomizedDocumentConfigs();
}

void IndexPartitionSchema::AddIndexConfig(const IndexConfigPtr& indexConfig,
                                          regionid_t id)
{
    mImpl->AddIndexConfig(indexConfig, id);
}

void IndexPartitionSchema::AddAttributeConfig(const string& fieldName,
                                              regionid_t id)
{
    mImpl->AddAttributeConfig(fieldName, id);
}
    
void IndexPartitionSchema::AddPackAttributeConfig(const string& attrName,
                            const vector<string>& subAttrNames,
                            const string& compressTypeStr,
                            regionid_t id)
{
    mImpl->AddPackAttributeConfig(attrName, subAttrNames, compressTypeStr, id);
}

void IndexPartitionSchema::AddVirtualAttributeConfig(const AttributeConfigPtr& virtualAttrConfig,
                               regionid_t id)
{
    mImpl->AddVirtualAttributeConfig(virtualAttrConfig, id);
}

void IndexPartitionSchema::AddSummaryConfig(const string& fieldName,
                      summarygroupid_t summaryGroupId,
                      regionid_t id)
{
    mImpl->AddSummaryConfig(fieldName, summaryGroupId, id);
}

void IndexPartitionSchema::SetTruncateProfileSchema(
    const TruncateProfileSchemaPtr& truncateProfileSchema,
    regionid_t id)
{
    mImpl->SetTruncateProfileSchema(truncateProfileSchema, id);
}

void IndexPartitionSchema::LoadValueConfig(regionid_t id)
{
    mImpl->LoadValueConfig(id);
}

void IndexPartitionSchema::ResetRegions()
{
    mImpl->ResetRegions();
}

const FieldSchemaPtr& IndexPartitionSchema::GetDefaultFieldSchema() const
{
    return mImpl->GetDefaultFieldSchema();
}

schemavid_t IndexPartitionSchema::GetSchemaVersionId() const
{
    return mImpl->GetSchemaVersionId();
}

void IndexPartitionSchema::SetSchemaVersionId(schemavid_t schemaId)
{
    mImpl->SetSchemaVersionId(schemaId);
}
    
const IndexPartitionSchemaImplPtr& IndexPartitionSchema::GetImpl() const
{
    return mImpl;
}

string IndexPartitionSchema::TableType2Str(TableType tableType)
{
    return IndexPartitionSchemaImpl::TableType2Str(tableType);
}

bool IndexPartitionSchema::HasModifyOperations() const
{
    return mImpl->HasModifyOperations();
}

bool IndexPartitionSchema::MarkOngoingModifyOperation(schema_opid_t opId)
{
    return mImpl->MarkOngoingModifyOperation(opId);
}

const SchemaModifyOperationPtr& IndexPartitionSchema::GetSchemaModifyOperation(
        schema_opid_t opId) const
{
    return mImpl->GetSchemaModifyOperation(opId);
}
    
size_t IndexPartitionSchema::GetModifyOperationCount() const
{
    return mImpl->GetModifyOperationCount();
}

string IndexPartitionSchema::GetEffectiveIndexInfo(
        bool modifiedIndexOnly) const
{
    return mImpl->GetEffectiveIndexInfo(modifiedIndexOnly);    
}

void IndexPartitionSchema::GetNotReadyModifyOperationIds(
        vector<schema_opid_t> &ids) const
{
    return mImpl->GetNotReadyModifyOperationIds(ids);
}

void IndexPartitionSchema::GetModifyOperationIds(
        vector<schema_opid_t> &ids) const
{
    return mImpl->GetModifyOperationIds(ids);
}

void IndexPartitionSchema::SetMaxModifyOperationCount(uint32_t count)
{
    mImpl->SetMaxModifyOperationCount(count);
}

uint32_t IndexPartitionSchema::GetMaxModifyOperationCount() const
{
    return mImpl->GetMaxModifyOperationCount();
}

void IndexPartitionSchema::EnableThrowAssertCompatibleException()
{
    mImpl->EnableThrowAssertCompatibleException();
}

IndexPartitionSchema* IndexPartitionSchema::CreateSchemaForTargetModifyOperation(
        schema_opid_t opId) const
{
    if (opId == INVALID_SCHEMA_OP_ID || opId > GetMaxModifyOperationCount())
    {
        return NULL;
    }
    return CreateSchemaWithMaxModifyOperationCount((uint32_t)opId);
}

IndexPartitionSchema* IndexPartitionSchema::CreateSchemaWithMaxModifyOperationCount(
        uint32_t maxOperationCount) const
{
    IndexPartitionSchemaPtr tmp(Clone());
    tmp->SetMaxModifyOperationCount(std::numeric_limits<uint32_t>::max());
    Any any = ToJson(*tmp);
    unique_ptr<IndexPartitionSchema> newSchema(new IndexPartitionSchema);
    newSchema->SetMaxModifyOperationCount(maxOperationCount);
    FromJson(*newSchema, any);
    newSchema->SetSchemaVersionId(maxOperationCount);
    return newSchema.release();
}

IndexPartitionSchema* IndexPartitionSchema::CreateSchemaWithoutModifyOperations() const
{
    return CreateSchemaWithMaxModifyOperationCount(0u);
}

IE_NAMESPACE_END(config);
