#include "indexlib/config/region_schema.h"
#include "indexlib/config/impl/region_schema_impl.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, RegionSchema);

RegionSchema::RegionSchema(IndexPartitionSchemaImpl* schema, bool multiRegionFormat)
{
    mImpl.reset(new RegionSchemaImpl(schema, multiRegionFormat));
}
    
const string& RegionSchema::GetRegionName() const
{
    return mImpl->GetRegionName();
}
    
const FieldSchemaPtr& RegionSchema::GetFieldSchema() const
{
    return mImpl->GetFieldSchema();
}
const IndexSchemaPtr& RegionSchema::GetIndexSchema() const
{
    return mImpl->GetIndexSchema();
}
const AttributeSchemaPtr& RegionSchema::GetAttributeSchema() const
{
    return mImpl->GetAttributeSchema();
}
const AttributeSchemaPtr& RegionSchema::GetVirtualAttributeSchema() const
{
    return mImpl->GetVirtualAttributeSchema();
}
const SummarySchemaPtr& RegionSchema::GetSummarySchema() const
{
    return mImpl->GetSummarySchema();
}
const TruncateProfileSchemaPtr& RegionSchema::GetTruncateProfileSchema() const
{
    return mImpl->GetTruncateProfileSchema();
}
    
void RegionSchema::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}
    
bool RegionSchema::IsUsefulField(const string& fieldName) const
{
    return mImpl->IsUsefulField(fieldName);
}
bool RegionSchema::TTLEnabled() const
{
    return mImpl->TTLEnabled();
}
int64_t RegionSchema::GetDefaultTTL() const
{
    return mImpl->GetDefaultTTL();
}
bool RegionSchema::SupportAutoUpdate() const
{
    return mImpl->SupportAutoUpdate();
}
bool RegionSchema::NeedStoreSummary() const
{
    return mImpl->NeedStoreSummary();
}

void RegionSchema::CloneVirtualAttributes(const RegionSchema& other)
{
    mImpl->CloneVirtualAttributes(*(other.mImpl.get()));
}
bool RegionSchema::AddVirtualAttributeConfigs(const AttributeConfigVector& virtualAttrConfigs)
{
    return mImpl->AddVirtualAttributeConfigs(virtualAttrConfigs);
}

void RegionSchema::AddAttributeConfig(const AttributeConfigPtr& attrConfig)
{
    return mImpl->AddAttributeConfig(attrConfig);
}

void RegionSchema::AssertEqual(const RegionSchema& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}
void RegionSchema::AssertCompatible(const RegionSchema& other) const
{
    mImpl->AssertCompatible(*(other.mImpl.get()));
}
void RegionSchema::Check() const
{
    mImpl->Check();
}

// for test
void RegionSchema::SetTruncateProfileSchema(const TruncateProfileSchemaPtr& truncateProfileSchema)
{
    mImpl->SetTruncateProfileSchema(truncateProfileSchema);
}

void RegionSchema::SetRegionName(const string& name)
{
    mImpl->SetRegionName(name);
}

FieldConfigPtr RegionSchema::AddFieldConfig(const string& fieldName, 
                                            FieldType fieldType, 
                                            bool multiValue,
                                            bool isVirtual,
                                            bool isBinary)
{
    return mImpl->AddFieldConfig(fieldName, fieldType, multiValue, isVirtual, isBinary);
}
    
EnumFieldConfigPtr RegionSchema::AddEnumFieldConfig(const string& fieldName, 
                                                    FieldType fieldType, vector<string>& validValues, 
                                                    bool multiValue)
{
    return mImpl->AddEnumFieldConfig(fieldName, fieldType, validValues, multiValue);
}

void RegionSchema::AddIndexConfig(const IndexConfigPtr& indexConfig)
{
    mImpl->AddIndexConfig(indexConfig);
}
void RegionSchema::AddAttributeConfig(const string& fieldName,
                                      const CustomizedConfigVector& customizedConfigs)
{
    mImpl->AddAttributeConfig(fieldName, customizedConfigs);
}

void RegionSchema::AddPackAttributeConfig(const string& attrName,
                                          const vector<string>& subAttrNames,
                                          const string& compressTypeStr)
{
    mImpl->AddPackAttributeConfig(attrName, subAttrNames, compressTypeStr);
}
    
void RegionSchema::AddPackAttributeConfig(const string& attrName,
                                          const vector<string>& subAttrNames,
                                          const string& compressTypeStr,
                                          uint64_t defragSlicePercent)
{
    mImpl->AddPackAttributeConfig(attrName, subAttrNames, compressTypeStr, defragSlicePercent);
}

void RegionSchema::AddVirtualAttributeConfig(const AttributeConfigPtr& virtualAttrConfig)
{
    mImpl->AddVirtualAttributeConfig(virtualAttrConfig);
}

void RegionSchema::AddSummaryConfig(const string& fieldName,
                                    summarygroupid_t summaryGroupId)
{
    mImpl->AddSummaryConfig(fieldName, summaryGroupId);
}
void RegionSchema::AddSummaryConfig(fieldid_t fieldId,
                                    summarygroupid_t summaryGroupId)
{
    mImpl->AddSummaryConfig(fieldId, summaryGroupId);
}
void RegionSchema::SetSummaryCompress(bool compress, const string& compressType,
                                      summarygroupid_t summaryGroupId)
{
    mImpl->SetSummaryCompress(compress, compressType, summaryGroupId);
}
summarygroupid_t RegionSchema::CreateSummaryGroup(const string& summaryGroupName)
{
    return mImpl->CreateSummaryGroup(summaryGroupName);
}
    
void RegionSchema::SetDefaultTTL(int64_t defaultTTL, const string& fieldName)
{
    mImpl->SetDefaultTTL(defaultTTL, fieldName);
}
void RegionSchema::SetEnableTTL(bool enableTTL, const string& fieldName)
{
    mImpl->SetEnableTTL(enableTTL, fieldName);
}

const string& RegionSchema::GetTTLFieldName() const
{
    return mImpl->GetTTLFieldName();
}

void RegionSchema::SetFieldSchema(const FieldSchemaPtr& fieldSchema)
{
    mImpl->SetFieldSchema(fieldSchema);
}

// for testlib
void RegionSchema::LoadValueConfig()
{
    mImpl->LoadValueConfig();
}
void RegionSchema::SetNeedStoreSummary()
{
    mImpl->SetNeedStoreSummary();
}
void RegionSchema::ResetIndexSchema()
{
    mImpl->ResetIndexSchema();
}
vector<AttributeConfigPtr> RegionSchema::EnsureSpatialIndexWithAttribute()
{
    return mImpl->EnsureSpatialIndexWithAttribute();
}
const RegionSchemaImplPtr& RegionSchema::GetImpl()
{
    return mImpl;
}

void RegionSchema::SetBaseSchemaImmutable()
{
    mImpl->SetBaseSchemaImmutable();
}

void RegionSchema::SetModifySchemaImmutable()
{
    mImpl->SetModifySchemaImmutable();
}

void RegionSchema::SetModifySchemaMutable()
{
    mImpl->SetModifySchemaMutable();
}

IE_NAMESPACE_END(config);
