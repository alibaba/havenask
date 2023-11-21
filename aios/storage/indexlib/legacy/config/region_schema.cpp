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
#include "indexlib/config/region_schema.h"

#include "indexlib/config/configurator_define.h"
#include "indexlib/config/impl/region_schema_impl.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, RegionSchema);

RegionSchema::RegionSchema(IndexPartitionSchemaImpl* schema, bool multiRegionFormat)
{
    mImpl.reset(new RegionSchemaImpl(schema, multiRegionFormat));
}

const string& RegionSchema::GetRegionName() const { return mImpl->GetRegionName(); }

const FieldSchemaPtr& RegionSchema::GetFieldSchema() const { return mImpl->GetFieldSchema(); }
const IndexSchemaPtr& RegionSchema::GetIndexSchema() const { return mImpl->GetIndexSchema(); }
const AttributeSchemaPtr& RegionSchema::GetAttributeSchema() const { return mImpl->GetAttributeSchema(); }
const AttributeSchemaPtr& RegionSchema::GetVirtualAttributeSchema() const { return mImpl->GetVirtualAttributeSchema(); }
const SummarySchemaPtr& RegionSchema::GetSummarySchema() const { return mImpl->GetSummarySchema(); }
const SourceSchemaPtr& RegionSchema::GetSourceSchema() const { return mImpl->GetSourceSchema(); }
const std::shared_ptr<FileCompressSchema>& RegionSchema::GetFileCompressSchema() const
{
    return mImpl->GetFileCompressSchema();
}
void RegionSchema::TEST_SetSourceSchema(const SourceSchemaPtr& sourceSchema)
{
    mImpl->TEST_SetSourceSchema(sourceSchema);
}
void RegionSchema::SetFileCompressSchema(const std::shared_ptr<FileCompressSchema>& fileCompressSchema)
{
    mImpl->SetFileCompressSchema(fileCompressSchema);
}

void RegionSchema::SetTemperatureLayerConfig(const TemperatureLayerConfigPtr& temperatureLayerConfig)
{
    mImpl->SetTemperatureLayerConfig(temperatureLayerConfig);
}

void RegionSchema::TEST_SetTemperatureLayerConfig(const TemperatureLayerConfigPtr& temperatureLayerConfig)
{
    mImpl->TEST_SetTemperatureLayerConfig(temperatureLayerConfig);
}

const TruncateProfileSchemaPtr& RegionSchema::GetTruncateProfileSchema() const
{
    return mImpl->GetTruncateProfileSchema();
}

void RegionSchema::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) { mImpl->Jsonize(json); }

bool RegionSchema::IsUsefulField(const string& fieldName) const { return mImpl->IsUsefulField(fieldName); }
bool RegionSchema::TTLEnabled() const { return mImpl->TTLEnabled(); }
bool RegionSchema::HashIdEnabled() const { return mImpl->HashIdEnabled(); }
int64_t RegionSchema::GetDefaultTTL() const { return mImpl->GetDefaultTTL(); }
bool RegionSchema::TTLFromDoc() const { return mImpl->TTLFromDoc(); }
bool RegionSchema::SupportAutoUpdate() const { return mImpl->SupportAutoUpdate(); }
bool RegionSchema::NeedStoreSummary() const { return mImpl->NeedStoreSummary(); }
bool RegionSchema::EnableTemperatureLayer() const { return mImpl->EnableTemperatureLayer(); }
const TemperatureLayerConfigPtr& RegionSchema::GetTemperatureLayerConfig() const
{
    return mImpl->GetTemperatureLayerConfig();
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

void RegionSchema::AssertEqual(const RegionSchema& other) const { mImpl->AssertEqual(*(other.mImpl.get())); }
void RegionSchema::AssertCompatible(const RegionSchema& other) const { mImpl->AssertCompatible(*(other.mImpl.get())); }
void RegionSchema::Check() const { mImpl->Check(); }

// for test
void RegionSchema::SetTruncateProfileSchema(const TruncateProfileSchemaPtr& truncateProfileSchema)
{
    mImpl->SetTruncateProfileSchema(truncateProfileSchema);
}

void RegionSchema::SetRegionName(const string& name) { mImpl->SetRegionName(name); }

FieldConfigPtr RegionSchema::AddFieldConfig(const string& fieldName, FieldType fieldType, bool multiValue,
                                            bool isVirtual, bool isBinary)
{
    return mImpl->AddFieldConfig(fieldName, fieldType, multiValue, isVirtual, isBinary);
}

EnumFieldConfigPtr RegionSchema::AddEnumFieldConfig(const string& fieldName, FieldType fieldType,
                                                    vector<string>& validValues, bool multiValue)
{
    return mImpl->AddEnumFieldConfig(fieldName, fieldType, validValues, multiValue);
}

void RegionSchema::AddIndexConfig(const IndexConfigPtr& indexConfig) { mImpl->AddIndexConfig(indexConfig); }
void RegionSchema::AddAttributeConfig(const string& fieldName, const CustomizedConfigVector& customizedConfigs)
{
    mImpl->AddAttributeConfig(fieldName, customizedConfigs);
}

AttributeConfigPtr RegionSchema::CreateAttributeConfig(const string& fieldName,
                                                       const CustomizedConfigVector& customizedConfigs)
{
    return mImpl->CreateAttributeConfig(fieldName, customizedConfigs);
}

void RegionSchema::AddPackAttributeConfig(const string& attrName, const vector<string>& subAttrNames,
                                          const string& compressTypeStr)
{
    mImpl->AddPackAttributeConfig(attrName, subAttrNames, compressTypeStr,
                                  index::ATTRIBUTE_DEFAULT_DEFRAG_SLICE_PERCENT, std::shared_ptr<FileCompressConfig>(),
                                  "");
}

void RegionSchema::AddPackAttributeConfig(const string& attrName, const vector<string>& subAttrNames,
                                          const string& compressTypeStr, uint64_t defragSlicePercent)
{
    mImpl->AddPackAttributeConfig(attrName, subAttrNames, compressTypeStr, defragSlicePercent,
                                  std::shared_ptr<FileCompressConfig>(), "");
}

void RegionSchema::AddVirtualAttributeConfig(const AttributeConfigPtr& virtualAttrConfig)
{
    mImpl->AddVirtualAttributeConfig(virtualAttrConfig);
}

void RegionSchema::AddSummaryConfig(const string& fieldName, index::summarygroupid_t summaryGroupId)
{
    mImpl->AddSummaryConfig(fieldName, summaryGroupId);
}
void RegionSchema::AddSummaryConfig(fieldid_t fieldId, index::summarygroupid_t summaryGroupId)
{
    mImpl->AddSummaryConfig(fieldId, summaryGroupId);
}
void RegionSchema::SetSummaryCompress(bool compress, const string& compressType, index::summarygroupid_t summaryGroupId)
{
    mImpl->SetSummaryCompress(compress, compressType, summaryGroupId);
}
index::summarygroupid_t RegionSchema::CreateSummaryGroup(const string& summaryGroupName)
{
    return mImpl->CreateSummaryGroup(summaryGroupName);
}

void RegionSchema::SetDefaultTTL(int64_t defaultTTL, const string& fieldName)
{
    mImpl->SetDefaultTTL(defaultTTL, fieldName);
}
void RegionSchema::SetEnableTTL(bool enableTTL, const string& fieldName) { mImpl->SetEnableTTL(enableTTL, fieldName); }
void RegionSchema::SetEnableHashId(bool enableHashId, const string& fieldName)
{
    mImpl->SetEnableHashId(enableHashId, fieldName);
}

void RegionSchema::TEST_SetTTLFromDoc(bool value) { mImpl->TEST_SetTTLFromDoc(value); }
const string& RegionSchema::GetTTLFieldName() const { return mImpl->GetTTLFieldName(); }

const std::string& RegionSchema::GetHashIdFieldName() const { return mImpl->GetHashIdFieldName(); }

const std::string& RegionSchema::GetOrderPreservingFieldName() const { return mImpl->GetOrderPreservingFieldName(); }
void RegionSchema::SetFieldSchema(const FieldSchemaPtr& fieldSchema) { mImpl->SetFieldSchema(fieldSchema); }

// for testlib
void RegionSchema::LoadValueConfig() { mImpl->LoadValueConfig(); }
void RegionSchema::SetNeedStoreSummary() { mImpl->SetNeedStoreSummary(); }
void RegionSchema::ResetIndexSchema() { mImpl->ResetIndexSchema(); }
vector<AttributeConfigPtr> RegionSchema::EnsureSpatialIndexWithAttribute()
{
    return mImpl->EnsureSpatialIndexWithAttribute();
}
const RegionSchemaImplPtr& RegionSchema::GetImpl() { return mImpl; }

void RegionSchema::SetBaseSchemaImmutable() { mImpl->SetBaseSchemaImmutable(); }

void RegionSchema::SetModifySchemaImmutable() { mImpl->SetModifySchemaImmutable(); }

void RegionSchema::SetModifySchemaMutable() { mImpl->SetModifySchemaMutable(); }
}} // namespace indexlib::config
