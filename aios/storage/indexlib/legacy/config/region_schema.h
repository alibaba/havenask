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
#pragma once

#include <memory>

#include "autil/Log.h"
#include "indexlib/config/FileCompressSchema.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/source_schema.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/config/temperature_layer_config.h"
#include "indexlib/config/truncate_profile_schema.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {

class RegionSchemaImpl;
typedef std::shared_ptr<RegionSchemaImpl> RegionSchemaImplPtr;
class IndexPartitionSchemaImpl;

class RegionSchema : public autil::legacy::Jsonizable
{
public:
    RegionSchema(IndexPartitionSchemaImpl* schema, bool multiRegionFormat = false);
    ~RegionSchema() {}

public:
    const std::string& GetRegionName() const;
    const FieldSchemaPtr& GetFieldSchema() const;
    const IndexSchemaPtr& GetIndexSchema() const;
    const AttributeSchemaPtr& GetAttributeSchema() const;
    const AttributeSchemaPtr& GetVirtualAttributeSchema() const;
    const SummarySchemaPtr& GetSummarySchema() const;
    const SourceSchemaPtr& GetSourceSchema() const;
    const TruncateProfileSchemaPtr& GetTruncateProfileSchema() const;
    const std::shared_ptr<FileCompressSchema>& GetFileCompressSchema() const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool IsUsefulField(const std::string& fieldName) const;
    bool HashIdEnabled() const;
    bool TTLEnabled() const;
    int64_t GetDefaultTTL() const;
    bool TTLFromDoc() const;
    bool SupportAutoUpdate() const;
    bool NeedStoreSummary() const;
    void CloneVirtualAttributes(const RegionSchema& other);
    bool AddVirtualAttributeConfigs(const AttributeConfigVector& virtualAttrConfigs);
    const std::string& GetTTLFieldName() const;
    const std::string& GetHashIdFieldName() const;
    const std::string& GetOrderPreservingFieldName() const;
    bool EnableTemperatureLayer() const;
    const TemperatureLayerConfigPtr& GetTemperatureLayerConfig() const;
    void SetTemperatureLayerConfig(const TemperatureLayerConfigPtr& temperatureLayerConfig);
    void SetFileCompressSchema(const std::shared_ptr<FileCompressSchema>& fileCompressSchema);

public:
    void AssertEqual(const RegionSchema& other) const;
    void AssertCompatible(const RegionSchema& other) const;
    void Check() const;

public:
    void SetTruncateProfileSchema(const TruncateProfileSchemaPtr& truncateProfileSchema);
    void SetRegionName(const std::string& name);
    FieldConfigPtr AddFieldConfig(const std::string& fieldName, FieldType fieldType, bool multiValue = false,
                                  bool isVirtual = false, bool isBinary = false);
    EnumFieldConfigPtr AddEnumFieldConfig(const std::string& fieldName, FieldType fieldType,
                                          std::vector<std::string>& validValues, bool multiValue = false);
    void AddIndexConfig(const IndexConfigPtr& indexConfig);
    void AddAttributeConfig(const std::string& fieldName,
                            const CustomizedConfigVector& customizedConfigs = CustomizedConfigVector());
    void AddAttributeConfig(const config::AttributeConfigPtr& attrConfig);
    AttributeConfigPtr
    CreateAttributeConfig(const std::string& fieldName,
                          const CustomizedConfigVector& customizedConfigs = CustomizedConfigVector());
    void AddPackAttributeConfig(const std::string& attrName, const std::vector<std::string>& subAttrNames,
                                const std::string& compressTypeStr);
    void AddPackAttributeConfig(const std::string& attrName, const std::vector<std::string>& subAttrNames,
                                const std::string& compressTypeStr, uint64_t defragSlicePercent);
    void AddVirtualAttributeConfig(const config::AttributeConfigPtr& virtualAttrConfig);
    void AddSummaryConfig(const std::string& fieldName,
                          index::summarygroupid_t summaryGroupId = index::DEFAULT_SUMMARYGROUPID);
    void AddSummaryConfig(fieldid_t fieldId, index::summarygroupid_t summaryGroupId = index::DEFAULT_SUMMARYGROUPID);
    void SetSummaryCompress(bool compress, const std::string& compressType = "",
                            index::summarygroupid_t summaryGroupId = index::DEFAULT_SUMMARYGROUPID);
    index::summarygroupid_t CreateSummaryGroup(const std::string& summaryGroupName);

    void SetFieldSchema(const config::FieldSchemaPtr& fieldSchema);

    void SetDefaultTTL(int64_t defaultTTL, const std::string& fieldName = DOC_TIME_TO_LIVE_IN_SECONDS);

    void SetEnableTTL(bool enableTTL, const std::string& fieldName = DOC_TIME_TO_LIVE_IN_SECONDS);

    void SetEnableHashId(bool enableHashId, const std::string& fieldName);

    void TEST_SetTTLFromDoc(bool value);
    void TEST_SetSourceSchema(const SourceSchemaPtr& sourceSchema);
    void TEST_SetTemperatureLayerConfig(const TemperatureLayerConfigPtr& temperatureLayerConfig);

public:
    // for testlib
    void LoadValueConfig();
    void SetNeedStoreSummary();
    void ResetIndexSchema();
    std::vector<config::AttributeConfigPtr> EnsureSpatialIndexWithAttribute();
    const RegionSchemaImplPtr& GetImpl();
    void SetBaseSchemaImmutable();
    void SetModifySchemaImmutable();
    void SetModifySchemaMutable();

private:
    RegionSchemaImplPtr mImpl;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<RegionSchema> RegionSchemaPtr;
}} // namespace indexlib::config
