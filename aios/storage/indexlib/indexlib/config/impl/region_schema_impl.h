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
#ifndef __INDEXLIB_REGION_SCHEMA_IMPL_H
#define __INDEXLIB_REGION_SCHEMA_IMPL_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/FileCompressSchema.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/source_schema.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/config/temperature_layer_config.h"
#include "indexlib/config/truncate_profile_schema.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchemaImpl);
DECLARE_REFERENCE_CLASS(config, RegionSchema);

namespace indexlib { namespace config {

class RegionSchemaImpl : public autil::legacy::Jsonizable
{
public:
    RegionSchemaImpl(IndexPartitionSchemaImpl* schema, bool multiRegionFormat = false);
    ~RegionSchemaImpl();

public:
    const std::string& GetRegionName() const { return mRegionName; }

    const FieldSchemaPtr& GetFieldSchema() const { return mFieldSchema; }
    const IndexSchemaPtr& GetIndexSchema() const { return mIndexSchema; }
    const AttributeSchemaPtr& GetAttributeSchema() const { return mAttributeSchema; }
    const AttributeSchemaPtr& GetVirtualAttributeSchema() const { return mVirtualAttributeSchema; }
    const SummarySchemaPtr& GetSummarySchema() const { return mSummarySchema; }
    const SourceSchemaPtr& GetSourceSchema() const { return mSourceSchema; }
    const TruncateProfileSchemaPtr& GetTruncateProfileSchema() const { return mTruncateProfileSchema; }
    const std::shared_ptr<FileCompressSchema>& GetFileCompressSchema() const { return mFileCompressSchema; }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    bool IsUsefulField(const std::string& fieldName) const;
    bool TTLEnabled() const;
    bool HashIdEnabled() const;
    int64_t GetDefaultTTL() const;
    bool SupportAutoUpdate() const;
    bool NeedStoreSummary() const { return mSummarySchema ? mSummarySchema->NeedStoreSummary() : false; }
    bool EnableTemperatureLayer() const { return mEnableTemperatureLayer; }
    const TemperatureLayerConfigPtr& GetTemperatureLayerConfig() { return mTemperatureLayer; }
    void SetTemperatureLayerConfig(const TemperatureLayerConfigPtr& config)
    {
        if (mEnableTemperatureLayer) {
            mTemperatureLayer = config;
        }
    }
    void CloneVirtualAttributes(const RegionSchemaImpl& other);
    bool AddVirtualAttributeConfigs(const AttributeConfigVector& virtualAttrConfigs);

public:
    void AssertEqual(const RegionSchemaImpl& other) const;
    void AssertCompatible(const RegionSchemaImpl& other) const;
    void Check() const;

public:
    // for test
    void TEST_SetSourceSchema(const SourceSchemaPtr& sourceSchema) { mSourceSchema = sourceSchema; }
    void SetFileCompressSchema(const std::shared_ptr<FileCompressSchema>& fileCompressSchema)
    {
        mFileCompressSchema = fileCompressSchema;
    }
    void TEST_SetTemperatureLayerConfig(const TemperatureLayerConfigPtr& config)
    {
        mTemperatureLayer = config;
        mEnableTemperatureLayer = (mTemperatureLayer != nullptr);
    }

    void SetTruncateProfileSchema(const TruncateProfileSchemaPtr& truncateProfileSchema)
    {
        mTruncateProfileSchema = truncateProfileSchema;
    }

    void SetRegionName(const std::string& name) { mRegionName = name; }

    FieldConfigPtr AddFieldConfig(const std::string& fieldName, FieldType fieldType, bool multiValue = false,
                                  bool isVirtual = false, bool isBinary = false);

    EnumFieldConfigPtr AddEnumFieldConfig(const std::string& fieldName, FieldType fieldType,
                                          std::vector<std::string>& validValues, bool multiValue = false);

    void AddIndexConfig(const IndexConfigPtr& indexConfig);
    // used for customized
    void AddAttributeConfig(const std::string& fieldName,
                            const CustomizedConfigVector& customizedConfigs = CustomizedConfigVector());

    void AddAttributeConfig(const config::AttributeConfigPtr& attrConfig);

    // used for new formatte
    void AddAttributeConfig(const autil::legacy::Any& any);
    void AddPackAttributeConfig(const std::string& attrName, const std::vector<std::string>& subAttrNames,
                                const std::string& compressTypeStr, uint64_t defragSlicePercent,
                                const std::shared_ptr<FileCompressConfig>& fileCompressConfig,
                                const std::string& valueFormat);

    void AddVirtualAttributeConfig(const config::AttributeConfigPtr& virtualAttrConfig);

    void AddSummaryConfig(const std::string& fieldName, summarygroupid_t summaryGroupId = DEFAULT_SUMMARYGROUPID);
    void AddSummaryConfig(fieldid_t fieldId, summarygroupid_t summaryGroupId = DEFAULT_SUMMARYGROUPID);
    void SetSummaryCompress(bool compress, const std::string& compressType = "",
                            summarygroupid_t summaryGroupId = DEFAULT_SUMMARYGROUPID);
    void SetAdaptiveOffset(bool adaptiveOffset, summarygroupid_t summaryGroupId = DEFAULT_SUMMARYGROUPID);
    void SetSummaryGroupDataParam(const GroupDataParameter& param,
                                  summarygroupid_t summaryGroupId = DEFAULT_SUMMARYGROUPID);

    summarygroupid_t CreateSummaryGroup(const std::string& summaryGroupName);

    void SetDefaultTTL(int64_t defaultTTL, const std::string& ttlFieldName);
    void SetEnableTTL(bool enableTTL, const std::string& ttlFieldName);

    const std::string& GetTTLFieldName() const { return mTTLFieldName; }
    bool TTLFromDoc() const { return mTTLFromDoc; }

    void SetEnableHashId(bool enableHashId, const std::string& fieldName);
    void SetEnableOrderPreserving();

    const std::string& GetHashIdFieldName() const { return mHashIdFieldName; }
    const std::string& GetOrderPreservingFieldName() const { return mOrderPreservingField; }

    void SetFieldSchema(const config::FieldSchemaPtr& fieldSchema)
    {
        mOwnFieldSchema = true;
        mFieldSchema = fieldSchema;
    }

public:
    // for testlib
    void LoadValueConfig();
    void SetNeedStoreSummary();

    void ResetIndexSchema() { mIndexSchema.reset(new IndexSchema); }

    std::vector<config::AttributeConfigPtr> EnsureSpatialIndexWithAttribute();

    void SetBaseSchemaImmutable();
    void SetModifySchemaImmutable();
    void SetModifySchemaMutable();
    void TEST_SetTTLFromDoc(bool value) { mTTLFromDoc = value; }

private:
    void ResolveEmptyProfileNamesForTruncateIndex();
    void UpdateIndexConfigForTruncate(IndexConfig* indexConfig, IndexConfig* truncateIndexConfig);
    AttributeConfigPtr
    CreateAttributeConfig(const std::string& fieldName,
                          const CustomizedConfigVector& customizedConfigs = CustomizedConfigVector());
    AttributeConfigPtr CreateVirtualAttributeConfig(const config::FieldConfigPtr& fieldConfig,
                                                    const common::AttributeValueInitializerCreatorPtr& creator);

    fieldid_t GetFieldIdForVirtualAttribute() const;

    void OptimizeKKVSKeyStore(const KKVIndexConfigPtr& kkvIndexConfig, std::vector<AttributeConfigPtr>& attrConfigs);

    void InitTruncateIndexConfigs();
    IndexConfigPtr CreateTruncateIndexConfig(const IndexConfigPtr& indexConfig,
                                             const TruncateProfileConfigPtr& truncateProfileConfig);

    void ToJson(autil::legacy::Jsonizable::JsonWrapper& json);
    void FromJson(autil::legacy::Jsonizable::JsonWrapper& json);

    void LoadFieldSchema(const autil::legacy::Any& any);
    void LoadFileCompressSchema(const autil::legacy::Any& any);
    void LoadTruncateProfileSchema(const autil::legacy::Any& any);
    void LoadIndexSchema(const autil::legacy::Any& any);
    void LoadAttributeSchema(const autil::legacy::Any& any);
    void LoadAttributeConfig(const autil::legacy::Any& any);
    void LoadPackAttributeConfig(const autil::legacy::Any& any);
    bool LoadSummaryGroup(const autil::legacy::Any& any, summarygroupid_t summaryGroupId);
    void LoadSummarySchema(const autil::legacy::Any& any);
    void LoadSourceSchema(const autil::legacy::Any& any);
    void LoadTemperatureConfig(const autil::legacy::Any& any);

    void CheckIndexSchema() const;
    void CheckFieldSchema() const;
    void CheckAttributeSchema() const;
    void CheckSourceSchema() const;
    void CheckIndexNameNotSummary(const IndexConfigPtr& indexConfig) const;
    void
    CheckSingleFieldIndex(const IndexConfigPtr& indexConfig, std::vector<uint32_t>* singleFieldIndexCounts,
                          std::map<std::string, std::set<std::string>>* singleFieldIndexConfigsWithProfileNames) const;
    void CheckFieldsOrderInPackIndex(const IndexConfigPtr& indexConfig) const;
    void CheckTruncateSortParams() const;
    void CheckTruncateProfileSchema() const;
    void CheckIndexTruncateProfiles(const IndexConfigPtr& indexConfig) const;

    void CheckSpatialIndexConfig(const IndexConfigPtr& indexConfig) const;
    void CheckAttributeConfig(TableType type, const AttributeConfigPtr& config) const;
    void CheckKvKkvPrimaryKeyConfig() const;

private:
    FieldSchemaPtr mFieldSchema;
    IndexSchemaPtr mIndexSchema;
    AttributeSchemaPtr mAttributeSchema;
    AttributeSchemaPtr mVirtualAttributeSchema;
    SummarySchemaPtr mSummarySchema;
    SourceSchemaPtr mSourceSchema;
    TruncateProfileSchemaPtr mTruncateProfileSchema;
    std::shared_ptr<FileCompressSchema> mFileCompressSchema;

    IndexPartitionSchemaImpl* mSchema;
    std::string mRegionName;
    std::string mTTLFieldName;
    int64_t mDefaultTTL; // use as BuildConfig.ttl & OnlineConfig.ttl default value, in second
    bool mEnableTemperatureLayer;
    TemperatureLayerConfigPtr mTemperatureLayer;
    std::string mOrderPreservingField;
    std::string mHashIdFieldName;
    bool mOwnFieldSchema;
    bool mMultiRegionFormat;
    bool mTTLFromDoc;

private:
    friend class IndexPartitionSchemaTest;
    friend class RegionSchema;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RegionSchemaImpl);
}} // namespace indexlib::config

#endif //__INDEXLIB_REGION_SCHEMA_IMPL_H
