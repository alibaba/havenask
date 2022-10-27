#ifndef __INDEXLIB_REGION_SCHEMA_IMPL_H
#define __INDEXLIB_REGION_SCHEMA_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/config/truncate_profile_schema.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/impl/kkv_index_config_impl.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchemaImpl);
DECLARE_REFERENCE_CLASS(config, RegionSchema);

IE_NAMESPACE_BEGIN(config);

class RegionSchemaImpl : public autil::legacy::Jsonizable
{
public:
    RegionSchemaImpl(IndexPartitionSchemaImpl* schema, bool multiRegionFormat = false);
    ~RegionSchemaImpl();
    
public:
    const std::string& GetRegionName() const { return mRegionName; }
    
    const FieldSchemaPtr& GetFieldSchema() const {return mFieldSchema;}
    const IndexSchemaPtr& GetIndexSchema() const {return mIndexSchema;}
    const AttributeSchemaPtr& GetAttributeSchema() const {return mAttributeSchema;}
    const AttributeSchemaPtr& GetVirtualAttributeSchema() const {return mVirtualAttributeSchema;}
    const SummarySchemaPtr& GetSummarySchema() const { return mSummarySchema; }
    const TruncateProfileSchemaPtr& GetTruncateProfileSchema() const
    { return mTruncateProfileSchema; }
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    
    bool IsUsefulField(const std::string& fieldName) const;
    bool TTLEnabled() const;
    int64_t GetDefaultTTL() const;
    bool SupportAutoUpdate() const;
    bool NeedStoreSummary() const
    { return mSummarySchema ? mSummarySchema->NeedStoreSummary() : false; }

    void CloneVirtualAttributes(const RegionSchemaImpl& other);
    bool AddVirtualAttributeConfigs(const AttributeConfigVector& virtualAttrConfigs);

public:
    void AssertEqual(const RegionSchemaImpl& other) const;
    void AssertCompatible(const RegionSchemaImpl& other) const;
    void Check() const;
    
public:
    // for test
    void SetTruncateProfileSchema(const TruncateProfileSchemaPtr& truncateProfileSchema)
    { mTruncateProfileSchema = truncateProfileSchema; }

    void SetRegionName(const std::string& name)  { mRegionName = name; }

    FieldConfigPtr AddFieldConfig(const std::string& fieldName, 
                                  FieldType fieldType, 
                                  bool multiValue = false,
                                  bool isVirtual = false,
                                  bool isBinary = false);
    
    EnumFieldConfigPtr AddEnumFieldConfig(const std::string& fieldName, 
            FieldType fieldType, std::vector<std::string>& validValues, 
            bool multiValue = false);

    void AddIndexConfig(const IndexConfigPtr& indexConfig);
    void AddAttributeConfig(const std::string& fieldName,
                            const CustomizedConfigVector& customizedConfigs = CustomizedConfigVector());

    void AddAttributeConfig(const config::AttributeConfigPtr& attrConfig);
    void AddPackAttributeConfig(const std::string& attrName,
                                const std::vector<std::string>& subAttrNames,
                                const std::string& compressTypeStr);
    
    void AddPackAttributeConfig(const std::string& attrName,
                                const std::vector<std::string>& subAttrNames,
                                const std::string& compressTypeStr,
                                uint64_t defragSlicePercent);

    void AddVirtualAttributeConfig(const config::AttributeConfigPtr& virtualAttrConfig);

    void AddSummaryConfig(const std::string& fieldName,
                          summarygroupid_t summaryGroupId = DEFAULT_SUMMARYGROUPID);
    void AddSummaryConfig(fieldid_t fieldId,
                          summarygroupid_t summaryGroupId = DEFAULT_SUMMARYGROUPID);
    void SetSummaryCompress(bool compress, const std::string& compressType = "",
                            summarygroupid_t summaryGroupId = DEFAULT_SUMMARYGROUPID);
    summarygroupid_t CreateSummaryGroup(const std::string& summaryGroupName);
    
    void SetDefaultTTL(int64_t defaultTTL, const std::string& ttlFieldName);
    void SetEnableTTL(bool enableTTL, const std::string& ttlFieldName);

    const std::string& GetTTLFieldName() const { return mTTLFieldName; }

    void SetFieldSchema(const config::FieldSchemaPtr& fieldSchema)
    {
        mOwnFieldSchema = true;        
        mFieldSchema = fieldSchema;
    }

public:
    // for testlib
    void LoadValueConfig();
    void SetNeedStoreSummary();

    void ResetIndexSchema()
    { mIndexSchema.reset(new IndexSchema); }

    std::vector<config::AttributeConfigPtr> EnsureSpatialIndexWithAttribute();

    void SetBaseSchemaImmutable();
    void SetModifySchemaImmutable();
    void SetModifySchemaMutable();
    
private:
    AttributeConfigPtr CreateAttributeConfig(const std::string& fieldName,
                                             const CustomizedConfigVector& customizedConfigs = CustomizedConfigVector());
    AttributeConfigPtr CreateVirtualAttributeConfig(
            const config::FieldConfigPtr& fieldConfig,
            const common::AttributeValueInitializerCreatorPtr& creator);
    
    fieldid_t GetFieldIdForVirtualAttribute() const;
    
    void OptimizeKKVSKeyStore(const KKVIndexConfigPtr& kkvIndexConfig,
                              std::vector<AttributeConfigPtr>& attrConfigs);

    void InitTruncateIndexConfigs();
    IndexConfigPtr CreateTruncateIndexConfig(const IndexConfigPtr& indexConfig, 
            const std::string& profileName);

    void ToJson(autil::legacy::Jsonizable::JsonWrapper& json);
    void FromJson(autil::legacy::Jsonizable::JsonWrapper& json);

    void LoadFieldSchema(const autil::legacy::Any& any);
    void LoadTruncateProfileSchema(const autil::legacy::Any& any);
    void LoadIndexSchema(const autil::legacy::Any& any);
    void LoadAttributeSchema(const autil::legacy::Any& any);
    void LoadAttributeConfig(const autil::legacy::Any& any);
    void LoadPackAttributeConfig(const autil::legacy::Any& any);
    bool LoadSummaryGroup(const autil::legacy::Any& any, summarygroupid_t summaryGroupId);
    void LoadSummarySchema(const autil::legacy::Any& any);

    void CheckIndexSchema() const;
    void CheckFieldSchema() const;
    void CheckAttributeSchema() const;
    void CheckIndexNameNotSummary(const IndexConfigPtr& indexConfig) const;
    void CheckSingleFieldIndex(const IndexConfigPtr& indexConfig,
                               std::vector<uint32_t>& singleFieldIndexCounts) const;
    void CheckFieldsOrderInPackIndex(const IndexConfigPtr& indexConfig) const;
    void CheckTruncateSortParams() const;
    void CheckIndexTruncateProfiles(const IndexConfigPtr& indexConfig) const;

    void CheckSpatialIndexConfig(const IndexConfigPtr& indexConfig) const;

private:
    FieldSchemaPtr mFieldSchema;
    IndexSchemaPtr mIndexSchema;
    AttributeSchemaPtr mAttributeSchema;
    AttributeSchemaPtr mVirtualAttributeSchema;
    SummarySchemaPtr mSummarySchema;
    TruncateProfileSchemaPtr mTruncateProfileSchema;

    IndexPartitionSchemaImpl* mSchema;
    std::string mRegionName;
    std::string mTTLFieldName;
    int64_t mDefaultTTL; // use as BuildConfig.ttl & OnlineConfig.ttl default value, in second
    bool mOwnFieldSchema;
    bool mMultiRegionFormat;
    
private:
    friend class IndexPartitionSchemaTest;
    friend class RegionSchema;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RegionSchemaImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_REGION_SCHEMA_IMPL_H
