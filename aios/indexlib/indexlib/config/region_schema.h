#ifndef __INDEXLIB_REGION_SCHEMA_H
#define __INDEXLIB_REGION_SCHEMA_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/config/truncate_profile_schema.h"

IE_NAMESPACE_BEGIN(config);

class RegionSchemaImpl;
DEFINE_SHARED_PTR(RegionSchemaImpl);
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
    const TruncateProfileSchemaPtr& GetTruncateProfileSchema() const;
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    bool IsUsefulField(const std::string& fieldName) const;
    bool TTLEnabled() const;
    int64_t GetDefaultTTL() const;
    bool SupportAutoUpdate() const;
    bool NeedStoreSummary() const;
    void CloneVirtualAttributes(const RegionSchema& other);
    bool AddVirtualAttributeConfigs(const AttributeConfigVector& virtualAttrConfigs);
    const std::string& GetTTLFieldName() const;

public:
    void AssertEqual(const RegionSchema& other) const;
    void AssertCompatible(const RegionSchema& other) const;
    void Check() const;

public:
    // for test
    void SetTruncateProfileSchema(const TruncateProfileSchemaPtr& truncateProfileSchema);
    void SetRegionName(const std::string& name);
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
    void AddPackAttributeConfig(const std::string& attrName,
                                const std::vector<std::string>& subAttrNames,
                                const std::string& compressTypeStr);
    void AddPackAttributeConfig(const std::string& attrName,
                                const std::vector<std::string>& subAttrNames,
                                const std::string& compressTypeStr,
                                uint64_t defragSlicePercent);
    void AddAttributeConfig(const config::AttributeConfigPtr& attrConfig);
    void AddVirtualAttributeConfig(const config::AttributeConfigPtr& virtualAttrConfig);
    void AddSummaryConfig(const std::string& fieldName,
                          summarygroupid_t summaryGroupId = DEFAULT_SUMMARYGROUPID);
    void AddSummaryConfig(fieldid_t fieldId,
                          summarygroupid_t summaryGroupId = DEFAULT_SUMMARYGROUPID);
    void SetSummaryCompress(bool compress, const std::string& compressType = "",
                            summarygroupid_t summaryGroupId = DEFAULT_SUMMARYGROUPID);
    summarygroupid_t CreateSummaryGroup(const std::string& summaryGroupName);

    void SetFieldSchema(const config::FieldSchemaPtr& fieldSchema);
    
    void SetDefaultTTL(int64_t defaultTTL,
                       const std::string& fieldName =DOC_TIME_TO_LIVE_IN_SECONDS);
    
    void SetEnableTTL(bool enableTTL,
                      const std::string& fieldName =DOC_TIME_TO_LIVE_IN_SECONDS);

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
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RegionSchema);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_REGION_SCHEMA_H
