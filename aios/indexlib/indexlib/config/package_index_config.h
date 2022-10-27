#ifndef __INDEXLIB_PACKAGE_INDEX_CONFIG_H
#define __INDEXLIB_PACKAGE_INDEX_CONFIG_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/section_attribute_config.h"

DECLARE_REFERENCE_CLASS(config, PackageIndexConfigImpl);

IE_NAMESPACE_BEGIN(config);

class PackageIndexConfig : public IndexConfig
{
public:
    static const uint32_t PACK_MAX_FIELD_NUM;
    static const uint32_t EXPACK_MAX_FIELD_NUM;
    static const uint32_t CUSTOMIZED_MAX_FIELD_NUM; 

public:
    PackageIndexConfig(const std::string& indexName, IndexType indexType);
    PackageIndexConfig(const PackageIndexConfig& other);
    ~PackageIndexConfig();

public:
    uint32_t GetFieldCount() const override;
    IndexConfig::Iterator CreateIterator() const override; 
    int32_t GetFieldIdxInPack(fieldid_t id) const override;
    bool IsInIndex(fieldid_t fieldId) const override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const IndexConfig& other) const override;
    void AssertCompatible(const IndexConfig& other) const override;
    IndexConfig* Clone() const override;

    int32_t GetFieldBoost(fieldid_t id) const; 
    void SetFieldBoost(fieldid_t id, int32_t boost);
    int32_t GetFieldIdxInPack(const std::string& fieldName) const;
    fieldid_t GetFieldId(int32_t fieldIdxInPack) const;
    int32_t GetTotalFieldBoost() const;
    void AddFieldConfig(const FieldConfigPtr& fieldConfig, int32_t boost = 0);
    void AddFieldConfig(fieldid_t id,int32_t boost = 0);
    void AddFieldConfig(const std::string& fieldName, int32_t boost = 0);
    bool HasSectionAttribute() const;
    const SectionAttributeConfigPtr &GetSectionAttributeConfig() const;
    void SetHasSectionAttributeFlag(bool flag);
    const FieldConfigVector& GetFieldConfigVector() const;
    void SetFieldSchema(const FieldSchemaPtr& fieldSchema);
    void SetDefaultAnalyzer();
    void SetMaxFirstOccInDoc(pos_t pos);
    pos_t GetMaxFirstOccInDoc() const; 

    //only for test
    void SetSectionAttributeConfig(SectionAttributeConfigPtr sectionAttributeConfig);

protected:
    bool CheckFieldType(FieldType ft) const override;
    void ResetImpl(IndexConfigImpl* impl);
    
private:
    PackageIndexConfigImpl* mImpl;
    
private:
    friend class PackageIndexConfigTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackageIndexConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_PACKAGE_INDEX_CONFIG_H
