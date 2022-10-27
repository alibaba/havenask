#ifndef __INDEXLIB_PACKAGE_INDEX_CONFIG_IMPL_H
#define __INDEXLIB_PACKAGE_INDEX_CONFIG_IMPL_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/config/field_schema.h"
#include "indexlib/config/impl/index_config_impl.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/config/impl/package_index_config_impl.h"
#include "indexlib/config/section_attribute_config.h"

IE_NAMESPACE_BEGIN(config);

class PackageIndexConfigImpl : public IndexConfigImpl
{
public:
    PackageIndexConfigImpl(const std::string& indexName, IndexType indexType);
    PackageIndexConfigImpl(const PackageIndexConfigImpl& other);
    ~PackageIndexConfigImpl();

public:
    uint32_t GetFieldCount() const override
    { return mIndexFields.size(); }
    IndexConfig::Iterator CreateIterator() const override 
    {
        return IndexConfig::Iterator(mIndexFields);
    }
    
    int32_t GetFieldBoost(fieldid_t id) const 
    {
        return (size_t)id < mFieldBoosts.size() ? mFieldBoosts[id] : 0;
    }
    void SetFieldBoost(fieldid_t id, int32_t boost);

    int32_t GetFieldIdxInPack(fieldid_t id) const override
    {
        return (size_t)id < mFieldIdxInPack.size() ? mFieldIdxInPack[id] : -1;
    }

    int32_t GetFieldIdxInPack(const std::string& fieldName) const
    {
        fieldid_t fid = mFieldSchema->GetFieldId(fieldName);
        if (fid == INVALID_FIELDID)
        {
            return -1;
        }
        return GetFieldIdxInPack(fid);
    }

    fieldid_t GetFieldId(int32_t fieldIdxInPack) const;

    int32_t GetTotalFieldBoost() const {return mTotalFieldBoost;}

    void AddFieldConfig(const FieldConfigPtr& fieldConfig, int32_t boost = 0);
    void AddFieldConfig(fieldid_t id,int32_t boost = 0);
    void AddFieldConfig(const std::string& fieldName, int32_t boost = 0);

    bool HasSectionAttribute() const
    {
        return mHasSectionAttribute && (mOptionFlag & of_position_list);
    }

    const SectionAttributeConfigPtr &GetSectionAttributeConfig() const;
    
    void SetHasSectionAttributeFlag(bool flag) { mHasSectionAttribute = flag; }

    const FieldConfigVector& GetFieldConfigVector() const
    { return mIndexFields; }

    void SetFieldSchema(const FieldSchemaPtr& fieldSchema)
    { mFieldSchema = fieldSchema; }

    bool IsInIndex(fieldid_t fieldId) const override;

    void SetMaxFirstOccInDoc(pos_t pos)
    {
        mMaxFirstOccInDoc = pos;
    }

    pos_t GetMaxFirstOccInDoc() const 
    {
        return mMaxFirstOccInDoc;
    }
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const IndexConfigImpl& other) const override;
    void AssertCompatible(const IndexConfigImpl& other) const override;
    IndexConfigImpl* Clone() const override;
    void SetDefaultAnalyzer();

    //only for test
    void SetSectionAttributeConfig(SectionAttributeConfigPtr sectionAttributeConfig)
    {
        mSectionAttributeConfig = sectionAttributeConfig;
    }

    bool CheckFieldType(FieldType ft) const override
    { return ft == ft_text; }

private:
    std::vector<int32_t> mFieldBoosts;
    std::vector<firstocc_t> mFieldHits;
    std::vector<int32_t> mFieldIdxInPack; // map fieldId => fieldIdxInPack
    FieldConfigVector mIndexFields;
    int32_t mTotalFieldBoost;
    FieldSchemaPtr mFieldSchema;
    bool mHasSectionAttribute;
    pos_t mMaxFirstOccInDoc;
    SectionAttributeConfigPtr mSectionAttributeConfig;

private:
    friend class PackageIndexConfig;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackageIndexConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_PACKAGE_INDEX_CONFIG_IMPL_H
