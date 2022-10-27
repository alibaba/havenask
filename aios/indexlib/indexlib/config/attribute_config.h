#ifndef __INDEXLIB_ATTRIBUTE_CONFIG_H
#define __INDEXLIB_ATTRIBUTE_CONFIG_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/config/customized_config.h"
#include "indexlib/config/field_config.h"

DECLARE_REFERENCE_CLASS(config, PackAttributeConfig);
DECLARE_REFERENCE_CLASS(config, AttributeConfigImpl);
DECLARE_REFERENCE_CLASS(common, AttributeValueInitializerCreator);

IE_NAMESPACE_BEGIN(config);

class AttributeConfig
{
public:
    enum ConfigType
    {
        ct_normal,
        ct_virtual,
        ct_section,
        ct_pk,
        ct_unknown,
        ct_index_accompany,
    };
    
public:
    AttributeConfig(ConfigType type = ct_normal);
    virtual ~AttributeConfig() {}

public:
    void Init(const config::FieldConfigPtr& fieldConfig, 
              const common::AttributeValueInitializerCreatorPtr& creator
              = common::AttributeValueInitializerCreatorPtr(),
              const config::CustomizedConfigVector& customizedConfigs
              = config::CustomizedConfigVector());
    ConfigType GetConfigType() const;
    attrid_t GetAttrId() const;
    void SetAttrId(attrid_t id);
    virtual const std::string& GetAttrName() const;
    fieldid_t GetFieldId() const;
    size_t EstimateAttributeExpandSize(size_t docCount);
    FieldType GetFieldType() const;
    bool IsMultiValue() const;
    bool IsLengthFixed() const;
    void SetUniqEncode(bool encode);
    bool IsUniqEncode() const;
    bool IsMultiString() const;
    virtual CompressTypeOption GetCompressType() const;
    bool IsAttributeUpdatable() const;
    float GetDefragSlicePercent();
    void SetUpdatableMultiValue(bool IsUpdatableMultiValue);
    const FieldConfigPtr &GetFieldConfig() const;
    common::AttributeValueInitializerCreatorPtr GetAttrValueInitializerCreator() const;
    void AssertEqual(const AttributeConfig& other) const;
    void SetPackAttributeConfig(PackAttributeConfig* packAttrConfig);
    PackAttributeConfig* GetPackAttributeConfig() const;
    bool IsLoadPatchExpand();
    void SetCustomizedConfig(const CustomizedConfigVector& customizedConfigs);
    const CustomizedConfigVector& GetCustomizedConfigs() const;
    
    void Disable();
    bool IsDisable() const;
    void Delete();
    bool IsDeleted() const;
    bool IsNormal() const;
    IndexStatus GetStatus() const;

    void SetOwnerModifyOperationId(schema_opid_t opId);
    schema_opid_t GetOwnerModifyOperationId() const;

public:
    // for test
    void SetConfigType(ConfigType type);
    
private:
    AttributeConfigImplPtr mImpl;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeConfig);
typedef std::vector<AttributeConfigPtr> AttributeConfigVector;

class AttributeConfigIterator
{
public:
    AttributeConfigIterator(const AttributeConfigVector& configs)
        : mConfigs(configs)
    {}

    AttributeConfigVector::const_iterator Begin() const
    { return mConfigs.begin(); }
    
    AttributeConfigVector::const_iterator End() const
    { return mConfigs.end(); }
    
private:
    AttributeConfigVector mConfigs;
};
DEFINE_SHARED_PTR(AttributeConfigIterator);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_ATTRIBUTE_CONFIG_H
