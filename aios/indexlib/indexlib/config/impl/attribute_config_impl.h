#ifndef __INDEXLIB_ATTRIBUTE_CONFIG_IMPL_H
#define __INDEXLIB_ATTRIBUTE_CONFIG_IMPL_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/config/field_config.h"
#include "indexlib/config/customized_config.h"
#include "indexlib/config/attribute_config.h"

DECLARE_REFERENCE_CLASS(config, PackAttributeConfig);
DECLARE_REFERENCE_CLASS(common, AttributeValueInitializerCreator);

IE_NAMESPACE_BEGIN(config);

class AttributeConfigImpl
{
public:
    AttributeConfigImpl(config::AttributeConfig::ConfigType type =
                        config::AttributeConfig::ct_normal);
    AttributeConfigImpl()
        : mAttrId(0)
        , mConfigType(config::AttributeConfig::ct_normal)
        , mPackAttrConfig(NULL)
        , mStatus(is_normal)
        , mOwnerOpId(INVALID_SCHEMA_OP_ID)
    {}
    
    virtual ~AttributeConfigImpl() {}

public:
    void Init(const config::FieldConfigPtr& fieldConfig, 
              const common::AttributeValueInitializerCreatorPtr& creator
              = common::AttributeValueInitializerCreatorPtr(),
              const config::CustomizedConfigVector& customizedConfigs
              = config::CustomizedConfigVector())
    {
        SetFieldConfig(fieldConfig);
        SetAttrValueInitializerCreator(creator);
        mCustomizedConfigs = customizedConfigs;   
    }

    config::AttributeConfig::ConfigType GetConfigType() const { return mConfigType; }
    attrid_t GetAttrId() const {return mAttrId;}
    void SetAttrId(attrid_t id) {mAttrId = id;}

    virtual const std::string& GetAttrName() const 
    {
        assert(mFieldConfig.get() != NULL);
        return mFieldConfig->GetFieldName();
    }

    fieldid_t GetFieldId() const
    {
        assert(mFieldConfig.get() != NULL);
        return mFieldConfig->GetFieldId();
    }
    
    size_t EstimateAttributeExpandSize(size_t docCount);
    
    FieldType GetFieldType() const 
    {
        assert(mFieldConfig.get() != NULL);
        return mFieldConfig->GetFieldType();
    }

    bool IsMultiValue() const
    {
        assert(mFieldConfig.get() != NULL);
        return mFieldConfig->IsMultiValue();
    }

    bool IsLengthFixed() const
    {
        if (!mFieldConfig->IsMultiValue())
        {
            if (mFieldConfig->GetFieldType() == ft_string)
            {
                return mFieldConfig->GetFixedMultiValueCount() != -1;
            }
            return true;
        }

        if (mFieldConfig->GetFieldType() == ft_string)
        {
            return false;
        }
        return mFieldConfig->GetFixedMultiValueCount() != -1;
    }

    void SetUniqEncode(bool encode)
    {
        assert(mFieldConfig);
        mFieldConfig->SetUniqEncode(encode);
    }

    inline bool IsUniqEncode() const
    {
        assert(mFieldConfig);
        return mFieldConfig->IsUniqEncode();
    }

    inline bool IsMultiString() const 
    {
        return GetFieldType() == ft_string && IsMultiValue();
    }

    virtual CompressTypeOption GetCompressType() const
    { return mFieldConfig->GetCompressType(); }

    inline bool IsAttributeUpdatable() const
    {
        assert(mFieldConfig);
        return mFieldConfig->IsAttributeUpdatable();
    }
    
    inline float GetDefragSlicePercent() 
    {
        return mFieldConfig->GetDefragSlicePercent();
    }

    void SetUpdatableMultiValue(bool IsUpdatableMultiValue)
    {
        assert(mFieldConfig);
        mFieldConfig->SetUpdatableMultiValue(IsUpdatableMultiValue);
    }

    void SetOwnerModifyOperationId(schema_opid_t opId) { mOwnerOpId = opId; }
    schema_opid_t GetOwnerModifyOperationId() const { return mOwnerOpId; }

    const FieldConfigPtr &GetFieldConfig() const {return mFieldConfig;}

    common::AttributeValueInitializerCreatorPtr GetAttrValueInitializerCreator() const
    { return mValueInitializerCreator; }

    void AssertEqual(const AttributeConfigImpl& other) const;

    void SetPackAttributeConfig(PackAttributeConfig* packAttrConfig)
    { mPackAttrConfig = packAttrConfig; }

    PackAttributeConfig* GetPackAttributeConfig() const
    { return mPackAttrConfig; }

    bool IsLoadPatchExpand();

    void SetCustomizedConfig(const CustomizedConfigVector& customizedConfigs)
    { mCustomizedConfigs = customizedConfigs; }

    const CustomizedConfigVector& GetCustomizedConfigs() const
    { return mCustomizedConfigs; }
    
    void Disable() { mStatus = (mStatus == is_normal) ? is_disable : mStatus ; }
    bool IsDisable() const { return mStatus == is_disable; }
    void Delete();
    bool IsDeleted() const { return mStatus == is_deleted; }
    bool IsNormal() const { return mStatus == is_normal; }
    IndexStatus GetStatus() const { return mStatus; }

public:
    // for test
    void SetConfigType(config::AttributeConfig::ConfigType type)
    { mConfigType = type; }
    
private:
    void SetFieldConfig(const FieldConfigPtr& fieldConfig);
    void SetAttrValueInitializerCreator(
            const common::AttributeValueInitializerCreatorPtr& creator);

private:
    attrid_t mAttrId;
    config::AttributeConfig::ConfigType mConfigType;
    PackAttributeConfig* mPackAttrConfig;
    FieldConfigPtr mFieldConfig;
    common::AttributeValueInitializerCreatorPtr mValueInitializerCreator;
    //customized config
    CustomizedConfigVector mCustomizedConfigs;
    IndexStatus mStatus;
    schema_opid_t mOwnerOpId;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeConfigImpl);
typedef std::vector<AttributeConfigImplPtr> AttributeConfigImplVector;

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_ATTRIBUTE_CONFIG_IMPL_H
