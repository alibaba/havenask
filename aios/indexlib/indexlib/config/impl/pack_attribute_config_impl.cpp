#include "indexlib/config/impl/pack_attribute_config_impl.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/config_define.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, PackAttributeConfigImpl);

void PackAttributeConfigImpl::Jsonize(JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON)
    {
        json.Jsonize(PACK_ATTR_NAME_FIELD, mPackAttrName);
        vector<string> subAttrNames;
        GetSubAttributeNames(subAttrNames);
        json.Jsonize(PACK_ATTR_SUB_ATTR_FIELD, subAttrNames);

        string compressStr = mCompressType.GetCompressStr();
        json.Jsonize(PACK_ATTR_COMPRESS_TYPE_FIELD, compressStr);
        if (mDefragSlicePercent != FIELD_DEFAULT_DEFRAG_SLICE_PERCENT)
        {
            json.Jsonize(FIELD_DEFRAG_SLICE_PERCENT, mDefragSlicePercent);
        }
    }
}

void PackAttributeConfigImpl::AddAttributeConfig(
        const AttributeConfigPtr& attributeConfig)
{
    if (mPackAttrName == attributeConfig->GetAttrName())
    {
        INDEXLIB_FATAL_ERROR(Schema,
            "sub attribute name is the same as pack attribute name[%s]!",
            mPackAttrName.c_str());
    }
    
    mAttributeConfigVec.push_back(attributeConfig);
    mIsUpdatable = mIsUpdatable || attributeConfig->IsAttributeUpdatable();
}

const std::vector<AttributeConfigPtr>& 
PackAttributeConfigImpl::GetAttributeConfigVec() const
{
    return mAttributeConfigVec;
}

void PackAttributeConfigImpl::GetSubAttributeNames(
        std::vector<std::string>& fieldNames) const
{
    fieldNames.clear();
    for (size_t i = 0; i < mAttributeConfigVec.size(); ++i) 
    {
        fieldNames.push_back(mAttributeConfigVec[i]->GetAttrName());
    }
}

void PackAttributeConfigImpl::AssertEqual(const PackAttributeConfigImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mPackAttrName, other.mPackAttrName, "mPackAttrName not equal");
    IE_CONFIG_ASSERT_EQUAL(mCompressType, other.mCompressType, "mCompressType not equal");
    IE_CONFIG_ASSERT_EQUAL(mPackId, other.mPackId, "mPackId not equal");
    IE_CONFIG_ASSERT_EQUAL(mIsUpdatable, other.mIsUpdatable, "mIsUpdatable not equal");
    IE_CONFIG_ASSERT_EQUAL(mDefragSlicePercent, other.mDefragSlicePercent, "mDefragSlicePercent not equal");
    IE_CONFIG_ASSERT_EQUAL(mIsDisable, other.mIsDisable, "mIsDisable not equal");

    vector<string> subAttrNames;
    vector<string> otherSubAttrNames;
    GetSubAttributeNames(subAttrNames);
    other.GetSubAttributeNames(otherSubAttrNames);

    IE_CONFIG_ASSERT_EQUAL(subAttrNames, otherSubAttrNames, "sub attributes not equal");
}

AttributeConfigPtr PackAttributeConfigImpl::CreateAttributeConfig() const
{
    FieldConfigPtr fieldConfig(new FieldConfig(mPackAttrName,
                    ft_string, false, true, false));
    fieldConfig->SetCompressType(mCompressType.GetCompressStr());
    fieldConfig->SetDefragSlicePercent(mDefragSlicePercent);

    AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
    attrConfig->Init(fieldConfig);
    attrConfig->SetUpdatableMultiValue(mIsUpdatable);
    return attrConfig;
}

void PackAttributeConfigImpl::Disable()
{
    mIsDisable = true;
    for (auto& attrConfig : mAttributeConfigVec)
    {
        attrConfig->Disable();
    }
}

IE_NAMESPACE_END(config);

