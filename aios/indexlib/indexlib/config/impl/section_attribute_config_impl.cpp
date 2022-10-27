#include "indexlib/config/section_attribute_config.h"
#include "indexlib/config/impl/section_attribute_config_impl.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/field_config.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, SectionAttributeConfigImpl);

SectionAttributeConfigImpl::SectionAttributeConfigImpl()
    : mHasSectionWeight(true)
    , mHasFieldId(true)
{
}

SectionAttributeConfigImpl::SectionAttributeConfigImpl(
        const string& compressString,
        bool hasSectionWeight, bool hasFieldId)
    : mHasSectionWeight(hasSectionWeight)
    , mHasFieldId(hasFieldId)
{
    mCompressType.Init(compressString);
}

SectionAttributeConfigImpl::~SectionAttributeConfigImpl() 
{
}

void SectionAttributeConfigImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize(HAS_SECTION_WEIGHT, mHasSectionWeight, true);
    json.Jsonize(HAS_SECTION_FIELD_ID, mHasFieldId, true);
    if (json.GetMode() == TO_JSON)
    {
        string compressStr = mCompressType.GetCompressStr();
        if (!compressStr.empty())
        {
            json.Jsonize(COMPRESS_TYPE, compressStr);
        }
    }
    else
    {
        string compressStr;
        json.Jsonize(COMPRESS_TYPE, compressStr, compressStr);
        mCompressType.Init(compressStr);
    }
}

void SectionAttributeConfigImpl::AssertEqual(
        const SectionAttributeConfigImpl& other) const
{
    mCompressType.AssertEqual(other.mCompressType);

    IE_CONFIG_ASSERT_EQUAL(mHasSectionWeight, other.mHasSectionWeight,
                           "has_section_weight not equal");

    IE_CONFIG_ASSERT_EQUAL(mHasFieldId, other.mHasFieldId,
                           "has_field_id not equal");
}

bool SectionAttributeConfigImpl::operator==(
        const SectionAttributeConfigImpl& other) const
{
    return mCompressType == other.mCompressType
        && mHasSectionWeight == other.mHasSectionWeight
        && mHasFieldId == other.mHasFieldId;
}


bool SectionAttributeConfigImpl::operator!=(
        const SectionAttributeConfigImpl& other) const
{
    return !(*this == other);
}

AttributeConfigPtr SectionAttributeConfigImpl::CreateAttributeConfig(
        const string& indexName) const
{
    string fieldName = SectionAttributeConfig::IndexNameToSectionAttributeName(indexName);
    // single string attribute
    FieldConfigPtr fieldConfig(new FieldConfig(fieldName, ft_string, false, true, false));
    fieldConfig->SetCompressType(mCompressType.GetCompressStr());

    AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_section));
    attrConfig->Init(fieldConfig);
    return attrConfig;
}

IE_NAMESPACE_END(config);

