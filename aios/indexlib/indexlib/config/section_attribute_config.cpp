#include "indexlib/config/section_attribute_config.h"
#include "indexlib/config/impl/section_attribute_config_impl.h"
#include "indexlib/config/configurator_define.h"

using namespace std;
using namespace autil::legacy;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, SectionAttributeConfig);

SectionAttributeConfig::SectionAttributeConfig()
    : mImpl(new SectionAttributeConfigImpl)
{
}

SectionAttributeConfig::SectionAttributeConfig(
        const string& compressString,
        bool hasSectionWeight, bool hasFieldId)
    : mImpl(new SectionAttributeConfigImpl(compressString,
                                           hasSectionWeight,
                                           hasFieldId))
{
}

SectionAttributeConfig::~SectionAttributeConfig() 
{
}

bool SectionAttributeConfig::IsUniqEncode() const
{
    return mImpl->IsUniqEncode();
}

bool SectionAttributeConfig::HasEquivalentCompress() const
{
    return mImpl->HasEquivalentCompress();
}

bool SectionAttributeConfig::HasSectionWeight() const
{
    return mImpl->HasSectionWeight();
}

bool SectionAttributeConfig::HasFieldId() const
{
    return mImpl->HasFieldId();
}

void SectionAttributeConfig::Jsonize(Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}

void SectionAttributeConfig::AssertEqual(
        const SectionAttributeConfig& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}

bool SectionAttributeConfig::operator==(
        const SectionAttributeConfig& other) const
{
    return *mImpl == *(other.mImpl);
}


bool SectionAttributeConfig::operator!=(
        const SectionAttributeConfig& other) const
{
    return !(*this == other);
}

AttributeConfigPtr SectionAttributeConfig::CreateAttributeConfig(
        const string& indexName) const
{
    return mImpl->CreateAttributeConfig(indexName);
}

string SectionAttributeConfig::IndexNameToSectionAttributeName(
        const string& indexName)
{
    return indexName + SECTION_DIR_NAME_SUFFIX;
}

string SectionAttributeConfig::SectionAttributeNameToIndexName(
        const string& attrName)
{
    size_t suffixLen = SECTION_DIR_NAME_SUFFIX.length();
    if (attrName.size() <= suffixLen)
    {
        return "";
    }
    if (attrName.substr(attrName.size() - suffixLen) != SECTION_DIR_NAME_SUFFIX)
    {
        return "";
    }
    return attrName.substr(0, attrName.size() - suffixLen);
}

IE_NAMESPACE_END(config);

