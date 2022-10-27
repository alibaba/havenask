#ifndef __INDEXLIB_SECTION_ATTRIBUTE_CONFIG_H
#define __INDEXLIB_SECTION_ATTRIBUTE_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/config_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/compress_type_option.h"

DECLARE_REFERENCE_CLASS(config, SectionAttributeConfigImpl);

IE_NAMESPACE_BEGIN(config);

class SectionAttributeConfig : public autil::legacy::Jsonizable
{
public:
    SectionAttributeConfig();
    //for test only
    SectionAttributeConfig(const std::string& compressString, 
                           bool hasSectionWeight, bool hasFieldId);
    ~SectionAttributeConfig();
public:
    bool IsUniqEncode() const;
    bool HasEquivalentCompress() const;

    bool HasSectionWeight() const;
    bool HasFieldId() const;
    void AssertEqual(const SectionAttributeConfig& other) const;
    bool operator==(const SectionAttributeConfig& other) const;
    bool operator!=(const SectionAttributeConfig& other) const;

    AttributeConfigPtr CreateAttributeConfig(const std::string& indexName) const;

    static std::string IndexNameToSectionAttributeName(const std::string& indexName);
    static std::string SectionAttributeNameToIndexName(const std::string& attrName);

public:
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

private:
    SectionAttributeConfigImplPtr mImpl;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SectionAttributeConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SECTION_ATTRIBUTE_CONFIG_H
