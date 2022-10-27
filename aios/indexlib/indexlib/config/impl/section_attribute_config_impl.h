#ifndef __INDEXLIB_SECTION_ATTRIBUTE_CONFIG_IMPL_H
#define __INDEXLIB_SECTION_ATTRIBUTE_CONFIG_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/config_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/compress_type_option.h"

IE_NAMESPACE_BEGIN(config);

class SectionAttributeConfigImpl : public autil::legacy::Jsonizable
{
public:
    SectionAttributeConfigImpl();
    //for test only
    SectionAttributeConfigImpl(const std::string& compressString, 
                           bool hasSectionWeight, bool hasFieldId);
    ~SectionAttributeConfigImpl();
public:
    bool IsUniqEncode() const
    { return mCompressType.HasUniqEncodeCompress(); }
    bool HasEquivalentCompress() const
    { return mCompressType.HasEquivalentCompress(); }

    bool HasSectionWeight() const { return mHasSectionWeight; }
    bool HasFieldId() const { return mHasFieldId; }
    void AssertEqual(const SectionAttributeConfigImpl& other) const;
    bool operator==(const SectionAttributeConfigImpl& other) const;
    bool operator!=(const SectionAttributeConfigImpl& other) const;

    AttributeConfigPtr CreateAttributeConfig(const std::string& indexName) const;

public:
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

private:
    CompressTypeOption mCompressType;
    bool mHasSectionWeight;
    bool mHasFieldId;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SectionAttributeConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SECTION_ATTRIBUTE_CONFIG_IMPL_H
