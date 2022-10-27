#ifndef __INDEXLIB_PACK_ATTRIBUTE_CONFIG_H
#define __INDEXLIB_PACK_ATTRIBUTE_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/field_type_traits.h"
#include "autil/legacy/jsonizable.h"

DECLARE_REFERENCE_CLASS(config, PackAttributeConfigImpl);

IE_NAMESPACE_BEGIN(config);

class PackAttributeConfig : public autil::legacy::Jsonizable
{
public:
    PackAttributeConfig(const std::string& attrName,
                        const CompressTypeOption& compressType,
                        uint64_t defragSlicePercent);    
    ~PackAttributeConfig();
    
public:
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    
    const std::string& GetAttrName() const;
    CompressTypeOption GetCompressType() const;
    packattrid_t GetPackAttrId() const;
    void SetPackAttrId(packattrid_t packId);
    
    void AddAttributeConfig(const AttributeConfigPtr& attributeConfig);
    const std::vector<AttributeConfigPtr>& GetAttributeConfigVec() const;
    void GetSubAttributeNames(std::vector<std::string>& attrNames) const;

    void AssertEqual(const PackAttributeConfig& other) const;

    AttributeConfigPtr CreateAttributeConfig() const;

    bool IsUpdatable() const;
    
    static uint32_t GetFixLenFieldSize(const config::FieldConfigPtr& fieldConfig);

    void Disable();
    bool IsDisable() const;
    
private:
    PackAttributeConfigImplPtr mImpl;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackAttributeConfig);
typedef std::vector<PackAttributeConfigPtr> PackAttributeConfigVector;
class PackAttributeConfigIterator
{
public:
    PackAttributeConfigIterator(const PackAttributeConfigVector& configs)
        : mConfigs(configs)
    {}

    PackAttributeConfigVector::const_iterator Begin() const
    { return mConfigs.begin(); }
    
    PackAttributeConfigVector::const_iterator End() const
    { return mConfigs.end(); }
    
private:
    PackAttributeConfigVector mConfigs;
};
DEFINE_SHARED_PTR(PackAttributeConfigIterator);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_PACK_ATTRIBUTE_CONFIG_H
