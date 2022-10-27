#ifndef __INDEXLIB_PACK_ATTRIBUTE_CONFIG_IMPL_H
#define __INDEXLIB_PACK_ATTRIBUTE_CONFIG_IMPLH

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/field_type_traits.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(config);

class PackAttributeConfigImpl : public autil::legacy::Jsonizable
{
public:
    PackAttributeConfigImpl(const std::string& attrName,
                            const CompressTypeOption& compressType,
                            uint64_t defragSlicePercent)
        : mPackAttrName(attrName)
        , mCompressType(compressType)
        , mPackId(INVALID_PACK_ATTRID)
        , mIsUpdatable(false)
        , mDefragSlicePercent(defragSlicePercent)
        , mIsDisable(false)
    {
        assert(attrName.size() > 0);
    }
    
    ~PackAttributeConfigImpl() {}
    
public:
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    
    const std::string& GetAttrName() const
    { return mPackAttrName; }

    CompressTypeOption GetCompressType() const
    { return mCompressType; }

    packattrid_t GetPackAttrId() const { return mPackId; }
    void SetPackAttrId(packattrid_t packId) { mPackId = packId; }
    
    void AddAttributeConfig(const AttributeConfigPtr& attributeConfig);
    const std::vector<AttributeConfigPtr>& GetAttributeConfigVec() const;
    void GetSubAttributeNames(std::vector<std::string>& attrNames) const;

    void AssertEqual(const PackAttributeConfigImpl& other) const;

    AttributeConfigPtr CreateAttributeConfig() const;

    bool IsUpdatable() const
    { return mIsUpdatable; }

    void Disable();

    bool IsDisable() const
    { return mIsDisable; }

private:
    std::string mPackAttrName;
    CompressTypeOption mCompressType;
    packattrid_t mPackId;
    std::vector<AttributeConfigPtr> mAttributeConfigVec;
    bool mIsUpdatable;
    uint64_t mDefragSlicePercent;
    bool mIsDisable;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackAttributeConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_PACK_ATTRIBUTE_CONFIG_IMPL_H
