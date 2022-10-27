#ifndef __INDEXLIB_VALUE_CONFIG_IMPL_H
#define __INDEXLIB_VALUE_CONFIG_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"

IE_NAMESPACE_BEGIN(config);

class ValueConfigImpl
{
public:
    ValueConfigImpl()
        : mEnableCompactPackAttribute(false)
        , mFixedValueLen(-1)
        , mActualFieldType(FieldType::ft_unknown)
    {}
    ~ValueConfigImpl() {}
    
public:
    void Init(const std::vector<AttributeConfigPtr>& attrConfigs);
    size_t GetAttributeCount() const { return mAttrConfigs.size(); }
    const config::AttributeConfigPtr& GetAttributeConfig(size_t idx) const
    {
        assert(idx < GetAttributeCount());
        return mAttrConfigs[idx];
    }

    config::PackAttributeConfigPtr CreatePackAttributeConfig() const;
    void EnableCompactFormat(bool toSetCompactFormat)
    {
        mEnableCompactPackAttribute = toSetCompactFormat;
    }
    bool IsCompactFormatEnabled() const
    {
        return mEnableCompactPackAttribute;
    }
    int32_t GetFixedLength() const
    {
        return mEnableCompactPackAttribute ? mFixedValueLen : -1;
    }
    FieldType GetFixedLengthValueType() const;
    FieldType GetActualFieldType() const;

private:
    FieldType CalculateFixLenValueType(int32_t size) const;

private:
    std::vector<config::AttributeConfigPtr> mAttrConfigs;
    bool mEnableCompactPackAttribute;
    int32_t mFixedValueLen;
    FieldType mActualFieldType;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ValueConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_VALUE_CONFIG_IMPL_H
