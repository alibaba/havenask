#ifndef __INDEXLIB_VALUE_CONFIG_H
#define __INDEXLIB_VALUE_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/pack_attribute_config.h"

DECLARE_REFERENCE_CLASS(config, ValueConfigImpl);

IE_NAMESPACE_BEGIN(config);

class ValueConfig
{
public:
    ValueConfig();
    ValueConfig(const ValueConfig& other);
    ~ValueConfig();
    
public:
    void Init(const std::vector<AttributeConfigPtr>& attrConfigs);
    size_t GetAttributeCount() const;
    const config::AttributeConfigPtr& GetAttributeConfig(size_t idx) const;

    config::PackAttributeConfigPtr CreatePackAttributeConfig() const;
    void EnableCompactFormat(bool toSetCompactFormat);
    bool IsCompactFormatEnabled() const;
    int32_t GetFixedLength() const;
    FieldType GetFixedLengthValueType() const;
    FieldType GetActualFieldType() const;
private:
    ValueConfigImplPtr mImpl;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ValueConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_VALUE_CONFIG_H
