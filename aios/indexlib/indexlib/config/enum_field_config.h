#ifndef __INDEXLIB_ENUM_FIELD_CONFIG_H
#define __INDEXLIB_ENUM_FIELD_CONFIG_H

#include <tr1/memory>
#include <vector>
#include <string>
#include <algorithm>
#include <functional>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/config/field_config.h"

IE_NAMESPACE_BEGIN(config);
class EnumFieldConfigImpl;
DEFINE_SHARED_PTR(EnumFieldConfigImpl);

class EnumFieldConfig : public FieldConfig
{
public:
    typedef std::vector<std::string> ValueVector;
public:
    EnumFieldConfig();
    EnumFieldConfig(const std::string& fieldName, bool multiValue);
    ~EnumFieldConfig() {}
public:
    void AddValidValue(const std::string& validValue);
    void AddValidValue(const ValueVector& validValues);
    const ValueVector& GetValidValues() const;
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    virtual void AssertEqual(const FieldConfig& other) const;
    
private:
    EnumFieldConfigImpl* mImpl;
    
private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<EnumFieldConfig> EnumFieldConfigPtr;

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_ENUM_FIELD_CONFIG_H
