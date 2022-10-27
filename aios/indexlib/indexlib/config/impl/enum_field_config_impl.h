#ifndef __INDEXLIB_ENUM_FIELD_CONFIG_IMPL_H
#define __INDEXLIB_ENUM_FIELD_CONFIG_IMPL_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include <vector>
#include <string>
#include <algorithm>
#include <functional>
#include "indexlib/config/impl/field_config_impl.h"

IE_NAMESPACE_BEGIN(config);

class EnumFieldConfigImpl : public FieldConfigImpl
{
public:
    typedef std::vector<std::string> ValueVector;
public:
    EnumFieldConfigImpl()
    {
        mFieldType = ft_enum;
    }

    EnumFieldConfigImpl(const std::string& fieldName, bool multiValue)
        :FieldConfigImpl(fieldName, ft_enum, multiValue)
    {}

    ~EnumFieldConfigImpl() {}
public:
    void AddValidValue(const std::string& validValue)
    {  mValidValues.push_back(validValue); }
    
    void AddValidValue(const ValueVector& validValues)
    { 
        std::copy(validValues.begin(), validValues.end(), 
                  std::back_inserter(mValidValues));
    }
    
    const ValueVector& GetValidValues() const 
    { return mValidValues; }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    void AssertEqual(const EnumFieldConfigImpl& other) const;

private:
    ValueVector mValidValues;
private:
    friend class FieldConfigTest;
private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<EnumFieldConfigImpl> EnumFieldConfigImplPtr;

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_ENUM_FIELD_CONFIG_IMPL_H
