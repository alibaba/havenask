#ifndef __INDEXLIB_DEFAULT_ATTRIBUTE_VALUE_INITIALIZER_H
#define __INDEXLIB_DEFAULT_ATTRIBUTE_VALUE_INITIALIZER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common/field_format/attribute/attribute_value_initializer.h"

IE_NAMESPACE_BEGIN(common);

class DefaultAttributeValueInitializer : public AttributeValueInitializer
{
public:
    DefaultAttributeValueInitializer(const std::string& valueStr);
    ~DefaultAttributeValueInitializer();

public:
    bool GetInitValue(
            docid_t docId, char* buffer, size_t bufLen) const override;

    bool GetInitValue(
            docid_t docId, autil::ConstString& value,
            autil::mem_pool::Pool *memPool) const override;

public:
    autil::ConstString GetDefaultValue(autil::mem_pool::Pool *memPool) const;

private:
    std::string mValueStr;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DefaultAttributeValueInitializer);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_DEFAULT_ATTRIBUTE_VALUE_INITIALIZER_H
