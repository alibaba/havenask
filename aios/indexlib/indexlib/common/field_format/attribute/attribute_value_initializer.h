#ifndef __INDEXLIB_ATTRIBUTE_VALUE_INITIALIZER_H
#define __INDEXLIB_ATTRIBUTE_VALUE_INITIALIZER_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(common);

class AttributeValueInitializer
{
public:
    AttributeValueInitializer() {}
    virtual ~AttributeValueInitializer() {}

public:
    virtual bool GetInitValue(
            docid_t docId, char* buffer, size_t bufLen) const = 0;

    virtual bool GetInitValue(
            docid_t docId, autil::ConstString& value, 
            autil::mem_pool::Pool *memPool) const = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeValueInitializer);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_ATTRIBUTE_VALUE_INITIALIZER_H
