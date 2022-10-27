#ifndef __INDEXLIB_NUMBERINDEXTYPETRANSFORMOR_H
#define __INDEXLIB_NUMBERINDEXTYPETRANSFORMOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"

IE_NAMESPACE_BEGIN(config);

class NumberIndexTypeTransformor
{
public:
    NumberIndexTypeTransformor();
    ~NumberIndexTypeTransformor();
public:
    static void Transform(IndexConfigPtr indexConfig, IndexType& type);
    static IndexType TransformFieldTypeToIndexType(FieldType fieldType);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NumberIndexTypeTransformor);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_NUMBERINDEXTYPETRANSFORMOR_H
