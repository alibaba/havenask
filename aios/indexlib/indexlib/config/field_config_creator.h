#ifndef __INDEXLIB_FIELD_CONFIG_CREATOR_H
#define __INDEXLIB_FIELD_CONFIG_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(config);

class FieldConfigCreator
{
public:
    FieldConfigCreator();
    ~FieldConfigCreator();
    
public:
    static void LoadFieldSchema

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FieldConfigCreator);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_FIELD_CONFIG_CREATOR_H
