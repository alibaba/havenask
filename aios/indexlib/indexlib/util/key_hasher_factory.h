#ifndef __INDEXLIB_KEY_HASHER_FACTORY_H
#define __INDEXLIB_KEY_HASHER_FACTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/key_hasher_typed.h"

IE_NAMESPACE_BEGIN(util);

class KeyHasherFactory
{
public:
    KeyHasherFactory();
    ~KeyHasherFactory();
public:
    static KeyHasher* Create(IndexType indexType);
    static KeyHasher* CreateByFieldType(FieldType fieldType);
    static KeyHasher* CreatePrimaryKeyHasher(FieldType fieldType, PrimaryKeyHashType hashType);
    static KeyHasher* Create(FieldType fieldType, bool useNumberHash); 

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KeyHasherFactory);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_KEY_HASHER_FACTORY_H
