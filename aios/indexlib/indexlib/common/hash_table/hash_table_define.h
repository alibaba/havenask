#ifndef __INDEXLIB_HASH_TABLE_DEFINE_H
#define __INDEXLIB_HASH_TABLE_DEFINE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(common);

enum HashTableType
{
    HTT_DENSE_HASH,
    HTT_CUCKOO_HASH,
    HTT_UNKOWN
};

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_HASH_TABLE_DEFINE_H
