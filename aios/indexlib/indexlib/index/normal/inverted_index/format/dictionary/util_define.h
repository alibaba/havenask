#ifndef __INDEXLIB_UTIL_DEFINE_H
#define __INDEXLIB_UTIL_DEFINE_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

IE_NAMESPACE_BEGIN(index);

static const uint32_t DICTIONARY_MAGIC_NUMBER = 0x98765432;
static const uint32_t ITEM_COUNT_PER_BLOCK = 128;

template <typename KeyType>
struct DictionaryItemTyped
{
    KeyType first;
    dictvalue_t second;
}__attribute__((packed));

template <typename KeyType>
struct HashDictionaryItemTyped
{
    KeyType key = 0;
    uint32_t next = std::numeric_limits<uint32_t>::max();
    dictvalue_t value = 0;
}__attribute__((packed));
    
IE_NAMESPACE_END(index);

#endif //__INDEXLIB_UTIL_DEFINE_H
