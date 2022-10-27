#ifndef __INDEXLIB_HASH_TABLE_NODE_H
#define __INDEXLIB_HASH_TABLE_NODE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include <limits>

IE_NAMESPACE_BEGIN(common);

const uint32_t INVALID_HASHTABLE_OFFSET = std::numeric_limits<uint32_t>::max();

#pragma pack(push, 4)
template<typename Key, typename Value>
struct HashTableNode
{
    Key key;
    Value value;
    uint32_t next;

    HashTableNode(const Key& _key = Key(), const Value& _value = Value(),
                  uint32_t _next = INVALID_HASHTABLE_OFFSET)
        : key(_key)
        , value(_value)
        , next(_next)
    { }
};
#pragma pack(pop)

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_HASH_TABLE_NODE_H
