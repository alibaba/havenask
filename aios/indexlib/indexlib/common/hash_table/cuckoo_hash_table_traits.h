#ifndef __INDEXLIB_CUCKOO_HASH_TABLE_TRAITS_H
#define __INDEXLIB_CUCKOO_HASH_TABLE_TRAITS_H

#include "indexlib/common/hash_table/cuckoo_hash_table.h"
#include "indexlib/common/hash_table/cuckoo_hash_table_file_reader.h"
#include "indexlib/common/hash_table/closed_hash_table_file_iterator.h"
#include "indexlib/common/hash_table/closed_hash_table_buffered_file_iterator.h"
#include "indexlib/common/hash_table/hash_table_options.h"
#include "indexlib/common/hash_table/hash_table_define.h"

IE_NAMESPACE_BEGIN(common);

template<typename _KT, typename _VT, bool useCompactBucket>
struct CuckooHashTableTraits
{
    const static bool HasSpecialKey = ClosedHashTableTraits<_KT, _VT, false>::HasSpecialKey;
    const static bool UseCompactBucket = ClosedHashTableTraits<_KT, _VT, useCompactBucket>::UseCompactBucket;
    const static HashTableType TableType = HTT_CUCKOO_HASH;
    
    typedef _KT KeyType;
    typedef _VT ValueType;
    typedef CuckooHashTable<_KT, _VT, HasSpecialKey, useCompactBucket> HashTable;
    typedef CuckooHashTableFileReader<_KT, _VT, HasSpecialKey, useCompactBucket> FileReader;
    typedef CuckooHashTable<_KT, _VT, HasSpecialKey, true> CompactHashTable;
    typedef CuckooHashTableFileReader<_KT, _VT, HasSpecialKey, true> CompactFileReader;
    typedef CuckooHashTable<_KT, _VT, HasSpecialKey, false> NormalHashTable;
    typedef CuckooHashTableFileReader<_KT, _VT, HasSpecialKey, false> NormalFileReader;
    typedef HashTableOptions Options;
    
    template <bool hasDeleted = true>
    using FileIterator = ClosedHashTableFileIterator<
        typename HashTable::HashTableHeader, _KT, _VT, HasSpecialKey, hasDeleted, useCompactBucket>;
    using BufferedFileIterator = ClosedHashTableBufferedFileIterator<
        typename HashTable::HashTableHeader, _KT, _VT, HasSpecialKey, useCompactBucket>;
};

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_CUCKOO_HASH_TABLE_TRAITS_H
