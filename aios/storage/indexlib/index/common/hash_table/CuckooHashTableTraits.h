/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include "indexlib/index/common/hash_table/ClosedHashTableBufferedFileIterator.h"
#include "indexlib/index/common/hash_table/ClosedHashTableFileIterator.h"
#include "indexlib/index/common/hash_table/CuckooHashTable.h"
#include "indexlib/index/common/hash_table/CuckooHashTableFileReader.h"
#include "indexlib/index/common/hash_table/HashTableDefine.h"
#include "indexlib/index/common/hash_table/HashTableOptions.h"

namespace indexlibv2::index {

template <typename _KT, typename _VT, bool useCompactBucket>
struct CuckooHashTableTraits {
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
    using FileIterator = ClosedHashTableFileIterator<typename HashTable::HashTableHeader, _KT, _VT, HasSpecialKey,
                                                     hasDeleted, useCompactBucket>;
    using BufferedFileIterator = ClosedHashTableBufferedFileIterator<typename HashTable::HashTableHeader, _KT, _VT,
                                                                     HasSpecialKey, useCompactBucket>;
};
} // namespace indexlibv2::index
