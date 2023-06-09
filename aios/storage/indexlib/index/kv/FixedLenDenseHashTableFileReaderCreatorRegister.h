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

#include "indexlib/base/FieldType.h"
#include "indexlib/index/common/hash_table/ClosedHashTableBufferedFileIterator.h"
#include "indexlib/index/common/hash_table/ClosedHashTableTraits.h"
#include "indexlib/index/common/hash_table/DenseHashTableFileReader.h"
#include "indexlib/index/common/hash_table/HashTableBase.h"
#include "indexlib/index/kv/FixedLenDenseHashTableFileReaderCreator.h"

namespace indexlibv2::index {

template <typename KeyType, typename ValueType, bool useCompactBucket>
std::unique_ptr<HashTableAccessor>
FixedLenDenseHashTableFileReaderCreator<KeyType, ValueType, useCompactBucket>::CreateHashTable() noexcept
{
    static constexpr bool HasSpecialKey = ClosedHashTableTraits<KeyType, ValueType, useCompactBucket>::HasSpecialKey;
    using HashTableType = DenseHashTableFileReader<KeyType, ValueType, HasSpecialKey, useCompactBucket>;
    return std::make_unique<HashTableType>();
}

template <typename KeyType, typename ValueType, bool useCompactBucket>
std::unique_ptr<HashTableFileIterator>
FixedLenDenseHashTableFileReaderCreator<KeyType, ValueType, useCompactBucket>::CreateHashTableFileIterator() noexcept
{
    static constexpr bool HasSpecialKey = ClosedHashTableTraits<KeyType, ValueType, useCompactBucket>::HasSpecialKey;
    using HashTableType = DenseHashTableFileReader<KeyType, ValueType, HasSpecialKey, useCompactBucket>;
    using IteratorType = ClosedHashTableBufferedFileIterator<typename HashTableType::HashTableHeader, KeyType,
                                                             ValueType, HasSpecialKey, useCompactBucket>;
    return std::make_unique<IteratorType>();
}

} // namespace indexlibv2::index

#define INDEXLIB_KV_HASHTABLE_INSTANTIATION_VALUETYPE(ValueType)                                                       \
    template class indexlibv2::index::FixedLenDenseHashTableFileReaderCreator<uint32_t, ValueType, true>;              \
    template class indexlibv2::index::FixedLenDenseHashTableFileReaderCreator<uint32_t, ValueType, false>;             \
    template class indexlibv2::index::FixedLenDenseHashTableFileReaderCreator<uint64_t, ValueType, true>;              \
    template class indexlibv2::index::FixedLenDenseHashTableFileReaderCreator<uint64_t, ValueType, false>;
