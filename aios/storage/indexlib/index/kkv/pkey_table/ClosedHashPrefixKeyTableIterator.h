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

#include <memory>

#include "indexlib/index/kkv/common/OnDiskPKeyOffset.h"
#include "indexlib/index/kkv/pkey_table/ClosedHashPrefixKeyTableTraits.h"
#include "indexlib/index/kkv/pkey_table/NonDeleteValue.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableIteratorTyped.h"

namespace indexlibv2::index {

template <typename _KT, typename _RealVT>
struct ClosedHashTableTraits<_KT, indexlibv2::index::NonDeleteValue<_RealVT>, false> {
    static constexpr bool HasSpecialKey = false;
    static constexpr bool UseCompactBucket = false;
    using _VT = indexlibv2::index::NonDeleteValue<_RealVT>;
    using Bucket = SpecialValueBucket<_KT, _VT>;
};

template <typename ValueType, PKeyTableType Type>
struct ClosedPKeyTableSpecialValueTraits {
    using StoreType = ValueType;
    using TableValueType = NonDeleteValue<StoreType>;
    using Traits = typename ClosedHashPrefixKeyTableTraits<TableValueType, Type>::Traits;
    using TableType = typename Traits::HashTable;
    using IteratorType = typename TableType::Iterator;

    static StoreType ToStoreValue(const ValueType& value) { return value; }
};

template <PKeyTableType Type>
struct ClosedPKeyTableSpecialValueTraits<OnDiskPKeyOffset, Type> {
    using StoreType = uint64_t;
    using TableValueType = SpecialValue<StoreType>;
    using Traits = typename ClosedHashPrefixKeyTableTraits<TableValueType, Type>::Traits;
    using TableType = typename Traits::HashTable;
    using IteratorType = typename TableType::Iterator;

    static StoreType ToStoreValue(const OnDiskPKeyOffset& value) { return value.ToU64Value(); }
};

template <typename ValueType, PKeyTableType Type,
          typename IteratorType = typename ClosedPKeyTableSpecialValueTraits<ValueType, Type>::IteratorType>
class ClosedHashPrefixKeyTableIterator : public PrefixKeyTableIteratorTyped<ValueType, IteratorType>
{
public:
    ClosedHashPrefixKeyTableIterator(IteratorType* iter) : PrefixKeyTableIteratorTyped<ValueType, IteratorType>(iter) {}

    ~ClosedHashPrefixKeyTableIterator() {}

public:
    void Get(uint64_t& key, ValueType& value) const override final
    {
        assert(this->IsValid());
        key = this->_iter->Key();
        using TableValueType = typename ClosedPKeyTableSpecialValueTraits<ValueType, Type>::TableValueType;
        const TableValueType& tableValue = this->_iter->Value();
        value = ValueType(tableValue.Value());
    }
};

} // namespace indexlibv2::index
