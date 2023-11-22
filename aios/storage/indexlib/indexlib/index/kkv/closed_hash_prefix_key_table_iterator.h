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

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/prefix_key_table_iterator_typed.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename ValueType, PKeyTableType Type>
struct ClosedPKeyTableSpecialValueTraits {
    typedef ValueType StoreType;
    typedef common::NonDeleteValue<StoreType> TableValueType;

    typedef typename ClosedHashPrefixKeyTableTraits<TableValueType, Type>::Traits Traits;
    typedef typename Traits::HashTable TableType;
    typedef typename TableType::Iterator IteratorType;

    static StoreType ToStoreValue(const ValueType& value) { return value; }
};

template <PKeyTableType Type>
struct ClosedPKeyTableSpecialValueTraits<OnDiskPKeyOffset, Type> {
    typedef uint64_t StoreType;
    typedef common::SpecialValue<StoreType> TableValueType;
    typedef typename ClosedHashPrefixKeyTableTraits<TableValueType, Type>::Traits Traits;
    typedef typename Traits::HashTable TableType;
    typedef typename TableType::Iterator IteratorType;

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
        key = this->mIter->Key();
        typedef typename ClosedPKeyTableSpecialValueTraits<ValueType, Type>::TableValueType TableValueType;
        const TableValueType& tableValue = this->mIter->Value();
        value = ValueType(tableValue.Value());
    }
};
}} // namespace indexlib::index
