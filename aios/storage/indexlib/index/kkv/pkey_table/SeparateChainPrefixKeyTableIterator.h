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

#include "indexlib/index/common/hash_table/SeparateChainHashTable.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableIteratorTyped.h"

namespace indexlibv2::index {

template <typename ValueType,
          typename IteratorType = typename SeparateChainHashTable<uint64_t, ValueType>::HashTableIterator>
class SeparateChainPrefixKeyTableIterator : public PrefixKeyTableIteratorTyped<ValueType, IteratorType>
{
public:
    SeparateChainPrefixKeyTableIterator(IteratorType* iter) : PrefixKeyTableIteratorTyped<ValueType, IteratorType>(iter)
    {
    }

    ~SeparateChainPrefixKeyTableIterator() {}

public:
    void Get(uint64_t& key, ValueType& value) const override final
    {
        assert(this->IsValid());
        key = this->_iter->Key();
        value = this->_iter->Value();
    }
};

} // namespace indexlibv2::index
