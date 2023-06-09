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
#ifndef __INDEXLIB_SEPARATE_CHAIN_PREFIX_KEY_TABLE_ITERATOR_H
#define __INDEXLIB_SEPARATE_CHAIN_PREFIX_KEY_TABLE_ITERATOR_H

#include <memory>

#include "indexlib/common/hash_table/separate_chain_hash_table.h"
#include "indexlib/common_define.h"
#include "indexlib/index/kkv/prefix_key_table_iterator_typed.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename ValueType,
          typename IteratorType = typename common::SeparateChainHashTable<uint64_t, ValueType>::HashTableIterator>
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
        key = this->mIter->Key();
        value = this->mIter->Value();
    }

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index

#endif //__INDEXLIB_SEPARATE_CHAIN_PREFIX_KEY_TABLE_ITERATOR_H
