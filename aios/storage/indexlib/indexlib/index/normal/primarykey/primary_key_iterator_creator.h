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
#ifndef __INDEXLIB_PRIMARY_KEY_ITERATOR_CREATOR_H
#define __INDEXLIB_PRIMARY_KEY_ITERATOR_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/index/normal/primarykey/hash_primary_key_iterator.h"
#include "indexlib/index/normal/primarykey/primary_key_iterator.h"
#include "indexlib/index/normal/primarykey/sequential_primary_key_iterator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class PrimaryKeyIteratorCreator
{
public:
    PrimaryKeyIteratorCreator() {}
    ~PrimaryKeyIteratorCreator() {}

public:
    template <typename Key>
    static std::shared_ptr<PrimaryKeyIterator<Key>> Create(const config::PrimaryKeyIndexConfigPtr& indexConfig);
};

//////////////////////////////////////////////////////////////////////
template <typename Key>
std::shared_ptr<PrimaryKeyIterator<Key>>
PrimaryKeyIteratorCreator::Create(const config::PrimaryKeyIndexConfigPtr& indexConfig)
{
    typedef std::shared_ptr<PrimaryKeyIterator<Key>> PrimaryKeyIteratorPtr;
    // TODO : check load mode
    switch (indexConfig->GetPrimaryKeyIndexType()) {
    case pk_hash_table:
        return PrimaryKeyIteratorPtr(new HashPrimaryKeyIterator<Key>(indexConfig));
    case pk_sort_array:
    case pk_block_array:
        return PrimaryKeyIteratorPtr(new SequentialPrimaryKeyIterator<Key>(indexConfig));
    default:
        assert(false);
    }
    return PrimaryKeyIteratorPtr();
}
}} // namespace indexlib::index

#endif //__INDEXLIB_PRIMARY_KEY_ITERATOR_CREATOR_H
