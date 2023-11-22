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
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_base.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class PackAttributeIterator : public AttributeIteratorBase
{
public:
    PackAttributeIterator(AttributeIteratorTyped<autil::MultiChar>* iterator, autil::mem_pool::Pool* pool)
        : AttributeIteratorBase(pool)
        , mIterator(iterator)
    {
    }

    ~PackAttributeIterator()
    {
        POOL_COMPATIBLE_DELETE_CLASS(mPool, mIterator);
        mIterator = NULL;
    }

public:
    void Reset() override
    {
        if (mIterator) {
            mIterator->Reset();
        }
    }

    inline const char* GetBaseAddress(docid_t docId) __ALWAYS_INLINE
    {
        assert(mIterator);
        autil::MultiChar multiChar;
        if (!mIterator->Seek(docId, multiChar)) {
            return NULL;
        }
        return multiChar.data();
    }

    future_lite::coro::Lazy<index::ErrorCodeVec> BatchSeek(const std::vector<docid_t>& docIds,
                                                           file_system::ReadOption readOption,
                                                           std::vector<std::string>* values) noexcept override
    {
        assert(false);
        co_return index::ErrorCodeVec(docIds.size(), index::ErrorCode::Runtime);
    }

private:
    AttributeIteratorTyped<autil::MultiChar>* mIterator;
};

DEFINE_SHARED_PTR(PackAttributeIterator);
}} // namespace indexlib::index
