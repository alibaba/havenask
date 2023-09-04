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

#include "indexlib/index/attribute/AttributeIteratorBase.h"
#include "indexlib/index/attribute/AttributeIteratorTyped.h"

namespace indexlibv2::index {

class PackAttributeIterator : public AttributeIteratorBase
{
public:
    PackAttributeIterator(AttributeIteratorTyped<autil::MultiChar>* iterator, autil::mem_pool::Pool* pool)
        : AttributeIteratorBase(pool)
        , _iterator(iterator)
    {
    }
    ~PackAttributeIterator()
    {
        POOL_COMPATIBLE_DELETE_CLASS(_pool, _iterator);
        _iterator = nullptr;
    }

public:
    void Reset() override
    {
        if (_iterator) {
            _iterator->Reset();
        }
    }
    bool Seek(docid_t docId, std::string& attrValue) noexcept override
    {
        // interface for export doc feature only, so we never got here.
        assert(false);
        return _iterator->Seek(docId, attrValue);
    }
    inline const char* GetBaseAddress(docid_t docId) __ALWAYS_INLINE
    {
        assert(_iterator);
        autil::MultiChar multiChar;
        if (!_iterator->Seek(docId, multiChar)) {
            return nullptr;
        }
        return multiChar.data();
    }
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> BatchSeek(const std::vector<docid_t>& docIds,
                                                                     indexlib::file_system::ReadOption readOption,
                                                                     std::vector<std::string>* values) noexcept override
    {
        assert(false);
        co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::Runtime);
    }

private:
    AttributeIteratorTyped<autil::MultiChar>* _iterator;
};

} // namespace indexlibv2::index
