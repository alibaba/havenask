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

#include "autil/SnapshotVector.h"
#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"
#include "indexlib/util/HashMap.h"

namespace indexlib::index {

class InMemDictionaryReader : public DictionaryReader
{
public:
    using HashKeyVector = autil::SnapshotVector<dictkey_t, autil::mem_pool::pool_allocator<dictkey_t>>;

    InMemDictionaryReader(HashKeyVector* hashKeyVectorPtr);
    ~InMemDictionaryReader() = default;

    InMemDictionaryReader(const InMemDictionaryReader&) = delete;
    InMemDictionaryReader& operator=(const InMemDictionaryReader&) = delete;
    InMemDictionaryReader(InMemDictionaryReader&&) = delete;
    InMemDictionaryReader& operator=(InMemDictionaryReader&&) = delete;

    Status Open(const std::shared_ptr<file_system::Directory>& directory, const std::string& fileName,
                bool supportFileCompress) override
    {
        assert(false);
        RETURN_IF_STATUS_ERROR(Status::InvalidArgs(), "InMemDictionaryReader does not support open");
        return Status::OK();
    }

    std::shared_ptr<DictionaryIterator> CreateIterator() const override;
    index::Result<bool> InnerLookup(dictkey_t key, indexlib::file_system::ReadOption option,
                                    dictvalue_t& value) noexcept override
    {
        assert(false);
        return false;
    }

private:
    HashKeyVector* _hashKeyVectorPtr;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
