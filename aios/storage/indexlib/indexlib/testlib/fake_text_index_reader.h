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
#ifndef __INDEXLIB_FAKE_TEXT_INDEX_READER_H
#define __INDEXLIB_FAKE_TEXT_INDEX_READER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/framework/legacy_index_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/testlib/fake_posting_iterator.h"
#include "indexlib/testlib/fake_section_attribute_reader.h"

namespace indexlib { namespace testlib {

class FakeTextIndexReader : public index::LegacyIndexReader
{
public:
    FakeTextIndexReader(const std::string& mpStr);
    FakeTextIndexReader(const std::string& mpStr, const FakePostingIterator::PositionMap& posMap);

public:
    index::Result<index::PostingIterator*> Lookup(const index::Term& term, uint32_t statePoolSize = 1000,
                                                  PostingType type = pt_default,
                                                  autil::mem_pool::Pool* sessionPool = nullptr);
    future_lite::coro::Lazy<indexlib::index::Result<indexlib::index::PostingIterator*>>
    LookupAsync(const indexlib::index::Term* term, uint32_t statePoolSize, PostingType type,
                autil::mem_pool::Pool* pool, indexlib::file_system::ReadOption option) noexcept
    {
        co_return Lookup(*term, statePoolSize, type, pool);
    }

    virtual const index::SectionAttributeReader* GetSectionReader(const std::string& indexName) const;
    virtual std::string GetIndexName() const { return ""; }

    virtual std::shared_ptr<index::KeyIterator> CreateKeyIterator(const std::string& indexName)
    {
        assert(false);
        return std::shared_ptr<index::KeyIterator>();
    }
    virtual index::InvertedIndexReader* Clone() const
    {
        assert(false);
        return nullptr;
    }
    void setType(const std::string& term, index::PostingIteratorType type) { _typeMap[term] = type; }

    void setPostitionMap(const FakePostingIterator::PositionMap& posMap) { _posMap = posMap; }

private:
    FakePostingIterator::Map _map;
    FakePostingIterator::DocSectionMap _docSectionMap;
    FakePostingIterator::IteratorTypeMap _typeMap;
    FakePostingIterator::PositionMap _posMap;
    mutable FakeSectionAttributeReaderPtr _ptr;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeTextIndexReader);
}} // namespace indexlib::testlib

#endif //__INDEXLIB_FAKE_TEXT_INDEX_READER_H
