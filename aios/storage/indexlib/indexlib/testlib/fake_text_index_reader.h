#pragma once

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
