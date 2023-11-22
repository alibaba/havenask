#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/framework/legacy_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/testlib/fake_posting_iterator.h"

namespace indexlib { namespace testlib {

class FakeBitmapIndexReader : public index::LegacyIndexReader
{
public:
    FakeBitmapIndexReader(const std::string& datas);
    ~FakeBitmapIndexReader();

private:
    FakeBitmapIndexReader(const FakeBitmapIndexReader&);
    FakeBitmapIndexReader& operator=(const FakeBitmapIndexReader&);

public:
    virtual index::Result<index::PostingIterator*> Lookup(const index::Term& term, uint32_t statePoolSize = 1000,
                                                          PostingType type = pt_default,
                                                          autil::mem_pool::Pool* sessionPool = nullptr);

    future_lite::coro::Lazy<indexlib::index::Result<indexlib::index::PostingIterator*>>
    LookupAsync(const indexlib::index::Term* term, uint32_t statePoolSize, PostingType type,
                autil::mem_pool::Pool* pool, indexlib::file_system::ReadOption option) noexcept
    {
        co_return Lookup(*term, statePoolSize, type, pool);
    }

    virtual const index::SectionAttributeReader* GetSectionReader(const std::string& indexName) const
    {
        assert(false);
        return nullptr;
    }
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

public:
    FakePostingIterator::Map _map;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeBitmapIndexReader);
}} // namespace indexlib::testlib
