#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/normal/framework/legacy_index_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class FakeIndexReaderBase : public LegacyIndexReader
{
public:
    FakeIndexReaderBase() {}
    ~FakeIndexReaderBase() {}

public:
    index::Result<PostingIterator*> Lookup(const index::Term& term, uint32_t statePoolSize = 1000,
                                           PostingType type = pt_default,
                                           autil::mem_pool::Pool* sessionPool = nullptr) override
    {
        return nullptr;
    }

    const SectionAttributeReader* GetSectionReader(const std::string& indexName) const override { return nullptr; }

    std::string GetIdentifier() const { return "fake_index_reader_base"; }
    std::string GetIndexName() const { return ""; }

    void SetIndexConfig(const config::IndexConfigPtr& indexConfig) {}

    std::shared_ptr<KeyIterator> CreateKeyIterator(const std::string& indexName) override
    {
        return std::shared_ptr<KeyIterator>();
    }

protected:
    std::shared_ptr<PostingIterator> LookupStorageKey(const std::string& hashKey, uint32_t statePoolSize = 1000)
    {
        return nullptr;
    }
};

typedef std::shared_ptr<FakeIndexReaderBase> FakeIndexReaderBasePtr;
}} // namespace indexlib::index
