#pragma once

#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "indexlib/index/common/Term.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/normal/framework/legacy_index_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace indexlib {
namespace index {
class SectionAttributeReader;
} // namespace index
} // namespace indexlib

namespace indexlib {
namespace index {

class FakeIndexReaderBase : public LegacyIndexReader {
public:
    FakeIndexReaderBase() {}
    ~FakeIndexReaderBase() {}

public:
    index::Result<PostingIterator *> Lookup(const Term &term,
                                            uint32_t statePoolSize = 1000,
                                            PostingType type = pt_default,
                                            autil::mem_pool::Pool *sessionPool = NULL) {
        return NULL;
    }
    future_lite::coro::Lazy<index::Result<PostingIterator *>>
    LookupAsync(const index::Term *term,
                uint32_t statePoolSize,
                PostingType type,
                autil::mem_pool::Pool *pool,
                file_system::ReadOption option) noexcept {
        co_return Lookup(*term, statePoolSize, type, pool);
    }

    const SectionAttributeReader *GetSectionReader(const std::string &indexName) const {
        return NULL;
    }

    InvertedIndexReader *Clone() const {
        return NULL;
    }
    std::string GetIdentifier() const {
        return "fake_index_reader_base";
    }
    std::string GetIndexName() const {
        return "";
    }

    void SetIndexConfig(const config::IndexConfigPtr &indexConfig) {
        _indexConfig = indexConfig;
    }

    std::shared_ptr<indexlib::index::KeyIterator> CreateKeyIterator(const std::string &indexName) {
        return nullptr;
    }

    void AddIndexSegmentReader(
        const std::shared_ptr<indexlib::index::IndexSegmentReader> &indexSegmentReader) {}

protected:
    std::shared_ptr<PostingIterator> LookupStorageKey(const std::string &hashKey,
                                                      uint32_t statePoolSize = 1000) {
        return std::shared_ptr<PostingIterator>();
    }
};

typedef std::shared_ptr<FakeIndexReaderBase> FakeIndexReaderBasePtr;

} // namespace index
} // namespace indexlib
