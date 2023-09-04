#pragma once

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3_sdk/testlib/index/fake_index_reader_base.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/indexlib.h"

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

class FakeTopIndexReader : public FakeIndexReaderBase {
public:
    FakeTopIndexReader() {}
    ~FakeTopIndexReader() {}

public:
    /**
     * lookup a term in inverted index
     *
     * @param hasPosition true if position-list is needed.
     * @return posting-list iterator or position-list iterator
     *
     */
    virtual index::Result<PostingIterator *> Lookup(const Term &term,
                                                    uint32_t statePoolSize = 1000,
                                                    PostingType type = pt_default,
                                                    autil::mem_pool::Pool *sessionPool = NULL) {
        assert(false);
        return NULL;
    }

    /*
     * lookup a term in inverted index, and return the total size of all cells
     */
    virtual uint64_t LookupTermIndexSize(const Term &term) {
        assert(false);
        return 0;
    }

    /**
     * Get section reader
     */
    virtual const SectionAttributeReader *GetSectionReader(const std::string &indexName) const {
        assert(false);
        return NULL;
    }

    virtual InvertedIndexReader *Clone() const {
        assert(false);
        return NULL;
    }

    /**
     * Get identifier of index reader
     */
    virtual std::string GetIdentifier() const {
        assert(false);
        return "";
    }

    virtual std::string GetIndexName() const {
        assert(false);
        return "";
    }

    virtual bool WarmUpTerm(const std::string &indexName, const std::string &hashKey) {
        assert(false);
        return false;
    }

    virtual bool IsInCache(const Term &term) {
        assert(false);
        return false;
    }

    virtual std::shared_ptr<indexlib::index::KeyIterator>
    CreateKeyIterator(const std::string &indexName) {
        assert(false);
        return nullptr;
    }

protected:
    std::shared_ptr<PostingIterator> LookupStorageKey(const std::string &hashKey,
                                                      uint32_t statePoolSize = 1000) {
        assert(false);
        return std::shared_ptr<PostingIterator>();
    }
};

} // namespace index
} // namespace indexlib
