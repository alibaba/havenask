#ifndef ISEARCH_FAKETOPINDEXREADER_H
#define ISEARCH_FAKETOPINDEXREADER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3_sdk/testlib/index/fake_index_reader_base.h>

IE_NAMESPACE_BEGIN(index);

class FakeTopIndexReader : public FakeIndexReaderBase
{
public:
    FakeTopIndexReader() {}
    ~FakeTopIndexReader() {}

public:
    virtual void SetIndexConfig(const config::IndexConfigPtr& indexConfig) {
        assert(false);
    }

    /**
     * lookup a term in inverted index
     *
     * @param hasPosition true if position-list is needed.
     * @return posting-list iterator or position-list iterator
     *
     */
    virtual PostingIterator *Lookup(const common::Term& term,
                                    uint32_t statePoolSize = 1000,
                                    PostingType type = pt_default,
                                    autil::mem_pool::Pool *sessionPool = NULL)
    {
        assert(false);
        return NULL;
    }

    /*
     * lookup a term in inverted index, and return the total size of all cells
     */
    virtual uint64_t LookupTermIndexSize(const common::Term& term) {
        assert(false);
        return 0;
    }

   /**
     * Get section reader
     */
    virtual const SectionAttributeReader* GetSectionReader(
            const std::string& indexName) const {
        assert(false);
        return NULL;
    }

    virtual IndexReader* Clone() const {
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

    virtual bool WarmUpTerm(const std::string& indexName,
                            const std::string& hashKey) {
        assert(false);
        return false;
    }

    virtual bool IsInCache(const common::Term& term) {
        assert(false);
        return false;
    }
    
    virtual KeyIteratorPtr CreateKeyIterator(const std::string& indexName)  {
        assert(false);
        return KeyIteratorPtr();
    }
protected:
    PostingIteratorPtr LookupStorageKey(const std::string& hashKey,
            uint32_t statePoolSize = 1000)
    {
        assert(false); 
        return PostingIteratorPtr();
    }
};

IE_NAMESPACE_END(index);

#endif //ISEARCH_FAKETOPINDEXREADER_H
