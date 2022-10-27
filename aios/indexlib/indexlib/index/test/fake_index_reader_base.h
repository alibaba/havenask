#ifndef __INDEXLIB_FAKE_INDEX_READER_BASE_H
#define __INDEXLIB_FAKE_INDEX_READER_BASE_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"

IE_NAMESPACE_BEGIN(index);

class FakeIndexReaderBase: public IndexReader
{
public:
    FakeIndexReaderBase() {}
    ~FakeIndexReaderBase() {}
public:
    PostingIterator *Lookup(const common::Term& term,
                            uint32_t statePoolSize = 1000,
                            PostingType type = pt_default,
                            autil::mem_pool::Pool *sessionPool = NULL) override
    { return NULL; }

    const SectionAttributeReader* GetSectionReader(
            const std::string& indexName) const override
    { return NULL; }
    
    IndexReader* Clone() const override
    { return NULL; }
    std::string GetIdentifier() const
    { return "fake_index_reader_base"; }
    std::string GetIndexName() const 
    { return ""; }

    void SetIndexConfig(const config::IndexConfigPtr& indexConfig) {}

    KeyIteratorPtr CreateKeyIterator(const std::string& indexName) override
    {
        return KeyIteratorPtr();
    }
protected:
    PostingIteratorPtr LookupStorageKey(const std::string& hashKey,
            uint32_t statePoolSize = 1000) 
    { return PostingIteratorPtr(); }
};

typedef std::tr1::shared_ptr<FakeIndexReaderBase> FakeIndexReaderBasePtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_FAKE_INDEX_READER_BASE_H
