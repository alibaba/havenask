#ifndef __INDEXLIB_FAKE_BITMAP_INDEX_READER_H
#define __INDEXLIB_FAKE_BITMAP_INDEX_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/testlib/fake_posting_iterator.h"

IE_NAMESPACE_BEGIN(testlib);

class FakeBitmapIndexReader : public index::IndexReader
{
public:
    FakeBitmapIndexReader(const std::string &datas);
    ~FakeBitmapIndexReader();
private:
    FakeBitmapIndexReader(const FakeBitmapIndexReader &);
    FakeBitmapIndexReader& operator=(const FakeBitmapIndexReader &);
public:
    virtual index::PostingIterator *Lookup(const common::Term& term,
            uint32_t statePoolSize = 1000,
            PostingType type = pt_default,
            autil::mem_pool::Pool *sessionPool = NULL);
    
    virtual const index::SectionAttributeReader* GetSectionReader(
            const std::string& indexName) const
    {
        assert(false);
        return NULL;
    }
    virtual std::string GetIndexName() const { return ""; }
    virtual index::KeyIteratorPtr CreateKeyIterator(const std::string& indexName)
    { assert(false); return index::KeyIteratorPtr(); }
    virtual index::IndexReader* Clone() const { assert(false); return NULL; }
public:
    FakePostingIterator::Map _map;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeBitmapIndexReader);

IE_NAMESPACE_END(testlib);

#endif //__INDEXLIB_FAKE_BITMAP_INDEX_READER_H
