#ifndef __INDEXLIB_ATOMIC_QUERY_H
#define __INDEXLIB_ATOMIC_QUERY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/test/query.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"

IE_NAMESPACE_BEGIN(test);

class AtomicQuery : public Query
{
public:
    AtomicQuery();
    ~AtomicQuery();
public:
    bool Init(index::PostingIterator* iter, std::string indexName)
    { 
        mIndexName = indexName;
        mIter = iter; 
        return true; 
    }

    virtual docid_t Seek(docid_t docid);
    virtual pos_t SeekPosition(pos_t pos);

    std::string GetIndexName() const { return mIndexName; }

private:
    index::PostingIterator* mIter;
    std::string mIndexName;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AtomicQuery);

IE_NAMESPACE_END(test);

#endif //__INDEXLIB_ATOMIC_QUERY_H
