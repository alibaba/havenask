#ifndef __INDEXLIB_ATOMIC_QUERY_H
#define __INDEXLIB_ATOMIC_QUERY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/query.h"

namespace indexlib { namespace test {

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
    void SetWord(const std::string& word) { mWord = word; }
    std::string GetWord() { return mWord; }

    std::string GetIndexName() const { return mIndexName; }

private:
    index::PostingIterator* mIter;
    std::string mIndexName;
    std::string mWord;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AtomicQuery);
}} // namespace indexlib::test

#endif //__INDEXLIB_ATOMIC_QUERY_H
