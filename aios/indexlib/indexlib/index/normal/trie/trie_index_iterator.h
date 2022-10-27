#ifndef __INDEXLIB_TRIE_INDEX_ITERATOR_H
#define __INDEXLIB_TRIE_INDEX_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index);

class TrieIndexIterator
{
public:
    TrieIndexIterator(DoubleArrayTrie::KVPairVector* results)
        : mResults(results)
        , mCursor(0)
    {
        assert(mResults);
    }
    ~TrieIndexIterator()
    {
        //IE_POOL_COMPATIBLE_DELETE_CLASS(mResults->get_allocator()._pool, mResults);
    }

public:
    typedef std::pair<autil::ConstString, docid_t> KVPair;
    typedef std::vector<KVPair, autil::mem_pool::pool_allocator<KVPair> > KVPairVector;

public:
    bool Valid() const { return mCursor < mResults->size(); }
    void Next() 
    {
        if (Valid())
        {
            mCursor++;
        }
    }
    const autil::ConstString& GetKey() const 
    {
        assert(Valid());
        return (*mResults)[mCursor].first;
    }
    docid_t GetDocid() const
    {
        assert(Valid());
        return (*mResults)[mCursor].second;
    }

public:
    //for test
    size_t Size() const { return mResults->size(); }

private:
    DoubleArrayTrie::KVPairVector* mResults;
    size_t mCursor;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TrieIndexIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRIE_INDEX_ITERATOR_H
