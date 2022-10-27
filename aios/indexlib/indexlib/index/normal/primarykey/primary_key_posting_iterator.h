#ifndef __INDEXLIB_PRIMARY_KEY_POSTING_ITERATOR_H
#define __INDEXLIB_PRIMARY_KEY_POSTING_ITERATOR_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"

IE_NAMESPACE_BEGIN(index);

class PrimaryKeyPostingIterator : public PostingIterator
{
public:
    PrimaryKeyPostingIterator(docid_t docId, autil::mem_pool::Pool *sessionPool = NULL);
    ~PrimaryKeyPostingIterator();
public:
    index::TermMeta *GetTermMeta() const override;
    docid_t SeekDoc(docid_t docId) override;
    common::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override {
        result = SeekDoc(docId);
        return common::ErrorCode::OK;
    }
    void Unpack(TermMatchData& termMatchData) override;
    PostingIteratorType GetType() const override  { return pi_pk; }

    bool HasPosition() const override { return false;}

    PostingIterator* Clone() const override;
    void Reset() override; 
    autil::mem_pool::Pool *GetSessionPool() const override {
        return mSessionPool;
    }
private:
    docid_t mDocId;
    bool mIsSearched;
    autil::mem_pool::Pool *mSessionPool;
    index::TermMeta *mTermMeta;
private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<PrimaryKeyPostingIterator> PrimaryKeyPostingIteratorPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_POSTING_ITERATOR_H
