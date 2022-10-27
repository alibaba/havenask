#ifndef __INDEXLIB_POSTING_ITERATOR_IMPL_H
#define __INDEXLIB_POSTING_ITERATOR_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"
#include "indexlib/util/object_pool.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"

IE_NAMESPACE_BEGIN(index);

class PostingIteratorImpl : public PostingIterator
{
public:
    PostingIteratorImpl(autil::mem_pool::Pool *sessionPool);

    virtual ~PostingIteratorImpl();

private:
    PostingIteratorImpl(const PostingIteratorImpl& other);

public:
    bool Init(const SegmentPostingVectorPtr& segPostings);
public:
    index::TermMeta *GetTermMeta() const override;
    index::TermMeta *GetTruncateTermMeta() const override;
    index::TermPostingInfo GetTermPostingInfo() const override;
    autil::mem_pool::Pool *GetSessionPool() const override;
    bool operator == (const PostingIteratorImpl& right) const;

protected:
    void InitTermMeta();
protected:
    autil::mem_pool::Pool *mSessionPool;
    index::TermMeta mTermMeta;
    index::TermMeta mCurrentChainTermMeta;
    SegmentPostingVectorPtr mSegmentPostings;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PostingIteratorImpl);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTING_ITERATOR_IMPL_H
