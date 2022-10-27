#ifndef __INDEXLIB_TRUNCATE_POSTING_ITERATOR_CREATOR_H
#define __INDEXLIB_TRUNCATE_POSTING_ITERATOR_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"
#include "indexlib/index/normal/inverted_index/accessor/buffered_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_iterator.h"

IE_NAMESPACE_BEGIN(index);

class TruncatePostingIteratorCreator
{
public:
    TruncatePostingIteratorCreator();
    ~TruncatePostingIteratorCreator();

public:
    static PostingIteratorPtr Create(
            const config::IndexConfigPtr& indexConfig,
            const SegmentPosting& segPosting,
            bool isBitmap = false);

    static PostingIteratorPtr Create(const PostingIteratorPtr& it);

private:
    static PostingIteratorPtr CreatePostingIterator(
            const config::IndexConfigPtr& indexConfig,
            const SegmentPostingVectorPtr& segPostings, 
            bool isBitmap);

    static PostingIteratorPtr CreateBitmapPostingIterator(
            optionflag_t optionFlag, 
            const SegmentPostingVectorPtr& segPostings);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncatePostingIteratorCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRUNCATE_POSTING_ITERATOR_CREATOR_H
