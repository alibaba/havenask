#ifndef __INDEXLIB_POSTING_DECODER_H
#define __INDEXLIB_POSTING_DECODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"

IE_NAMESPACE_BEGIN(index);

class PostingDecoder
{
public:
    PostingDecoder() {}
    virtual ~PostingDecoder() {}

public:
    const index::TermMeta *GetTermMeta() const { return mTermMeta; }

protected:
    index::TermMeta *mTermMeta;
};

DEFINE_SHARED_PTR(PostingDecoder);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTING_DECODER_H
