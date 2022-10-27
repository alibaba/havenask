#ifndef __INDEXLIB_BITMAP_POSTING_MERGE_PARAM_H
#define __INDEXLIB_BITMAP_POSTING_MERGE_PARAM_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/common/byte_slice_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"

IE_NAMESPACE_BEGIN(index);

struct BitmapPostingMergeParam
{
public:
    BitmapPostingMergeParam(docid_t oldBaseId, docid_t newBaseId, 
                         const index::TermMeta *tm, 
                         const util::ByteSliceList* bsl)
        : oldBaseDocId(oldBaseId)
        , newBaseDocId(newBaseId)
        , termMeta(tm)
        , postList(bsl)
    {
    }

    ~BitmapPostingMergeParam() 
    {
    }

public:
    docid_t oldBaseDocId;
    docid_t newBaseDocId;
    const index::TermMeta* termMeta;
    const util::ByteSliceList* postList;
};

typedef std::tr1::shared_ptr<BitmapPostingMergeParam> BitmapPostingMergeParamPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BITMAP_POSTING_MERGE_PARAM_H
