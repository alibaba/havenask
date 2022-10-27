#ifndef __INDEXLIB_MULTI_SEGMENT_TERM_META_CALCULATOR_H
#define __INDEXLIB_MULTI_SEGMENT_TERM_META_CALCULATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"

IE_NAMESPACE_BEGIN(index);

class MultiSegmentTermMetaCalculator
{
public:
    MultiSegmentTermMetaCalculator()
        : mDocFreq(0)
        , mTotalTF(0)
        , mPayload(0)
    {}

public:
    void AddSegment(const index::TermMeta& tm)
    {
        mDocFreq += tm.GetDocFreq();
        mTotalTF += tm.GetTotalTermFreq();
        mPayload = tm.GetPayload();
    }

    void InitTermMeta(index::TermMeta& tm) const
    {
        tm.SetDocFreq(mDocFreq);
        tm.SetTotalTermFreq(mTotalTF);
        tm.SetPayload(mPayload);
    }

private:
    df_t mDocFreq;
    tf_t mTotalTF;
    termpayload_t mPayload;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiSegmentTermMetaCalculator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTI_SEGMENT_TERM_META_CALCULATOR_H
