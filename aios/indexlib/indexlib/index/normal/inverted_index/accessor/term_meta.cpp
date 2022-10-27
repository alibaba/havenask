#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TermMeta);

TermMeta& TermMeta::operator = (const TermMeta& termMeta)
{
    mDocFreq = termMeta.mDocFreq;
    mTotalTermFreq = termMeta.mTotalTermFreq;
    mPayload = termMeta.mPayload;
    return (*this);
}

bool TermMeta::operator == (const TermMeta& termMeta) const
{
    return (mDocFreq == termMeta.mDocFreq) 
        && (mTotalTermFreq == termMeta.mTotalTermFreq)
        && (mPayload == termMeta.mPayload);
}

IE_NAMESPACE_END(index);

