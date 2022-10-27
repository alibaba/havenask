#ifndef __INDEXLIB_TERM_META_H
#define __INDEXLIB_TERM_META_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

IE_NAMESPACE_BEGIN(index);

class TermMeta
{
public:
    TermMeta()
        : mDocFreq(0)
        , mTotalTermFreq(0)
        , mPayload(0)
    {}

    TermMeta(df_t docFreq, tf_t totalTermFreq, termpayload_t payload = 0)
        : mDocFreq(docFreq)
        , mTotalTermFreq(totalTermFreq)
        , mPayload(payload)
    {}

    TermMeta(const TermMeta& termMeta)
        : mDocFreq(termMeta.mDocFreq)
        , mTotalTermFreq(termMeta.mTotalTermFreq)
        , mPayload(termMeta.mPayload)
    {}

public:
    df_t GetDocFreq() const {return mDocFreq;}
    tf_t GetTotalTermFreq() const {return mTotalTermFreq;}

    termpayload_t GetPayload() const {return mPayload;}
    void SetPayload(termpayload_t payload) { mPayload = payload;}

    void SetDocFreq(df_t docFreq) { mDocFreq = docFreq;}
    void SetTotalTermFreq(tf_t totalTermFreq) { mTotalTermFreq = totalTermFreq;}

    TermMeta& operator = (const TermMeta& payload);
    bool operator == (const TermMeta& payload) const;

    void Reset()
    {
        mDocFreq = 0;
        mTotalTermFreq = 0;
        mPayload = 0;
    }
    
private:
    df_t mDocFreq;
    tf_t mTotalTermFreq;
    termpayload_t mPayload;

private:
    friend class TermMetaTest;
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<TermMeta> TermMetaPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXENGINETERM_META_H
