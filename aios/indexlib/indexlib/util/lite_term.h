#ifndef __INDEXLIB_LITE_TERM_H
#define __INDEXLIB_LITE_TERM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

class LiteTerm
{
public:
    LiteTerm()
        : mIndexId(INVALID_INDEXID)
        , mTruncateIndexId(INVALID_INDEXID)
        , mTermHashKey(INVALID_DICT_VALUE)
    {}
    
    LiteTerm(dictkey_t termHashKey, indexid_t indexId, 
             indexid_t truncIndexId = INVALID_INDEXID)
        : mIndexId(indexId)
        , mTruncateIndexId(truncIndexId)
        , mTermHashKey(termHashKey)
    {}
    
    ~LiteTerm() {}
    
public:
    dictkey_t GetTermHashKey() const { return mTermHashKey; }
    indexid_t GetIndexId() const { return mIndexId; }
    indexid_t GetTruncateIndexId() const { return mTruncateIndexId; }

    void SetTermHashKey(dictkey_t termHashKey) { mTermHashKey = termHashKey; }
    void SetIndexId(indexid_t indexId) { mIndexId = indexId; }
    void SetTruncateIndexId(indexid_t truncIndexId) { mTruncateIndexId = truncIndexId; }

    bool operator == (const LiteTerm& term) const {
        return mIndexId == term.mIndexId &&
            mTruncateIndexId == term.mTruncateIndexId &&
            mTermHashKey == term.mTermHashKey;
    }
    
    bool operator != (const LiteTerm& term) const { return !(*this == term); }
    
private:
    indexid_t mIndexId;
    indexid_t mTruncateIndexId;
    dictkey_t mTermHashKey;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LiteTerm);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_LITE_TERM_H
