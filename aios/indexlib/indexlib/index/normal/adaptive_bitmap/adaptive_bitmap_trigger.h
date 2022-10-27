#ifndef __INDEXLIB_ADAPTIVE_BITMAP_TRIGGER_H
#define __INDEXLIB_ADAPTIVE_BITMAP_TRIGGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/high_frequency_vocabulary.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger.h"

IE_NAMESPACE_BEGIN(index);

class AdaptiveBitmapTrigger
{
public:
    AdaptiveBitmapTrigger(){}
    virtual ~AdaptiveBitmapTrigger() {}

public:
    void Init(const config::IndexConfigPtr& indexConfig)
    {
        mIndexConfig = indexConfig;
    }

    bool NeedGenerateAdaptiveBitmap(dictkey_t key,
            const PostingMergerPtr& postingMerger);

    virtual bool MatchAdaptiveBitmap(
            const PostingMergerPtr& postingMerger) = 0;

protected:
    bool IsHighFreqencyTerm(dictkey_t key);

protected:
    config::IndexConfigPtr mIndexConfig;
};

inline bool AdaptiveBitmapTrigger::IsHighFreqencyTerm(dictkey_t key)
{
    assert(mIndexConfig);
    config::HighFrequencyVocabularyPtr vol = mIndexConfig->GetHighFreqVocabulary();
    if (vol)
    {
        return vol->Lookup(key);
    }
    return false;
}

inline bool AdaptiveBitmapTrigger::NeedGenerateAdaptiveBitmap(
        dictkey_t key,
        const PostingMergerPtr& postingMerger)
{
    if (IsHighFreqencyTerm(key))
    {
        return false;
    }
    return MatchAdaptiveBitmap(postingMerger);
}

DEFINE_SHARED_PTR(AdaptiveBitmapTrigger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ADAPTIVE_BITMAP_TRIGGER_H
