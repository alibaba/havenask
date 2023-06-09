/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __INDEXLIB_ADAPTIVE_BITMAP_TRIGGER_H
#define __INDEXLIB_ADAPTIVE_BITMAP_TRIGGER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

class AdaptiveBitmapTrigger
{
public:
    AdaptiveBitmapTrigger() {}
    virtual ~AdaptiveBitmapTrigger() {}

public:
    void Init(const config::IndexConfigPtr& indexConfig) { mIndexConfig = indexConfig; }

    bool NeedGenerateAdaptiveBitmap(const index::DictKeyInfo& key, const PostingMergerPtr& postingMerger);

    virtual bool MatchAdaptiveBitmap(const PostingMergerPtr& postingMerger) = 0;

protected:
    bool IsHighFreqencyTerm(const index::DictKeyInfo& key);

protected:
    config::IndexConfigPtr mIndexConfig;
};

inline bool AdaptiveBitmapTrigger::IsHighFreqencyTerm(const index::DictKeyInfo& key)
{
    assert(mIndexConfig);
    std::shared_ptr<config::HighFrequencyVocabulary> vol = mIndexConfig->GetHighFreqVocabulary();
    if (vol) {
        return vol->Lookup(key);
    }
    return false;
}

inline bool AdaptiveBitmapTrigger::NeedGenerateAdaptiveBitmap(const index::DictKeyInfo& key,
                                                              const PostingMergerPtr& postingMerger)
{
    if (IsHighFreqencyTerm(key)) {
        return false;
    }
    return MatchAdaptiveBitmap(postingMerger);
}

DEFINE_SHARED_PTR(AdaptiveBitmapTrigger);
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_ADAPTIVE_BITMAP_TRIGGER_H
