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
#pragma once

#include "autil/Log.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"

namespace indexlib::config {
class HighFrequencyVocabulary;
}

namespace inedxlib::util {
class DictKeyInfo;
}

namespace indexlib::index {

class PostingMerger;

class AdaptiveBitmapTrigger
{
public:
    AdaptiveBitmapTrigger() = default;
    virtual ~AdaptiveBitmapTrigger() = default;

    void Init(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig)
    {
        _indexConfig = indexConfig;
    }

    bool NeedGenerateAdaptiveBitmap(const index::DictKeyInfo& key, const std::shared_ptr<PostingMerger>& postingMerger);

    virtual bool MatchAdaptiveBitmap(const std::shared_ptr<PostingMerger>& postingMerger) = 0;

protected:
    bool IsHighFreqencyTerm(const index::DictKeyInfo& key);

    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;

private:
    AUTIL_LOG_DECLARE();
};

inline bool AdaptiveBitmapTrigger::IsHighFreqencyTerm(const index::DictKeyInfo& key)
{
    assert(_indexConfig);
    std::shared_ptr<config::HighFrequencyVocabulary> vol = _indexConfig->GetHighFreqVocabulary();
    if (vol) {
        return vol->Lookup(key);
    }
    return false;
}

inline bool AdaptiveBitmapTrigger::NeedGenerateAdaptiveBitmap(const index::DictKeyInfo& key,
                                                              const std::shared_ptr<PostingMerger>& postingMerger)
{
    if (IsHighFreqencyTerm(key)) {
        return false;
    }
    return MatchAdaptiveBitmap(postingMerger);
}

} // namespace indexlib::index
