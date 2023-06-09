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
#include <memory>

#include "indexlib/index/inverted_index/PostingMerger.h"
#include "indexlib/index/inverted_index/builtin_index/adaptive_bitmap/AdaptiveBitmapTrigger.h"
#include "indexlib/util/MathUtil.h"

namespace indexlib::index {

class PercentAdaptiveBitmapTrigger : public AdaptiveBitmapTrigger
{
public:
    PercentAdaptiveBitmapTrigger(uint32_t totalDocCount, int32_t percent)
        : _totalDocCount(totalDocCount)
        , _percent(percent)
    {
        assert(percent >= 0 && percent <= 100);
    }
    ~PercentAdaptiveBitmapTrigger() = default;

    bool MatchAdaptiveBitmap(const std::shared_ptr<PostingMerger>& postingMerger) override
    {
        df_t df = postingMerger->GetDocFreq();
        int32_t threshold = util::MathUtil::MultiplyByPercentage(_totalDocCount, _percent);
        return df >= threshold;
    }

private:
    uint32_t _totalDocCount;
    int32_t _percent;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
