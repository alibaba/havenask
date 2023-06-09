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
#include "indexlib/index/normal/adaptive_bitmap/percent_adaptive_bitmap_trigger.h"

#include "indexlib/util/MathUtil.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, PercentAdaptiveBitmapTrigger);

PercentAdaptiveBitmapTrigger::PercentAdaptiveBitmapTrigger(uint32_t totalDocCount, int32_t percent)
    : mTotalDocCount(totalDocCount)
    , mPercent(percent)
{
    assert(percent >= 0 && percent <= 100);
}

PercentAdaptiveBitmapTrigger::~PercentAdaptiveBitmapTrigger() {}

bool PercentAdaptiveBitmapTrigger::MatchAdaptiveBitmap(const PostingMergerPtr& postingMerger)
{
    df_t df = postingMerger->GetDocFreq();
    int32_t threshold = MathUtil::MultiplyByPercentage(mTotalDocCount, mPercent);
    return df >= threshold;
}
}}} // namespace indexlib::index::legacy
