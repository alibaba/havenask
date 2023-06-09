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
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_trigger_creator.h"

#include "indexlib/config/index_config.h"
#include "indexlib/index/inverted_index/config/AdaptiveDictionaryConfig.h"
#include "indexlib/index/inverted_index/config/HighFreqVocabularyCreator.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_trigger.h"
#include "indexlib/index/normal/adaptive_bitmap/df_adaptive_bitmap_trigger.h"
#include "indexlib/index/normal/adaptive_bitmap/index_size_adaptive_bitmap_trigger.h"
#include "indexlib/index/normal/adaptive_bitmap/percent_adaptive_bitmap_trigger.h"
#include "indexlib/index/util/reclaim_map.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, AdaptiveBitmapTriggerCreator);

AdaptiveBitmapTriggerCreator::AdaptiveBitmapTriggerCreator(const ReclaimMapPtr& reclaimMap) : mReclaimMap(reclaimMap)
{
    assert(mReclaimMap);
}

AdaptiveBitmapTriggerCreator::~AdaptiveBitmapTriggerCreator() {}

AdaptiveBitmapTriggerPtr AdaptiveBitmapTriggerCreator::Create(const IndexConfigPtr& indexConf)
{
    assert(indexConf);
    std::shared_ptr<HighFrequencyVocabulary> vol = HighFreqVocabularyCreator::CreateVocabulary(
        indexConf->GetIndexName(), indexConf->GetInvertedIndexType(), indexConf->GetDictConfig(),
        indexConf->GetNullTermLiteralString(), indexConf->GetDictHashParams());
    indexConf->SetHighFreqVocabulary(vol);

    std::shared_ptr<AdaptiveDictionaryConfig> adaptiveConfig = indexConf->GetAdaptiveDictionaryConfig();
    if (!adaptiveConfig) {
        // check no adaptive dictionary
        return AdaptiveBitmapTriggerPtr();
    }

    AdaptiveDictionaryConfig::DictType type = adaptiveConfig->GetDictType();

    uint32_t totalDocCount = mReclaimMap->GetNewDocCount();
    int32_t adaptiveThreshold = adaptiveConfig->GetThreshold();
    AdaptiveBitmapTriggerPtr trigger;
    switch (type) {
    case AdaptiveDictionaryConfig::DF_ADAPTIVE:
        trigger.reset(new DfAdaptiveBitmapTrigger(adaptiveThreshold));
        break;

    case AdaptiveDictionaryConfig::PERCENT_ADAPTIVE:
        trigger.reset(new PercentAdaptiveBitmapTrigger(totalDocCount, adaptiveThreshold));
        break;

    case AdaptiveDictionaryConfig::SIZE_ADAPTIVE:
        trigger.reset(new IndexSizeAdaptiveBitmapTrigger(totalDocCount));
        break;

    default:
        IE_LOG(ERROR, "unsupported DictType [%d]", type);
    }

    if (trigger) {
        trigger->Init(indexConf);
    }
    return trigger;
}
}}} // namespace indexlib::index::legacy
