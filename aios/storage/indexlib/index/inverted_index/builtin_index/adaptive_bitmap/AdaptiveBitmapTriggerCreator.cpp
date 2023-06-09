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
#include "indexlib/index/inverted_index/builtin_index/adaptive_bitmap/AdaptiveBitmapTriggerCreator.h"

#include "indexlib/index/DocMapper.h"
#include "indexlib/index/inverted_index/builtin_index/adaptive_bitmap/AdaptiveBitmapTrigger.h"
#include "indexlib/index/inverted_index/builtin_index/adaptive_bitmap/DfAdaptiveBitmapTrigger.h"
#include "indexlib/index/inverted_index/builtin_index/adaptive_bitmap/IndexSizeAdaptiveBitmapTrigger.h"
#include "indexlib/index/inverted_index/builtin_index/adaptive_bitmap/PercentAdaptiveBitmapTrigger.h"
#include "indexlib/index/inverted_index/config/AdaptiveDictionaryConfig.h"
#include "indexlib/index/inverted_index/config/HighFreqVocabularyCreator.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, AdaptiveBitmapTriggerCreator);

AdaptiveBitmapTriggerCreator::AdaptiveBitmapTriggerCreator(
    const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper)
    : _docMapper(docMapper)
{
    assert(_docMapper);
}

std::shared_ptr<AdaptiveBitmapTrigger>
AdaptiveBitmapTriggerCreator::Create(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConf)
{
    assert(indexConf);
    std::shared_ptr<config::HighFrequencyVocabulary> vol = config::HighFreqVocabularyCreator::CreateVocabulary(
        indexConf->GetIndexName(), indexConf->GetInvertedIndexType(), indexConf->GetDictConfig(),
        indexConf->GetNullTermLiteralString(), indexConf->GetDictHashParams());
    indexConf->SetHighFreqVocabulary(vol);

    std::shared_ptr<config::AdaptiveDictionaryConfig> adaptiveConfig = indexConf->GetAdaptiveDictionaryConfig();
    if (!adaptiveConfig) {
        // check no adaptive dictionary
        return nullptr;
    }

    config::AdaptiveDictionaryConfig::DictType type = adaptiveConfig->GetDictType();

    uint32_t totalDocCount = _docMapper->GetNewDocCount();
    int32_t adaptiveThreshold = adaptiveConfig->GetThreshold();
    std::shared_ptr<AdaptiveBitmapTrigger> trigger;
    switch (type) {
    case config::AdaptiveDictionaryConfig::DF_ADAPTIVE:
        trigger.reset(new DfAdaptiveBitmapTrigger(adaptiveThreshold));
        break;

    case config::AdaptiveDictionaryConfig::PERCENT_ADAPTIVE:
        trigger.reset(new PercentAdaptiveBitmapTrigger(totalDocCount, adaptiveThreshold));
        break;

    case config::AdaptiveDictionaryConfig::SIZE_ADAPTIVE:
        trigger.reset(new IndexSizeAdaptiveBitmapTrigger(totalDocCount));
        break;

    default:
        AUTIL_LOG(ERROR, "unsupported DictType [%d]", type);
    }

    if (trigger) {
        trigger->Init(indexConf);
    }
    return trigger;
}

} // namespace indexlib::index
