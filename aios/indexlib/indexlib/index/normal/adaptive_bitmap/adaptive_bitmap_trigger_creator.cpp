#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_trigger_creator.h"
#include "indexlib/index/normal/adaptive_bitmap/df_adaptive_bitmap_trigger.h"
#include "indexlib/index/normal/adaptive_bitmap/percent_adaptive_bitmap_trigger.h"
#include "indexlib/index/normal/adaptive_bitmap/index_size_adaptive_bitmap_trigger.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_trigger.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/config/adaptive_dictionary_config.h"
#include "indexlib/config/high_freq_vocabulary_creator.h"
#include "indexlib/config/index_config.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AdaptiveBitmapTriggerCreator);

AdaptiveBitmapTriggerCreator::AdaptiveBitmapTriggerCreator(
        const ReclaimMapPtr& reclaimMap)
    : mReclaimMap(reclaimMap)
{
    assert(mReclaimMap);
}

AdaptiveBitmapTriggerCreator::~AdaptiveBitmapTriggerCreator() 
{
}

AdaptiveBitmapTriggerPtr AdaptiveBitmapTriggerCreator::Create(
        const IndexConfigPtr& indexConf)
{
    assert(indexConf);
    HighFrequencyVocabularyPtr vol = 
        HighFreqVocabularyCreator::CreateVocabulary(indexConf->GetIndexName(), 
                indexConf->GetIndexType(), indexConf->GetDictConfig());
    indexConf->SetHighFreqVocabulary(vol);

    AdaptiveDictionaryConfigPtr adaptiveConfig = 
        indexConf->GetAdaptiveDictionaryConfig();
    if (!adaptiveConfig)
    {
        // check no adaptive dictionary
        return AdaptiveBitmapTriggerPtr();
    }

    AdaptiveDictionaryConfig::DictType type =
        adaptiveConfig->GetDictType();

    uint32_t totalDocCount = mReclaimMap->GetNewDocCount();
    int32_t adaptiveThreshold = adaptiveConfig->GetThreshold();
    AdaptiveBitmapTriggerPtr trigger;
    switch (type)
    {
    case AdaptiveDictionaryConfig::DF_ADAPTIVE:
        trigger.reset(new DfAdaptiveBitmapTrigger(
                        adaptiveThreshold));
        break;
        
    case AdaptiveDictionaryConfig::PERCENT_ADAPTIVE:
        trigger.reset(new PercentAdaptiveBitmapTrigger(
                        totalDocCount, adaptiveThreshold));
        break;

    case AdaptiveDictionaryConfig::SIZE_ADAPTIVE:
        trigger.reset(new IndexSizeAdaptiveBitmapTrigger(
                        totalDocCount));
        break;

    default:
        IE_LOG(ERROR, "unsupported DictType [%d]", type);
    }

    if (trigger)
    {
        trigger->Init(indexConf);
    }
    return trigger;
}

IE_NAMESPACE_END(index);

