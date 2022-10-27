#include "indexlib/index/normal/inverted_index/truncate/truncate_trigger_creator.h"
#include "indexlib/index/normal/inverted_index/truncate/default_truncate_trigger.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_meta_trigger.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/truncate_index_property.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TruncateTriggerCreator);

TruncateTriggerCreator::TruncateTriggerCreator() 
{
}

TruncateTriggerCreator::~TruncateTriggerCreator() 
{
}

TruncateTriggerPtr TruncateTriggerCreator::Create(
        const TruncateIndexProperty& truncateIndexProperty)
{
    const string &strategyType = truncateIndexProperty.GetStrategyType();
    uint64_t threshold = truncateIndexProperty.GetThreshold();
    TruncateTriggerPtr truncStrategy;
    if (strategyType == TRUNCATE_META_STRATEGY_TYPE)
    {
        truncStrategy.reset(new TruncateMetaTrigger(threshold));
    }
    else 
    {
        truncStrategy.reset(new DefaultTruncateTrigger(threshold));
    }
    return truncStrategy;
}

IE_NAMESPACE_END(index);

