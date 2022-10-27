#include "indexlib/index/normal/primarykey/primary_key_load_strategy_creator.h"
#include "indexlib/index/normal/primarykey/default_primary_key_load_strategy.h"
#include "indexlib/index/normal/primarykey/combine_segments_primary_key_load_strategy.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PrimaryKeyLoadStrategyCreator);

PrimaryKeyLoadStrategyCreator::PrimaryKeyLoadStrategyCreator() 
{
}

PrimaryKeyLoadStrategyCreator::~PrimaryKeyLoadStrategyCreator() 
{
}

PrimaryKeyLoadStrategyPtr PrimaryKeyLoadStrategyCreator::CreatePrimaryKeyLoadStrategy(
        const config::PrimaryKeyIndexConfigPtr& pkIndexConfig)
{
    PrimaryKeyLoadStrategyParamPtr param = pkIndexConfig->GetPKLoadStrategyParam();
    assert(param);
    if (param->NeedCombineSegments())
    {
        return PrimaryKeyLoadStrategyPtr(
                new CombineSegmentsPrimaryKeyLoadStrategy(pkIndexConfig));
    }
    return PrimaryKeyLoadStrategyPtr(
            new DefaultPrimaryKeyLoadStrategy(pkIndexConfig));
}

IE_NAMESPACE_END(index);

