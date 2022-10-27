#include "indexlib/merger/split_strategy/split_segment_strategy_factory.h"
#include "indexlib/merger/split_strategy/simple_split_strategy.h"
#include "indexlib/merger/split_strategy/time_series_split_strategy.h"
#include "indexlib/merger/split_strategy/default_split_strategy.h"
#include "indexlib/merger/split_strategy/test_split_strategy.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index/segment_directory_base.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"

using namespace std;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, SplitSegmentStrategyFactory);

const std::string SplitSegmentStrategyFactory::SPLIT_SEGMENT_STRATEGY_FACTORY_SUFFIX
    = "Factory_SplitSegmentStrategy";

SplitSegmentStrategyFactory::SplitSegmentStrategyFactory() {}

void SplitSegmentStrategyFactory::Init(SegmentDirectoryBasePtr segDir,
    IndexPartitionSchemaPtr schema, OfflineAttributeSegmentReaderContainerPtr attrReaders,
    const SegmentMergeInfos& segMergeInfos,
    std::function<std::unique_ptr<index::SegmentMetricsUpdater>()> segAttrUpdaterGenerator)
{
    mSegDir = segDir;
    mSchema = schema;
    mAttrReaders = attrReaders;
    mSegMergeInfos = segMergeInfos;
    mSegAttrUpdaterGenerator = std::move(segAttrUpdaterGenerator);
}

SplitSegmentStrategyFactory::~SplitSegmentStrategyFactory() 
{
}

SplitSegmentStrategyPtr SplitSegmentStrategyFactory::CreateSplitStrategy(
    const string& strategyName, const util::KeyValueMap& parameters, const MergePlan& plan)
{
    if (mSchema->GetTableType() == tt_kv || mSchema->GetTableType() == tt_kkv)
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "kv and kkv table do not support split segment strategy");
    }
    SplitSegmentStrategyPtr ret;
    if (strategyName.empty() || strategyName == SimpleSplitStrategy::STRATEGY_NAME)
    {
        ret.reset(new SimpleSplitStrategy(mSegDir, mSchema, mAttrReaders, mSegMergeInfos, plan));
    }
    else if (strategyName == TestSplitStrategy::STRATEGY_NAME)
    {
        ret.reset(new TestSplitStrategy(mSegDir, mSchema, mAttrReaders, mSegMergeInfos, plan));
    }
    else if (strategyName == TimeSeriesSplitStrategy::STRATEGY_NAME)
    {
        ret.reset(new TimeSeriesSplitStrategy(mSegDir, mSchema, mAttrReaders, mSegMergeInfos, plan));
    }
    else if (strategyName == DefaultSplitStrategy::STRATEGY_NAME)
    {
        ret.reset(new DefaultSplitStrategy(mSegDir, mSchema, mAttrReaders, mSegMergeInfos, plan));
    }
    if (ret)
    {
        if (!ret->Init(parameters))
        {
            IE_LOG(ERROR, "init strategy[%s] failed", strategyName.c_str());
            return SplitSegmentStrategyPtr();
        }
        ret->SetAttrUpdaterGenerator(mSegAttrUpdaterGenerator);
        return ret;
    }
    IE_LOG(ERROR, "strategy[%s] not found", strategyName.c_str());    
    return SplitSegmentStrategyPtr();    
}

IE_NAMESPACE_END(merger);

