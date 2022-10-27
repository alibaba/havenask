#include "indexlib/merger/merge_strategy/key_value_optimize_merge_strategy.h"

using namespace std;

IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, KeyValueOptimizeMergeStrategy);

KeyValueOptimizeMergeStrategy::KeyValueOptimizeMergeStrategy(
    const merger::SegmentDirectoryPtr& segDir,
    const config::IndexPartitionSchemaPtr& schema)
    : MergeStrategy(segDir, schema)
{
}

KeyValueOptimizeMergeStrategy::~KeyValueOptimizeMergeStrategy()
{
}

void KeyValueOptimizeMergeStrategy::SetParameter(const config::MergeStrategyParameter& param)
{
}

std::string KeyValueOptimizeMergeStrategy::GetParameter() const
{
    return "";
}

MergeTask KeyValueOptimizeMergeStrategy::CreateMergeTask(
        const SegmentMergeInfos& segMergeInfos,
        const LevelInfo& levelInfo)
{
    LevelTopology topology = levelInfo.GetTopology();
    if (topology != topo_hash_mod)
    {
        INDEXLIB_FATAL_ERROR(UnSupported,
                             "kv and kkv table optimize merge strategy only support topo_hash_mod,"
                             " [%s] is unsupported", LevelMeta::TopologyToStr(topology).c_str());
    }
    if (!NeedMerge(segMergeInfos, levelInfo))
    {
        return MergeTask();
    }

    map<segmentid_t, size_t> segMergeInfoMap;
    for (size_t i = 0; i < segMergeInfos.size(); ++i)
    {
        segMergeInfoMap[segMergeInfos[i].segmentId] = i;
    }
    uint32_t columnCount = segMergeInfos[0].segmentInfo.shardingColumnCount;
    vector<MergePlan> mergePlans(columnCount);
    for (const auto& levelMeta : levelInfo.levelMetas)
    {
        if (levelMeta.topology == topo_sequence)
        {
            for (auto segmentId : levelMeta.segments)
            {
                if (segmentId == INVALID_SEGMENTID)
                {
                    continue;
                }
                size_t segIdx = segMergeInfoMap[segmentId];
                for (auto& mergePlan : mergePlans)
                {
                    mergePlan.AddSegment(segMergeInfos[segIdx]);
                }
            }
        }
        else if (levelMeta.topology == topo_hash_mod)
        {
            assert(mergePlans.size() == levelMeta.segments.size());
            for (size_t i = 0; i < levelMeta.segments.size(); ++i)
            {
                segmentid_t segmentId = levelMeta.segments[i];
                if (segmentId == INVALID_SEGMENTID)
                {
                    continue;
                }
                size_t segIdx = segMergeInfoMap[segmentId];
                mergePlans[i].AddSegment(segMergeInfos[segIdx]);
            }
        }
        else
        {
            assert(false);
        }
    }
    MergeTask mergeTask;
    SegmentTopologyInfo segTopoInfo;
    segTopoInfo.levelTopology = topology;
    segTopoInfo.levelIdx = levelInfo.GetLevelCount() - 1;
    segTopoInfo.isBottomLevel = true;
    segTopoInfo.columnCount = columnCount;
    for (size_t i = 0; i < mergePlans.size(); ++i)
    {
        auto& mergePlan = mergePlans[i];
        if (mergePlan.GetSegmentCount() != 0)
        {
            segTopoInfo.columnIdx = i;
            mergePlan.SetTargetTopologyInfo(0, segTopoInfo);
            SegmentInfo& segInfo = mergePlan.GetTargetSegmentInfo(0);
            segInfo.shardingColumnId = i;
            mergeTask.AddPlan(mergePlan);
        }
    }
    return mergeTask;
}

MergeTask KeyValueOptimizeMergeStrategy::CreateMergeTaskForOptimize(
        const SegmentMergeInfos& segMergeInfos, const LevelInfo& levelInfo)
{
    return CreateMergeTask(segMergeInfos, levelInfo);
}

bool KeyValueOptimizeMergeStrategy::NeedMerge(
        const SegmentMergeInfos& segMergeInfos, const LevelInfo& levelInfo) const
{
    for (const auto& segMergeInfo : segMergeInfos)
    {
        if (segMergeInfo.levelIdx != levelInfo.GetLevelCount() - 1)
        {
            return true;
        }
    }
    return false;
}

IE_NAMESPACE_END(merger);

