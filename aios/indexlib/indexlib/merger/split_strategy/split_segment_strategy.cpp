#include "indexlib/merger/split_strategy/split_segment_strategy.h"

using namespace std;

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, SplitSegmentStrategy);

segmentindex_t SplitSegmentStrategy::Process(segmentid_t oldSegId, docid_t oldLocalId)
{
    segmentindex_t segIdx = DoProcess(oldSegId, oldLocalId);
    auto currentSize = mMetricsUpdaters.size();
    if (segIdx >= currentSize)
    {
        for (size_t i = currentSize; i <= segIdx; ++i)
        {
            mMetricsUpdaters.push_back(mUpdaterGenerator());
        }
    }
    const auto& updater = mMetricsUpdaters[segIdx];
    if (updater)
    {
        updater->UpdateForMerge(oldSegId, oldLocalId);
    }
    return segIdx;
}

std::vector<autil::legacy::json::JsonMap> SplitSegmentStrategy::GetSegmentCustomizeMetrics()
{
    std::vector<autil::legacy::json::JsonMap> jsonMaps;
    if (mMetricsUpdaters.size() == 0)
    {
        const auto& updater = mUpdaterGenerator();
        if (updater)
        {
            return { updater->Dump() };
        }
        else
        {
            return { autil::legacy::json::JsonMap() };
        }
    }
    jsonMaps.reserve(mMetricsUpdaters.size());
    for (const auto& updater : mMetricsUpdaters)
    {
        if (updater)
        {
            jsonMaps.push_back(updater->Dump());
        }
        else
        {
            jsonMaps.push_back(autil::legacy::json::JsonMap());
        }
    }
    return jsonMaps;
}

IE_NAMESPACE_END(merger);

