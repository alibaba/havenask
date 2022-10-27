#include "indexlib/merger/split_strategy/default_split_strategy.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::legacy::json;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, DefaultSplitStrategy);
const std::string DefaultSplitStrategy::STRATEGY_NAME = "default";
DefaultSplitStrategy::DefaultSplitStrategy(SegmentDirectoryBasePtr segDir,
    IndexPartitionSchemaPtr schema, OfflineAttributeSegmentReaderContainerPtr attrReaders,
    const SegmentMergeInfos& segMergeInfos, const MergePlan& plan)
    : SplitSegmentStrategy(segDir, schema, attrReaders, segMergeInfos, plan)
    , mTotalValidDocCount(0)
    , mSplitDocCount(0)
    , mCurrentDocCount(0)
    , mCurrentSegIndex(0)
{
    for (const auto& segMergeInfo : segMergeInfos)
    {
        auto segId = segMergeInfo.segmentId;
        if (plan.HasSegment(segId))
        {
            mTotalValidDocCount
                += segMergeInfo.segmentInfo.docCount - segMergeInfo.deletedDocCount;
        }
    }
}

DefaultSplitStrategy::~DefaultSplitStrategy() {}

bool DefaultSplitStrategy::Init(const KeyValueMap& parameters)
{

    string splitNumStr = GetValueFromKeyValueMap(parameters, "split_num", "1");
    size_t splitNum = 0;
    if (!(StringUtil::fromString(splitNumStr, splitNum)))
    {
        IE_LOG(ERROR, "convert split_num faile: [%s]", splitNumStr.c_str());
        return false;
    }
    mSplitDocCount = mTotalValidDocCount / splitNum;
    return true;
}

segmentindex_t DefaultSplitStrategy::DoProcess(segmentid_t oldSegId, docid_t oldLocalId)
{
    ++mCurrentDocCount;
    if (mCurrentDocCount > mSplitDocCount)
    {
        mCurrentDocCount = 0;
        return ++mCurrentSegIndex;
    }
    return mCurrentSegIndex;
}


IE_NAMESPACE_END(merger);
