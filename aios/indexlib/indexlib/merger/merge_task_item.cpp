#include <autil/StringUtil.h>
#include "indexlib/merger/merge_task_item.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, MergeTaskItem);

const std::string MergeTaskItem::MERGE_TASK_ITEMS_FILE_NAME = "merge_task_items";

MergeTaskItem::MergeTaskItem(uint32_t mergePlanIdx, const std::string& mergeType,
    const std::string& name, bool isSubItem, int64_t targetSegIdx)
    : mMergePlanIdx(mergePlanIdx)
    , mMergeType(mergeType)
    , mName(name)
    , mIsSubItem(isSubItem)
    , mCost(0.0f)
    , mTargetSegmentIdx(targetSegIdx)
{
}

MergeTaskItem::~MergeTaskItem() 
{
}

void MergeTaskItem::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("MergePlanId", mMergePlanIdx);
    json.Jsonize("MergeType", mMergeType);
    json.Jsonize("Name", mName);
    json.Jsonize("IsSubItem", mIsSubItem);
    json.Jsonize("Cost", mCost);
    if (json.GetMode() == TO_JSON)
    {
        if (mParallelMergeItem.GetId() != ParallelMergeItem::INVALID_PARALLEL_MERGE_ITEM_ID)
        {
            json.Jsonize("ParallelMergeItem", mParallelMergeItem);
        }
        if (mTargetSegmentIdx != -1)
        {
            json.Jsonize("TargetSegmentIdx", mTargetSegmentIdx);
        }
    }
    else
    {
        JsonMap jsonMap = json.GetMap();
        JsonMap::iterator iter = jsonMap.find("ParallelMergeItem");
        if (iter != jsonMap.end())
        {
            json.Jsonize("ParallelMergeItem", mParallelMergeItem);
        }
        iter = jsonMap.find("TargetSegmentIdx");
        if (iter != jsonMap.end())
        {
            json.Jsonize("TargetSegmentIdx", mTargetSegmentIdx);
        }        
    }
}

std::string MergeTaskItem::ToString() const
{
    string str = "MergePlan[" + autil::StringUtil::toString(mMergePlanIdx) + "] ";
    return str + mMergeType + " [" + mName + GetParallelMergeSuffixString(mParallelMergeItem)
        + "] " + "TargetSegmentIdx[" + GetTargetSegmentString() + "]";
}

std::string MergeTaskItem::GetCheckPointName() const
{
    string subInfo = mIsSubItem ? "_sub" : "";
    string str = "MergePlan_" + autil::StringUtil::toString(mMergePlanIdx) + "_";
    return str + mMergeType + "_" + mName + GetParallelMergeSuffixString(mParallelMergeItem) + "_"
        + GetTargetSegmentString() + subInfo + ".checkpoint";
}

void MergeTaskItem::SetParallelMergeItem(ParallelMergeItem parallelMergeItem)
{
    mParallelMergeItem = parallelMergeItem;
}

ParallelMergeItem MergeTaskItem::GetParallelMergeItem() const
{
    return mParallelMergeItem;
}

string MergeTaskItem::GetParallelMergeSuffixString(
        const ParallelMergeItem& parallelMergeItem)
{
    if (parallelMergeItem.GetTotalParallelCount() <= 0 || parallelMergeItem.GetId() < 0)
    {
        IE_LOG(WARN, "invalid parallelMergeItem [%s], maybe read from legacy mergeMeta",
               parallelMergeItem.ToString().c_str());
        return string("");
    }

    if (parallelMergeItem.GetTotalParallelCount() == 1)
    {
        return string("");
    }
    return string("-") + StringUtil::toString(parallelMergeItem.GetTotalParallelCount()) +
        "-" + StringUtil::toString(parallelMergeItem.GetId());
}

string MergeTaskItem::GetTargetSegmentString() const { return std::to_string(mTargetSegmentIdx); }

IE_NAMESPACE_END(merger);

