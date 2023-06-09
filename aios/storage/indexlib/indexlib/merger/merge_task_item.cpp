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
#include "indexlib/merger/merge_task_item.h"

#include "autil/StringUtil.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MergeTaskItem);

const std::string MergeTaskItem::MERGE_TASK_ITEMS_FILE_NAME = "merge_task_items";

MergeTaskItem::MergeTaskItem(uint32_t mergePlanIdx, const std::string& mergeType, const std::string& name,
                             bool isSubItem, int64_t targetSegIdx)
    : mMergePlanIdx(mergePlanIdx)
    , mMergeType(mergeType)
    , mName(name)
    , mIsSubItem(isSubItem)
    , mCost(0.0f)
    , mTargetSegmentIdx(targetSegIdx)
{
}

MergeTaskItem::~MergeTaskItem() {}

void MergeTaskItem::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("MergePlanId", mMergePlanIdx);
    json.Jsonize("MergeType", mMergeType);
    json.Jsonize("Name", mName);
    json.Jsonize("IsSubItem", mIsSubItem);
    json.Jsonize("Cost", mCost);
    if (json.GetMode() == TO_JSON) {
        if (mParallelMergeItem.GetId() != ParallelMergeItem::INVALID_PARALLEL_MERGE_ITEM_ID) {
            json.Jsonize("ParallelMergeItem", mParallelMergeItem);
        }
        if (mTargetSegmentIdx != -1) {
            json.Jsonize("TargetSegmentIdx", mTargetSegmentIdx);
        }
    } else {
        JsonMap jsonMap = json.GetMap();
        JsonMap::iterator iter = jsonMap.find("ParallelMergeItem");
        if (iter != jsonMap.end()) {
            json.Jsonize("ParallelMergeItem", mParallelMergeItem);
        }
        iter = jsonMap.find("TargetSegmentIdx");
        if (iter != jsonMap.end()) {
            json.Jsonize("TargetSegmentIdx", mTargetSegmentIdx);
        }
    }
}

std::string MergeTaskItem::ToString() const
{
    string str = "MergePlan[" + autil::StringUtil::toString(mMergePlanIdx) + "] ";
    return str + mMergeType + " [" + mName + GetParallelMergeSuffixString(mParallelMergeItem) + "] " +
           "TargetSegmentIdx[" + GetTargetSegmentString() + "]";
}

std::string MergeTaskItem::GetCheckPointName() const
{
    string subInfo = mIsSubItem ? "_sub" : "";
    string str = "MergePlan_" + autil::StringUtil::toString(mMergePlanIdx) + "_";
    return str + mMergeType + "_" + mName + GetParallelMergeSuffixString(mParallelMergeItem) + "_" +
           GetTargetSegmentString() + subInfo + ".checkpoint";
}

void MergeTaskItem::SetParallelMergeItem(ParallelMergeItem parallelMergeItem)
{
    mParallelMergeItem = parallelMergeItem;
}

ParallelMergeItem MergeTaskItem::GetParallelMergeItem() const { return mParallelMergeItem; }

string MergeTaskItem::GetParallelMergeSuffixString(const ParallelMergeItem& parallelMergeItem)
{
    if (parallelMergeItem.GetTotalParallelCount() <= 0 || parallelMergeItem.GetId() < 0) {
        IE_LOG(WARN, "invalid parallelMergeItem [%s], maybe read from legacy mergeMeta",
               parallelMergeItem.ToString().c_str());
        return string("");
    }

    if (parallelMergeItem.GetTotalParallelCount() == 1) {
        return string("");
    }
    return string("-") + StringUtil::toString(parallelMergeItem.GetTotalParallelCount()) + "-" +
           StringUtil::toString(parallelMergeItem.GetId());
}

string MergeTaskItem::GetTargetSegmentString() const { return std::to_string(mTargetSegmentIdx); }
}} // namespace indexlib::merger
