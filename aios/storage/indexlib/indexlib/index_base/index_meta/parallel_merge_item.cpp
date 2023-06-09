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
#include "indexlib/index_base/index_meta/parallel_merge_item.h"

#include "autil/StringUtil.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, ParallelMergeItem);

ParallelMergeItem::ParallelMergeItem()
    : mId(INVALID_PARALLEL_MERGE_ITEM_ID)
    , mDataRatio(1.0)
    , mTaskGroupId(INVALID_TASK_GROUP_ID)
    , mTotalParallelCount(INVALID_TOTAL_PARALLEL_COUNT)
{
}

ParallelMergeItem::ParallelMergeItem(int32_t id, float dataRatio, int32_t taskGroupId, int32_t totalParallelCount)
    : mId(id)
    , mDataRatio(dataRatio)
    , mTaskGroupId(taskGroupId)
    , mTotalParallelCount(totalParallelCount)
{
    if (dataRatio > 1.0 || dataRatio < 0) {
        INDEXLIB_THROW(util::BadParameterException, "invalid dataRatio[%.4f]", dataRatio);
    }
}

ParallelMergeItem::~ParallelMergeItem() {}

int32_t ParallelMergeItem::GetId() const { return mId; }

void ParallelMergeItem::SetId(int32_t id) { mId = id; }

float ParallelMergeItem::GetDataRatio() const { return mDataRatio; }

void ParallelMergeItem::SetDataRatio(float dataRatio)
{
    if (dataRatio > 1.0 || dataRatio <= 0) {
        INDEXLIB_THROW(util::BadParameterException, "invalid dataRatio[%.4f]", dataRatio);
    }
    mDataRatio = dataRatio;
}

vector<int32_t> ParallelMergeItem::GetResourceIds() const { return mResourceIds; }

void ParallelMergeItem::AddResource(int32_t resId) { mResourceIds.push_back(resId); }

void ParallelMergeItem::RemoveResource(int32_t resId)
{
    for (auto it = mResourceIds.begin(); it != mResourceIds.end(); it++) {
        if ((*it) != resId) {
            continue;
        }
        mResourceIds.erase(it--);
    }
}

void ParallelMergeItem::Jsonize(JsonWrapper& json)
{
    json.Jsonize("id", mId);
    json.Jsonize("data_ratio", mDataRatio);
    json.Jsonize("resource_ids", mResourceIds);
    json.Jsonize("task_group_id", mTaskGroupId);
    json.Jsonize("total_parallel_count", mTotalParallelCount);
}

string ParallelMergeItem::ToString() const { return "ParallelMergeItem[" + autil::StringUtil::toString(mId) + "]"; }

int32_t ParallelMergeItem::GetTaskGroupId() const { return mTaskGroupId; }

void ParallelMergeItem::SetTaskGroupId(int32_t taskGroupId) { mTaskGroupId = taskGroupId; }

int32_t ParallelMergeItem::GetTotalParallelCount() const { return mTotalParallelCount; }

void ParallelMergeItem::SetTotalParallelCount(int32_t totalParallelCount) { mTotalParallelCount = totalParallelCount; }

bool ParallelMergeItem::operator==(const ParallelMergeItem& other) const
{
    if (mId != other.mId) {
        return false;
    }
    if (mDataRatio != other.mDataRatio) {
        return false;
    }
    if (mTaskGroupId != other.mTaskGroupId) {
        return false;
    }
    if (mTotalParallelCount != other.mTotalParallelCount) {
        return false;
    }
    if (mResourceIds.size() != other.mResourceIds.size()) {
        return false;
    }
    for (size_t i = 0; i < mResourceIds.size(); i++) {
        if (mResourceIds[i] != other.mResourceIds[i]) {
            return false;
        }
    }
    return true;
}

string ParallelMergeItem::GetParallelInstanceDirName() const
{
    return GetParallelInstanceDirName(mTotalParallelCount, mId);
}

string ParallelMergeItem::GetParallelInstanceDirName(int32_t parallelCount, int32_t parallelId)
{
    if (parallelCount > 1 && parallelId >= 0) {
        return "inst_" + StringUtil::toString(parallelCount) + "_" + StringUtil::toString(parallelId);
    }
    return string("");
}
}} // namespace indexlib::index_base
