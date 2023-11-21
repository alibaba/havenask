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
#pragma once

#include <stdint.h>
#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace merger {
class MergeTaskItem : public autil::legacy::Jsonizable
{
public:
    static const std::string MERGE_TASK_ITEMS_FILE_NAME;

public:
    MergeTaskItem(uint32_t mergePlanIdx = -1, const std::string& mergeType = "", const std::string& name = "",
                  bool isSubItem = false, int64_t targetSegIdx = -1);

    ~MergeTaskItem();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    std::string ToString() const;
    std::string GetCheckPointName() const;
    void SetParallelMergeItem(index_base::ParallelMergeItem parallelMergeItem);
    index_base::ParallelMergeItem GetParallelMergeItem() const;

private:
    static std::string GetParallelMergeSuffixString(const index_base::ParallelMergeItem& parallelMergeItem);
    std::string GetTargetSegmentString() const;

public:
    uint32_t mMergePlanIdx; // idx of merge plan in merge task
    std::string mMergeType; // index or summary or attribute or pack_attribute
    std::string mName;      // index name or attribute name or summary group name
    bool mIsSubItem;
    double mCost;
    int64_t mTargetSegmentIdx;
    index_base::ParallelMergeItem mParallelMergeItem;

private:
    IE_LOG_DECLARE();
};
typedef std::vector<MergeTaskItem> MergeTaskItems;
DEFINE_SHARED_PTR(MergeTaskItem);

struct MergeTaskItemComp {
    bool operator()(const MergeTaskItem& lst, const MergeTaskItem& rst) { return lst.mCost > rst.mCost; }
};
}} // namespace indexlib::merger
