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
#ifndef __INDEXLIB_MERGE_TASK_ITEM_DISPATCHER_H
#define __INDEXLIB_MERGE_TASK_ITEM_DISPATCHER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/truncate_option_config.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/index_merge_meta.h"
#include "indexlib/merger/merge_task.h"
#include "indexlib/merger/merge_task_item.h"
#include "indexlib/merger/merge_work_item_typed.h"

namespace indexlib { namespace merger {

class MergeTaskItemDispatcher
{
public:
    MergeTaskItemDispatcher(const merger::SegmentDirectoryPtr& segDir, const merger::SegmentDirectoryPtr& subSegDir,
                            const config::IndexPartitionSchemaPtr& schema,
                            const config::TruncateOptionConfigPtr& truncateOptionConfig);
    MergeTaskItemDispatcher(const merger::SegmentDirectoryPtr& segDir, const config::IndexPartitionSchemaPtr& schema,
                            const config::TruncateOptionConfigPtr& truncateOptionConfig);
    ~MergeTaskItemDispatcher();

public:
    struct MergeItemIdentify {
    public:
        MergeItemIdentify(const std::string& t = "", const std::string& n = "", bool sub = false)
            : type(t)
            , name(n)
            , isSub(sub)
        {
        }
        ~MergeItemIdentify() {}

    public:
        bool operator<(const MergeItemIdentify& other) const
        {
            if (type != other.type) {
                return type < other.type;
            }
            if (name != other.name) {
                return name < other.name;
            }
            return isSub < other.isSub;
        }
        std::string ToString() const { return type + " " + name + " " + (isSub ? "true" : "false"); }

    public:
        std::string type;
        std::string name;
        bool isSub;
    };

public:
    std::vector<MergeTaskItems> DispatchMergeTaskItem(const IndexMergeMeta& meta, MergeTaskItems& allItems,
                                                      uint32_t instanceCount);

private:
    std::pair<int32_t, int32_t> GetSampleSegmentIdx(const std::vector<MergePlan>& mergePlanMetas);

    void InitMergeTaskItemCost(segmentid_t sampleSegId, uint32_t mainPartDocCount, uint32_t subPartDocCount,
                               const IndexMergeMeta& meta, std::vector<MergeTaskItem>& allItems);

    std::vector<MergeTaskItems> DispatchWorkItems(MergeTaskItems& allItems, uint32_t instanceCount);

    uint64_t GetDirSize(const index_base::SegmentData& segData, const std::string& dirName, const std::string& name);

    uint64_t GetDirSize(const file_system::DirectoryPtr& directory, const std::string& dirName,
                        const std::string& name);

    uint64_t GetDirSize(const file_system::DirectoryPtr& directory);

private:
    merger::SegmentDirectoryPtr mSegDir;
    merger::SegmentDirectoryPtr mSubSegDir;
    config::IndexPartitionSchemaPtr mSchema;
    config::TruncateOptionConfigPtr mTruncateOptionConfig;

private:
    // merging index is slower than attribute
    static constexpr double INDEX_COST_BOOST = 20;
    static constexpr double UNIQ_ENCODE_ATTR_COST_BOOST = 8;
    static constexpr double DELETION_MAP_COST = 0.01;

private:
    friend class MergeTaskItemDispatcherTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeTaskItemDispatcher);
}} // namespace indexlib::merger

#endif //__INDEXLIB_MERGE_TASK_ITEM_DISPATCHER_H
