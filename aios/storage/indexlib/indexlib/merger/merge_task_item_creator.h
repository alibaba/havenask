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
#ifndef __INDEXLIB_MERGE_TASK_ITEM_CREATOR_H
#define __INDEXLIB_MERGE_TASK_ITEM_CREATOR_H

#include <functional>
#include <memory>

#include "autil/legacy/any.h"
#include "indexlib/common_define.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/index_merge_meta.h"

DECLARE_REFERENCE_CLASS(merger, MergeMeta);
DECLARE_REFERENCE_CLASS(merger, MergePlan);
DECLARE_REFERENCE_CLASS(merger, MergeTaskItem);
DECLARE_REFERENCE_CLASS(merger, SegmentDirectory);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(config, SummarySchema);
DECLARE_REFERENCE_CLASS(config, IndexSchema);
DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, MergeTaskResourceManager);
DECLARE_REFERENCE_CLASS(index, SummaryMerger);
DECLARE_REFERENCE_CLASS(index, AttributeMerger);
DECLARE_REFERENCE_CLASS(index, IndexMerger);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);

namespace indexlib { namespace merger {

class MergeTaskItemCreator
{
public:
    MergeTaskItemCreator(IndexMergeMeta* meta, const merger::SegmentDirectoryPtr& segDir,
                         const merger::SegmentDirectoryPtr& subSegDir, const config::IndexPartitionSchemaPtr& schema,
                         const plugin::PluginManagerPtr& pluginManager, const config::MergeConfig& mergeConfig,
                         index_base::MergeTaskResourceManagerPtr& resMgr);
    ~MergeTaskItemCreator();

public:
    using GenerateParallelMergeItem =
        std::function<std::vector<index_base::ParallelMergeItem>(size_t planIdx, bool isEntireDataSet)>;
    struct MergeItemIdentify {
    public:
        MergeItemIdentify(const std::string& t = "", const std::string& n = "", bool sub = false,
                          GenerateParallelMergeItem generateParallel = nullptr, bool enableParallel = false)
            : type(t)
            , name(n)
            , isSub(sub)
            , generateParallelMergeItem(std::move(generateParallel))
            , enableInPlanMultiSegmentParallel(enableParallel)
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
        GenerateParallelMergeItem generateParallelMergeItem;
        bool enableInPlanMultiSegmentParallel;
    };

public:
    std::vector<MergeTaskItem> CreateMergeTaskItems(uint32_t instanceCount = 1, bool isOptimize = false);

private:
    void InitMergeTaskIdentify(const config::IndexPartitionSchemaPtr& schema, bool isSubSchema, uint32_t instanceCount,
                               bool isOptimize);

    void InitSummaryMergeTaskIdentify(const config::IndexPartitionSchemaPtr& schema, bool isSubSchema,
                                      uint32_t instanceCount, std::vector<MergeItemIdentify>& mergeItemIdentifys);

    void InitSourceMergeTaskIdentify(const config::IndexPartitionSchemaPtr& schema, bool isSubSchema,
                                     uint32_t instanceCount, std::vector<MergeItemIdentify>& mergeItemIdentifys);

    void InitIndexMergeTaskIdentify(const config::IndexPartitionSchemaPtr& schema, bool isSubSchema,
                                    uint32_t instanceCount, std::vector<MergeItemIdentify>& mergeItemIdentifys);

    void InitAttributeMergeTaskIdentify(const config::IndexPartitionSchemaPtr& schema, bool isSubSchema,
                                        uint32_t instanceCount, bool isOptimize,
                                        std::vector<MergeItemIdentify>& mergeItemIdentifys);

    void CreateMergeTaskItemsByMergeItemIdentifiers(const std::vector<MergeItemIdentify>& mergeItemIdentifys,
                                                    size_t instanceCount,
                                                    std::vector<MergeTaskItem>& mergeTaskItems) const;

    index_base::OutputSegmentMergeInfo CreateOutputSegmentMergeInfo();

private:
    // for test
    virtual index::SummaryMergerPtr CreateSummaryMerger(const config::SummarySchemaPtr& summarySchema,
                                                        const std::string& summaryGroupName) const;
    virtual index::AttributeMergerPtr CreateAttributeMerger(const config::AttributeSchemaPtr& attrSchema,
                                                            const std::string& attrName, bool isPackAttribute,
                                                            bool isOptimize) const;
    virtual index::IndexMergerPtr CreateIndexMerger(const config::IndexConfigPtr& indexConfig) const;

    void CreateParallelMergeTaskItems(const MergeItemIdentify& identify, size_t planIdx, bool isEntireDataSet,
                                      size_t instanceCount, size_t& taskGroupId,
                                      std::vector<MergeTaskItem>& mergeTaskItems) const;

    template <typename MergerType>
    std::vector<index_base::ParallelMergeItem>
    DoCreateParallelMergeItemByMerger(const MergerType& mergerPtr, size_t planIdx, bool isSub, bool isEntireDataSet,
                                      size_t instanceCount) const;

private:
    IndexMergeMeta* mMergeMeta;
    merger::SegmentDirectoryPtr mSegDir;
    merger::SegmentDirectoryPtr mSubSegDir;
    config::IndexPartitionSchemaPtr mSchema;
    config::MergeConfig mMergeConfig;
    index_base::MergeTaskResourceManagerPtr& mResMgr;
    plugin::PluginManagerPtr mPluginManager;
    std::vector<MergeItemIdentify> mMergeItemIdentifys;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeTaskItemCreator);
}} // namespace indexlib::merger

#endif //__INDEXLIB_MERGE_TASK_ITEM_CREATOR_H
