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
#ifndef __INDEXLIB_CUSTOMIZED_INDEX_MERGER_H
#define __INDEXLIB_CUSTOMIZED_INDEX_MERGER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/normal/framework/index_merger_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_merger.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_reducer.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);

namespace indexlib { namespace index {

class CustomizedIndexMerger : public index::NormalIndexMerger
{
public:
    CustomizedIndexMerger(const plugin::PluginManagerPtr& pluginManager);
    ~CustomizedIndexMerger();

    DECLARE_INDEX_MERGER_IDENTIFIER(customized);

public:
    void Merge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
               const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    void SortByWeightMerge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    void EndMerge() override;

    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              bool isSortedMerge) const override;

    bool EnableMultiOutputSegmentParallel() const override;

    std::vector<index_base::ParallelMergeItem>
    CreateParallelMergeItems(const SegmentDirectoryBasePtr& segDir,
                             const index_base::SegmentMergeInfos& inPlanSegMergeInfos, uint32_t instanceCount,
                             bool isEntireDataSet, index_base::MergeTaskResourceManagerPtr& resMgr) const override;

    void EndParallelMerge(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, int32_t totalParallelCount,
                          const std::vector<index_base::MergeTaskResourceVector>& instResourceVec) override;

protected:
    void Init(const config::IndexConfigPtr& indexConfig) override;

private:
    void InnerMerge(const index::MergerResource& mergerResource, const index_base::SegmentMergeInfos& segMergeInfos,
                    index_base::OutputSegmentMergeInfos outputSegMergeInfos, bool isSortMerge);

    void GetFilteredSegments(const SegmentDirectoryBasePtr& segDir, const index_base::SegmentMergeInfos& segMergeInfos,
                             std::vector<file_system::DirectoryPtr>& indexDirs,
                             index_base::SegmentMergeInfos& filteredSegMergeInfos) const;

private:
    std::shared_ptr<legacy::OnDiskIndexIteratorCreator> CreateOnDiskIndexIteratorCreator() override;

private:
    plugin::PluginManagerPtr mPluginManager;
    IndexReducerPtr mIndexReducer;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomizedIndexMerger);
}} // namespace indexlib::index

#endif //__INDEXLIB_CUSTOMIZED_INDEX_MERGER_H
