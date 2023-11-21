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

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/ann/aitheta2/util/parallel_merge/ParallelReduceMeta.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/index_base/index_meta/merge_task_resource.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/KeyValueMap.h"

DECLARE_REFERENCE_CLASS(index, IndexReduceItem);

namespace indexlib { namespace index {

using ParallelReduceMeta = indexlibv2::index::ann::ParallelReduceMeta;
class ReduceTask
{
public:
    // reduce task id
    int32_t id;
    // 'dataRatio' describes how much percent of data will be processed by this sub reduce task.
    // this parameter is used to distribute reduce task.
    float dataRatio;
    // 'resourceIds' describes how many resources is needed by this reduce task.
    // User plugin should use MergeTaskResourceManager to declare resources.
    std::vector<index_base::resourceid_t> resourceIds;
};

class ReduceResource
{
public:
    ReduceResource(const index_base::SegmentMergeInfos& _segMergeInfos, const ReclaimMapPtr& _reclaimMap,
                   bool _isEntireDataSet, const SegmentDirectoryBasePtr& segDir);
    ~ReduceResource();
    const index_base::SegmentMergeInfos& segMergeInfos;
    const ReclaimMapPtr& reclaimMap;
    bool isEntireDataSet;
    const SegmentDirectoryBasePtr& segDirectory;
};

class IndexReducer
{
public:
    IndexReducer(const util::KeyValueMap& parameters) : mParameters(parameters) {}
    virtual ~IndexReducer() {}

public:
    bool Init(const IndexerResourcePtr& resource, const index_base::MergeItemHint& mergeHint,
              const index_base::MergeTaskResourceVector& taskResources);

    virtual bool DoInit(const IndexerResourcePtr& resource) = 0;

    virtual IndexReduceItemPtr CreateReduceItem() = 0;

    virtual bool Reduce(const std::vector<IndexReduceItemPtr>& reduceItems,
                        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, // target segments
                        bool isSortMerge, const ReduceResource& resource) = 0;

    // avoid actual reading index files in this interface
    virtual int64_t EstimateMemoryUse(const std::vector<file_system::DirectoryPtr>& indexDirs,
                                      const index_base::SegmentMergeInfos& segmentInfos,
                                      const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, // target segments
                                      bool isSortMerge) const = 0;

    virtual bool EnableMultiOutputSegmentParallel() const { return false; }

    // by default, return empty ReduceTask
    virtual std::vector<ReduceTask> CreateReduceTasks(const std::vector<file_system::DirectoryPtr>& indexDirs,
                                                      const index_base::SegmentMergeInfos& segmentInfos,
                                                      uint32_t instanceCount, bool isEntireDataSet,
                                                      index_base::MergeTaskResourceManagerPtr& resMgr);

    // should be re-accessable in EndMerge phase
    virtual void EndParallelReduce(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                   int32_t totalParallelCount,
                                   const std::vector<index_base::MergeTaskResourceVector>& instResourceVec);

protected:
    util::KeyValueMap mParameters;
    index_base::MergeItemHint mMergeHint;
    index_base::MergeTaskResourceVector mTaskResources;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexReducer);
}} // namespace indexlib::index
