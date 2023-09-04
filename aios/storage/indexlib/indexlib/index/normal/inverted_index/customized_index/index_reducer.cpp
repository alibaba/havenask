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
#include "indexlib/index/normal/inverted_index/customized_index/index_reducer.h"

#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy;

using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, IndexReducer);

ReduceResource::ReduceResource(const SegmentMergeInfos& _segMergeInfos, const ReclaimMapPtr& _reclaimMap,
                               bool _isEntireDataSet, const SegmentDirectoryBasePtr& segDir)
    : segMergeInfos(_segMergeInfos)
    , reclaimMap(_reclaimMap)
    , isEntireDataSet(_isEntireDataSet)
    , segDirectory(segDir)
{
}

ReduceResource::~ReduceResource() {}

bool IndexReducer::Init(const IndexerResourcePtr& resource, const MergeItemHint& mergeHint,
                        const MergeTaskResourceVector& taskResources)
{
    if (!resource->schema) {
        IE_LOG(ERROR, "indexer init failed: schema is NULL");
        return false;
    }
    mMergeHint = mergeHint;
    mTaskResources = taskResources;
    return DoInit(resource);
}

vector<ReduceTask> IndexReducer::CreateReduceTasks(const vector<DirectoryPtr>& indexDirs,
                                                   const SegmentMergeInfos& segmentInfos, uint32_t instanceCount,
                                                   bool isEntireDataSet, MergeTaskResourceManagerPtr& resMgr)
{
    vector<ReduceTask> emptyReduceTask;
    return emptyReduceTask;
}

void IndexReducer::EndParallelReduce(const OutputSegmentMergeInfos& outputSegMergeInfos, int32_t totalParallelCount,
                                     const std::vector<index_base::MergeTaskResourceVector>& instResourceVec)
{
    for (const auto& info : outputSegMergeInfos) {
        indexlibv2::index::ann::ParallelReduceMeta meta(totalParallelCount);
        meta.Store(info.directory);
    }
}
}} // namespace indexlib::index
