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
#include "indexlib_plugin/plugins/aitheta/aitheta_index_reducer.h"

#include <time.h>

#include <algorithm>
#include <map>
#include <queue>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "fslib/fslib.h"
#include <aitheta/index_framework.h>
#include "autil/TimeUtility.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include <proxima/common/params_define.h>
#include "indexlib/util/Exception.h"

#include "indexlib_plugin/plugins/aitheta/util/custom_logger.h"
#include "indexlib_plugin/plugins/aitheta/aitheta_index_reduce_item.h"
#include "indexlib_plugin/plugins/aitheta/raw_segment.h"
#include "indexlib_plugin/plugins/aitheta/index_segment.h"
#include "indexlib_plugin/plugins/aitheta/index_segment_builder.h"
#include "indexlib_plugin/plugins/aitheta/util/indexlib_io_wrapper.h"
#include "indexlib_plugin/plugins/aitheta/util/parallel_merge/parallel_merge_task.h"
#include "indexlib_plugin/plugins/aitheta/util/parallel_merge/parallel_merge_util.h"

using namespace std;
using namespace aitheta;
using namespace fslib;
using namespace fslib::fs;
using namespace autil::legacy;

using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::index_base;
using namespace indexlib::common;
namespace indexlib {
namespace aitheta_plugin {

IE_LOG_SETUP(aitheta_plugin, AithetaIndexReducer);

AithetaIndexReducer::~AithetaIndexReducer() {}

bool AithetaIndexReducer::DoInit(const IndexerResourcePtr &resource) {
    RegisterGlobalIndexLogger();
    mIndexerResource = resource;
    mKnnConfig.Load(mIndexerResource->pluginPath);
    IE_LOG(INFO, "Offline KnnConfig is [%s]", mKnnConfig.IsAvailable() ? "Available" : "Not Available");
    return true;
}

int64_t AithetaIndexReducer::EstimateMemoryUse(const vector<DirectoryPtr> &indexDirs,
                                               const SegmentMergeInfos &segMergeInfos,
                                               const index_base::OutputSegmentMergeInfos &outputSgtMergeInfos,
                                               bool isSortMerge) const {
    ParallelMergeTaskPtr parallelMergeTask;
    if (!ParallelMergeTask::Retrieve(mMergeHint, mTaskResources, parallelMergeTask)) {
        INDEXLIB_FATAL_ERROR(NonExist, "failed to retrieve parallel merge task");
    }

    uint64_t maxNum = 0ul;
    {
        std::unordered_map<catid_t, size_t> catid2DocNum;
        auto func = [&](const OfflineIndexAttr &indexAttr) {
            if (parallelMergeTask) {
                auto buidingCatids = parallelMergeTask->catidSet;
                if (buidingCatids.find(indexAttr.catid) == buidingCatids.end()) {
                    return;
                }
            }
            auto &docNum = catid2DocNum[indexAttr.catid];
            docNum += indexAttr.docNum;
            maxNum = maxNum > docNum ? maxNum : docNum;
        };

        for (const auto &dir : indexDirs) {
            OfflineIndexAttrHolder holder;
            if (!holder.Load(dir)) {
                INDEXLIB_FATAL_ERROR(NonExist, "failed to load offlineIndexAttrHolder");
            }
            auto &offlineAttrs = holder.GetOfflineIndexAttrs();
            for_each(offlineAttrs.begin(), offlineAttrs.end(), func);
        }
    }
    uint64_t size = maxNum * (sizeof(docid_t) + mSchemaParam.dimension * sizeof(float));
    size *= REDUCE_MEM_USE_MAGIC_NUM;
    IE_LOG(INFO, "max doc number is[%lu], reduce memory use[%lu]", maxNum, size);
    return size;
}

// build index by ascending order of category id
bool AithetaIndexReducer::Reduce(const vector<IndexReduceItemPtr> &reduceItems,
                                 const index_base::OutputSegmentMergeInfos &outputSgtMergeInfos, bool isSortMerge,
                                 const ReduceResource &resource) {
    if (1u != outputSgtMergeInfos.size()) {
        IE_LOG(ERROR, "unexpected outputSgtMergeInfos size[%lu]", outputSgtMergeInfos.size());
        return false;
    }

    ParallelMergeTaskPtr parallelMergeTask;
    if (!ParallelMergeTask::Retrieve(mMergeHint, mTaskResources, parallelMergeTask)) {
        IE_LOG(ERROR, "failed to retrieve parallel merge task");
        return false;
    }
    set<catid_t> buildingCatids;
    if (parallelMergeTask) {
        buildingCatids = parallelMergeTask->catidSet;
    }

    vector<SegmentPtr> segments;
    for (size_t idx = 0u; idx < reduceItems.size(); ++idx) {
        auto item = DYNAMIC_POINTER_CAST(AithetaIndexReduceItem, reduceItems[idx]);
        if (nullptr == item) {
            IE_LOG(ERROR, "failed to cast to AithetaIndexReduceItem");
            return false;
        }
        auto segment = item->GetSegment();
        auto sgtType = segment->GetMeta().GetType();
        switch (sgtType) {
            case SegmentType::kIndex: {
                segments.push_back(segment);
                break;
            }
            case SegmentType::kPMIndex: {
                segments.push_back(segment);
                break;
            }
            case SegmentType::kRaw: {
                segments.push_back(segment);
            } break;
            default: {
                IE_LOG(ERROR, "unexpected type[%s]", Type2Str(sgtType).c_str());
                return false;
            }
        }
        if (!parallelMergeTask) {
            const auto &holder = segment->GetOfflineIndexAttrHolder();
            for (size_t idx = 0u; idx < holder.Size(); ++idx) {
                buildingCatids.insert(holder.Get(idx).catid);
            }
        }
    }

    IndexSegmentBuilder builder(mSchemaParam, mKnnConfig);
    auto schema = mIndexerResource->schema;
    string tableName = schema != nullptr ? schema->GetSchemaName() : "DEFAULT";
    builder.SetMetricReporter(MetricReporterPtr(
        new MetricReporter(mIndexerResource->metricProvider, tableName, mIndexerResource->indexName)));

    return builder.BatchBuild(segments, outputSgtMergeInfos[0].directory, buildingCatids);
}

vector<ReduceTask> AithetaIndexReducer::CreateReduceTasks(const vector<DirectoryPtr> &indexDirs,
                                                          const SegmentMergeInfos &segmentInfos, uint32_t taskNum,
                                                          bool isEntireDataSet, MergeTaskResourceManagerPtr &resMgr) {
    ParallelMergeInput input;
    // richard.sy(TODO): "for loop" can be set in ParallelMergeTask::Create
    for (auto dir : indexDirs) {
        //  check validation through loading meta
        if (!SegmentMeta().Load(dir)) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "load segment meta failure");
        }

        OfflineIndexAttrHolder holder;
        if (!holder.Load(dir)) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "load index attrib failure");
        }
        for (size_t idx = 0u; idx < holder.Size(); ++idx) {
            const auto &indexAttr = holder.Get(idx);
            input[indexAttr.catid] += indexAttr.docNum;
        }
    }

    vector<ParallelMergeTask> parallelMergeTasks;
    if (!ParallelMergeTask::Create(taskNum, input, parallelMergeTasks)) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "failed to create ParallelMergeTask");
    }
    vector<ReduceTask> reduceTasks;
    if (!ParallelMergeTask::Transform(parallelMergeTasks, resMgr, reduceTasks)) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "failed to transform to ReduceTask");
    }
    IE_LOG(INFO, "create num[%lu] reduce tasks", reduceTasks.size());
    return reduceTasks;
}

void AithetaIndexReducer::EndParallelReduce(const OutputSegmentMergeInfos &outputSgtMergeInfos, int32_t parallelCount,
                                            const vector<MergeTaskResourceVector> &instResourceVec) {
    for (const auto &sgtMergeInfo : outputSgtMergeInfos) {
        ParallelReduceMeta parallelReduceMeta(parallelCount);
        parallelReduceMeta.Store(sgtMergeInfo.directory);

        SegmentMeta sgtMeta;
        if (sgtMeta.Load(sgtMergeInfo.directory) && sgtMeta.GetType() != SegmentType::kPMIndex) {
            IE_LOG(INFO, "no need to do EndParallelReduce");
            continue;
        }
        ParallelMergeUtil::DumpSegmentMeta(sgtMergeInfo.directory, parallelReduceMeta);
        ParallelMergeUtil::DumpOfflineIndexAttr(sgtMergeInfo.directory, parallelReduceMeta);
    }

    IE_LOG(INFO, "finish doing EndParallelReduce");
}

}
}
