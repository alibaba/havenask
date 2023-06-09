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
#include "indexlib/merger/index_table_merge_meta_creator.h"

#include <beeper/beeper.h>

#include "autil/TimeUtility.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/calculator/on_disk_segment_size_calculator.h"
#include "indexlib/index/merger_util/reclaim_map/reclaim_map_creator.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/merger/merge_meta_work_item.h"
#include "indexlib/merger/merge_task_item_creator.h"
#include "indexlib/merger/merge_task_item_dispatcher.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/merger/split_strategy/split_segment_strategy_factory.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/resource_control_thread_pool.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::plugin;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, IndexTableMergeMetaCreator);

const size_t IndexTableMergeMetaCreator::MAX_THREAD_COUNT = 10;

IndexTableMergeMetaCreator::IndexTableMergeMetaCreator(const config::IndexPartitionSchemaPtr& schema,
                                                       const index::ReclaimMapCreatorPtr& reclaimMapCreator,
                                                       SplitSegmentStrategyFactory* splitStrategyFactory)
    : mSchema(schema)
    , mReclaimMapCreator(reclaimMapCreator)
    , mSplitStrategyFactory(splitStrategyFactory)
    , mInstanceCount(0)
{
}

IndexTableMergeMetaCreator::~IndexTableMergeMetaCreator() {}

void IndexTableMergeMetaCreator::Init(const merger::SegmentDirectoryPtr& segmentDirectory,
                                      const index_base::SegmentMergeInfos& mergeInfos, const MergeConfig& mergeConfig,
                                      const DumpStrategyPtr& dumpStrategy, const PluginManagerPtr& pluginManager,
                                      uint32_t instanceCount)
{
    IE_LOG(INFO, "init begin");
    mSegmentDirectory = segmentDirectory;
    mMergeConfig = mergeConfig;
    mSegMergeInfos = mergeInfos;
    IE_LOG(INFO, "create segmentMergeInfos done");
    mDumpStrategy = dumpStrategy;
    mPluginManager = pluginManager;
    mInstanceCount = instanceCount;
    IE_LOG(INFO, "init done");
}

IndexMergeMeta* IndexTableMergeMetaCreator::Create(MergeTask& task)
{
    IE_LOG(INFO, "create begin");
    CheckMergeTask(task);

    if (mMergeConfig.truncateOptionConfig) {
        CreateMergeTaskForIncTruncate(task);
    }

    unique_ptr<IndexMergeMeta> meta(new IndexMergeMeta());
    segmentid_t lastSegmentId = mDumpStrategy->GetLastSegmentId();

    DeletionMapReaderPtr deletionMapReader = mSegmentDirectory->GetDeletionMapReader();
    assert(deletionMapReader);

    SegmentMergeInfos subSegMergeInfos;
    DeletionMapReaderPtr subDeletionMapReader;

    merger::SegmentDirectoryPtr subPartSegmentDirectory = mSegmentDirectory->GetSubSegmentDirectory();
    if (subPartSegmentDirectory) {
        subDeletionMapReader = subPartSegmentDirectory->GetDeletionMapReader();
        assert(subDeletionMapReader);
        subSegMergeInfos = CreateSegmentMergeInfos(mSchema, mMergeConfig, subPartSegmentDirectory);
    }

    uint32_t planCount = task.GetPlanCount();
    meta->Resize(planCount);

    ResourceControlThreadPool threadPool(min((size_t)planCount, MAX_THREAD_COUNT), 0);
    vector<ResourceControlWorkItemPtr> workItems;
    const auto& splitConfig = mMergeConfig.GetSplitSegmentConfig();

    for (uint32_t i = 0; i < task.GetPlanCount(); ++i) {
        // create work item
        const MergePlan& plan = task[i];
        SplitSegmentStrategyPtr splitStrategy;
        if (mSplitStrategyFactory) {
            splitStrategy =
                mSplitStrategyFactory->CreateSplitStrategy(splitConfig.className, splitConfig.parameters, plan);
        }
        ResourceControlWorkItemPtr item(new MergeMetaWorkItem(
            mSchema, mMergeConfig, mSegmentDirectory, plan, mSegMergeInfos, deletionMapReader, mReclaimMapCreator,
            splitStrategy, subSegMergeInfos, subDeletionMapReader, i, meta.get()));
        workItems.push_back(item);
    }
    threadPool.Init(workItems);
    IE_LOG(INFO, "pool inited");
    threadPool.Run("indexMergeMeta");
    IE_LOG(INFO, "pool finished");
    workItems.clear();

    ResetTargetSegmentId(*meta, lastSegmentId);

    Version targetVersion = CreateNewVersion(*meta, task);
    if (!mDumpStrategy->IsDirty()) {
        return meta.release();
    }

    meta->SetTargetVersion(targetVersion);
    SchedulerMergeTaskItem(*meta, mInstanceCount);
    return meta.release();
}

void IndexTableMergeMetaCreator::ResetTargetSegmentId(IndexMergeMeta& meta, segmentid_t lastSegmentId)
{
    auto& plans = meta.GetMergePlans();
    size_t cursor = 0;
    stringstream ss;
    for (MergePlan& plan : plans) {
        ss << "MergePlan [" << cursor++ << "]: toMergeSegments [" << plan.ToString() << "], targetSegments [";
        for (int i = 0, count = plan.GetTargetSegmentCount(); i < count; ++i) {
            plan.SetTargetSegmentId(i, ++lastSegmentId);
            if (i != 0) {
                ss << ", ";
            }
            ss << plan.GetTargetSegmentId(i);
        }
        ss << "]" << endl;
    }
    BEEPER_REPORT(INDEXLIB_BUILD_INFO_COLLECTOR_NAME, ss.str());
}

void IndexTableMergeMetaCreator::CheckMergeTask(const MergeTask& task) const
{
    for (size_t i = 0; i < task.GetPlanCount(); ++i) {
        const MergePlan& plan = task[i];
        if (plan.GetSegmentCount() == 0) {
            INDEXLIB_FATAL_ERROR(BadParameter, "Invalid merge task: no segment in merge plan.");
        }

        const Version& version = mSegmentDirectory->GetVersion();
        MergePlan::Iterator iter = plan.CreateIterator();
        while (iter.HasNext()) {
            segmentid_t segId = iter.Next();
            if (!version.HasSegment(segId)) {
                INDEXLIB_FATAL_ERROR(BadParameter, "MergePlan has invalid segment [%d].", segId);
            }
        }
    }
}

void IndexTableMergeMetaCreator::CreateMergeTaskForIncTruncate(MergeTask& task)
{
    // inc build does not support parallel
    SegmentMergeInfos notMergeSegs;
    GetNotMergedSegments(notMergeSegs);
    for (size_t i = 0; i < notMergeSegs.size(); i++) {
        bool alreadyInPlan = false;
        for (size_t j = 0; j < task.GetPlanCount(); j++) {
            if (task[j].HasSegment(notMergeSegs[i].segmentId)) {
                alreadyInPlan = true;
                break;
            }
        }
        if (!alreadyInPlan) {
            MergePlan plan;
            plan.AddSegment(notMergeSegs[i]);
            task.AddPlan(plan);
        }
    }
}

void IndexTableMergeMetaCreator::GetNotMergedSegments(SegmentMergeInfos& segments)
{
    for (const auto& segmentMergeInfo : mSegMergeInfos) {
        const SegmentInfo& segInfo = segmentMergeInfo.segmentInfo;
        if (!segInfo.mergedSegment) {
            segments.push_back(segmentMergeInfo);
        }
    }
}

void IndexTableMergeMetaCreator::CreateInPlanMergeInfos(SegmentMergeInfos& inPlanSegMergeInfos,
                                                        const SegmentMergeInfos& segMergeInfos,
                                                        const MergePlan& plan) const
{
    exdocid_t baseDocId = 0;
    for (size_t i = 0; i < segMergeInfos.size(); ++i) {
        const SegmentMergeInfo& segMergeInfo = segMergeInfos[i];
        if (plan.HasSegment(segMergeInfo.segmentId)) {
            SegmentMergeInfo inPlanSegMergeInfo = segMergeInfo;
            inPlanSegMergeInfo.baseDocId = baseDocId;
            inPlanSegMergeInfos.push_back(segMergeInfo);

            baseDocId += segMergeInfo.segmentInfo.docCount;
        }
    }
}

void IndexTableMergeMetaCreator::CreateNotInPlanMergeInfos(SegmentMergeInfos& notInPlanSegMergeInfos,
                                                           const SegmentMergeInfos& segMergeInfos,
                                                           const MergeTask& task) const
{
    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        bool inplan = false;
        for (uint32_t j = 0; j < task.GetPlanCount(); j++) {
            const MergePlan& plan = task[j];
            if (plan.HasSegment(segMergeInfos[i].segmentId)) {
                inplan = true;
                break;
            }
        }
        if (!inplan) {
            notInPlanSegMergeInfos.push_back(segMergeInfos[i]);
        }
    }
}

index_base::Version IndexTableMergeMetaCreator::CreateNewVersion(IndexMergeMeta& meta, MergeTask& task)
{
    const vector<MergePlan>& mergePlans = meta.GetMergePlans();
    // this addSegments is for full merger && split to multi segment, it may cause remove should keep segment
    set<segmentid_t> addSegments;
    for (uint32_t i = 0; i < mergePlans.size(); ++i) {
        const auto& plan = mergePlans[i];
        const auto& segMergeInfos = plan.GetSegmentMergeInfos();
        for (size_t j = 0; j < segMergeInfos.size(); ++j) {
            if (addSegments.find(segMergeInfos[j].segmentId) == addSegments.end()) {
                mDumpStrategy->RemoveSegment(segMergeInfos[j].segmentId);
            }
        }
        for (size_t p = 0; p < plan.GetTargetSegmentCount(); ++p) {
            mDumpStrategy->AddMergedSegment(plan.GetTargetSegmentId(p));
            if (mSchema->EnableTemperatureLayer()) {
                mDumpStrategy->AddSegmentTemperatureMeta(plan.GetTargetSegmentTemperatureMeta(p));
            }
            addSegments.insert(plan.GetTargetSegmentId(p));
        }
    }
    mDumpStrategy->IncVersionId();
    return mDumpStrategy->GetVersion();
}

void IndexTableMergeMetaCreator::SchedulerMergeTaskItem(IndexMergeMeta& meta, uint32_t instanceCount)
{
    MergeTaskItemDispatcher mergeTaskItemDispatcher(mSegmentDirectory, mSegmentDirectory->GetSubSegmentDirectory(),
                                                    mSchema, mMergeConfig.truncateOptionConfig);

    MergeTaskResourceManagerPtr taskResMgr(new MergeTaskResourceManager);
    taskResMgr->Init(GetMergeResourceRootPath(), mSegmentDirectory->GetRootDir()->GetFenceContext().get());
    MergeTaskItemCreator mergeTaskItemCreator(&meta, mSegmentDirectory, mSegmentDirectory->GetSubSegmentDirectory(),
                                              mSchema, mPluginManager, mMergeConfig, taskResMgr);
    MergeTaskItems allItems = mergeTaskItemCreator.CreateMergeTaskItems(instanceCount);
    vector<MergeTaskItems> schedulerMergeTaskItem =
        mergeTaskItemDispatcher.DispatchMergeTaskItem(meta, allItems, instanceCount);
    meta.SetMergeTaskItems(schedulerMergeTaskItem);
    meta.SetMergeTaskResourceVec(taskResMgr->GetResourceVec());
}

std::string IndexTableMergeMetaCreator::GetMergeResourceRootPath() const
{
    return util::PathUtil::JoinPath(mDumpStrategy->GetRootPath(), "merge_resource",
                                    mDumpStrategy->GetVersion().GetVersionFileName());
}
}} // namespace indexlib::merger
