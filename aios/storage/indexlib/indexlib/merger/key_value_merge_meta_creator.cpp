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
#include "indexlib/merger/key_value_merge_meta_creator.h"

#include <algorithm>
#include <beeper/beeper.h>
#include <limits>
#include <memory>
#include <ostream>
#include <stddef.h>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/document/locator.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/IoConfig.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/index_base/index_meta/progress_synchronizer.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/index_base/index_meta/segment_topology_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/dump_strategy.h"
#include "indexlib/merger/merge_meta_work_item.h"
#include "indexlib/merger/merge_task_item.h"
#include "indexlib/merger/merge_task_item_creator.h"
#include "indexlib/merger/merge_task_item_dispatcher.h"
#include "indexlib/merger/multi_part_segment_directory.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::plugin;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, KeyValueMergeMetaCreator);

KeyValueMergeMetaCreator::KeyValueMergeMetaCreator(const IndexPartitionSchemaPtr& schema)
    : mSchema(schema)
    , mInstanceCount(0)
{
}

KeyValueMergeMetaCreator::~KeyValueMergeMetaCreator() {}

void KeyValueMergeMetaCreator::Init(const merger::SegmentDirectoryPtr& segmentDirectory,
                                    const index_base::SegmentMergeInfos& mergeInfos, const MergeConfig& mergeConfig,
                                    const DumpStrategyPtr& dumpStrategy, const PluginManagerPtr& pluginManager,
                                    uint32_t instanceCount)
{
    mSegmentDirectory = segmentDirectory;
    mSegMergeInfos = mergeInfos;
    mMergeConfig = mergeConfig;
    mDumpStrategy = dumpStrategy;
    mPluginManager = pluginManager;
    mInstanceCount = instanceCount;
}

IndexMergeMeta* KeyValueMergeMetaCreator::Create(MergeTask& task)
{
    IndexMergeMeta* meta = new IndexMergeMeta();
    try {
        CreateMergePlans(*meta, task);
        CreateMergeTaskItems(*meta);
    } catch (...) {
        DELETE_AND_SET_NULL(meta);
        throw;
    }
    return meta;
}

void KeyValueMergeMetaCreator::CreateMergePlans(IndexMergeMeta& meta, const MergeTask& task)
{
    uint32_t planCount = task.GetPlanCount();
    meta.Resize(planCount);
    segmentid_t lastSegmentId = mDumpStrategy->GetLastSegmentId();
    vector<pair<segmentid_t, SegmentTopologyInfo>> targetSegments;

    auto multiPartSegDir = DYNAMIC_POINTER_CAST(MultiPartSegmentDirectory, mSegmentDirectory);
    SegmentInfo mergedSegInfo;
    if (multiPartSegDir) {
        vector<SegmentInfo> segInfos;
        MergeMetaWorkItem::GetLastSegmentInfosForMultiPartitionMerge(multiPartSegDir, mSegMergeInfos, segInfos);
        // use maxTs & min locator
        ProgressSynchronizer ps;
        ps.Init(segInfos);
        indexlibv2::framework::Locator locator;
        locator.Deserialize(ps.GetLocator().ToString());
        mergedSegInfo.SetLocator(locator);
        mergedSegInfo.timestamp = ps.GetTimestamp();
    }

    stringstream ss;
    for (size_t i = 0; i < planCount; ++i) {
        MergePlan& mergePlan = meta.GetMergePlan(i);
        mergePlan = task[i];
        segmentid_t targetSegmentId = ++lastSegmentId;
        mergePlan.SetTargetSegmentId(0, targetSegmentId);
        MergePlan::Iterator iter = mergePlan.CreateIterator();
        while (iter.HasNext()) {
            segmentid_t segId = iter.Next();
            mDumpStrategy->RemoveSegment(segId);
        }
        targetSegments.push_back({targetSegmentId, mergePlan.GetTargetTopologyInfo(0)});
        if (multiPartSegDir) {
            mergePlan.GetTargetSegmentInfo(0).SetLocator(mergedSegInfo.GetLocator());
            mergePlan.GetTargetSegmentInfo(0).timestamp = mergedSegInfo.timestamp;
        }
        UpdateLevelCursor(mergePlan, mDumpStrategy);

        ss << "MergePlan [" << i << "]: toMergeSegments [" << mergePlan.ToString() << "], targetSegments [";
        for (size_t idx = 0; idx < mergePlan.GetTargetSegmentCount(); idx++) {
            if (idx != 0) {
                ss << ", ";
            }
            ss << mergePlan.GetTargetSegmentId(idx);
        }
        ss << "]" << endl;
    }
    BEEPER_REPORT(INDEXLIB_BUILD_INFO_COLLECTOR_NAME, ss.str());

    for (const auto& segment : targetSegments) {
        mDumpStrategy->AddMergedSegment(segment.first, segment.second);
    }

    mDumpStrategy->IncVersionId();
    if (mDumpStrategy->IsDirty()) {
        meta.SetTargetVersion(mDumpStrategy->GetVersion());
    }
}

void KeyValueMergeMetaCreator::CreateMergeTaskItems(IndexMergeMeta& meta)
{
    MergeTaskItemDispatcher mergeTaskItemDispatcher(mSegmentDirectory, mSegmentDirectory->GetSubSegmentDirectory(),
                                                    mSchema, mMergeConfig.truncateOptionConfig);
    MergeTaskResourceManagerPtr taskResMgr(new MergeTaskResourceManager);
    taskResMgr->Init(GetMergeResourceRootPath(), mSegmentDirectory->GetRootDir()->GetFenceContext().get());
    MergeTaskItemCreator mergeTaskItemCreator(&meta, mSegmentDirectory, mSegmentDirectory->GetSubSegmentDirectory(),
                                              mSchema, mPluginManager, mMergeConfig, taskResMgr);
    MergeTaskItems allItems = mergeTaskItemCreator.CreateMergeTaskItems(mInstanceCount);
    meta.SetMergeTaskItems(mergeTaskItemDispatcher.DispatchMergeTaskItem(meta, allItems, mInstanceCount));
    meta.SetMergeTaskResourceVec(taskResMgr->GetResourceVec());
}

void KeyValueMergeMetaCreator::UpdateLevelCursor(const MergePlan& mergePlan, const DumpStrategyPtr& dumpStrategy)
{
    uint32_t minLevel = numeric_limits<uint32_t>::max();
    uint32_t maxLevel = 0;
    for (const auto& segMergeInfo : mergePlan.GetSegmentMergeInfos()) {
        minLevel = min(minLevel, segMergeInfo.levelIdx);
        maxLevel = max(maxLevel, segMergeInfo.levelIdx);
    }
    for (uint32_t levelIdx = minLevel; levelIdx < maxLevel; ++levelIdx) {
        if (levelIdx > 0) {
            dumpStrategy->IncreaseLevelCursor(levelIdx);
        }
    }
}

std::string KeyValueMergeMetaCreator::GetMergeResourceRootPath() const
{
    return util::PathUtil::JoinPath(mDumpStrategy->GetRootPath(), "merge_resource",
                                    mDumpStrategy->GetVersion().GetVersionFileName());
}
}} // namespace indexlib::merger
