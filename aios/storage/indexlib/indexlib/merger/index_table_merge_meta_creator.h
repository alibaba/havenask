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

#include <stddef.h>
#include <stdint.h>
#include <string>

#include "indexlib/base/Types.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/index_merge_meta.h"
#include "indexlib/merger/merge_meta_creator.h"
#include "indexlib/merger/merge_plan.h"
#include "indexlib/merger/merge_task.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

DECLARE_REFERENCE_CLASS(merger, SplitSegmentStrategyFactory);

namespace indexlib { namespace merger {

// TODO: refactor

class IndexTableMergeMetaCreator : public MergeMetaCreator
{
public:
    static const size_t MAX_THREAD_COUNT;

public:
    IndexTableMergeMetaCreator(const config::IndexPartitionSchemaPtr& schema,
                               const index::ReclaimMapCreatorPtr& reclaimMapCreator,
                               SplitSegmentStrategyFactory* splitStrategyFactory);
    ~IndexTableMergeMetaCreator();

public:
    void Init(const merger::SegmentDirectoryPtr& segmentDirectory, const index_base::SegmentMergeInfos& mergeInfos,
              const config::MergeConfig& mergeConfig, const merger::DumpStrategyPtr& dumpStrategy,
              const plugin::PluginManagerPtr& pluginManager, uint32_t instanceCount) override final;

    IndexMergeMeta* Create(MergeTask& task) override final;

private:
    void CheckMergeTask(const MergeTask& task) const;
    void CreateMergeTaskForIncTruncate(MergeTask& task);
    void GetNotMergedSegments(index_base::SegmentMergeInfos& segments);
    void CreateInPlanMergeInfos(index_base::SegmentMergeInfos& inPlanSegMergeInfos,
                                const index_base::SegmentMergeInfos& segMergeInfos,
                                const merger::MergePlan& plan) const;
    void CreateNotInPlanMergeInfos(index_base::SegmentMergeInfos& notInPlanSegMergeInfos,
                                   const index_base::SegmentMergeInfos& segMergeInfos, const MergeTask& task) const;
    index_base::Version CreateNewVersion(IndexMergeMeta& meta, MergeTask& task);
    void SchedulerMergeTaskItem(IndexMergeMeta& meta, uint32_t instanceCount);
    std::string GetMergeResourceRootPath() const;
    void ResetTargetSegmentId(IndexMergeMeta& meta, segmentid_t lastSegmentId);

private:
    config::IndexPartitionSchemaPtr mSchema;
    merger::SegmentDirectoryPtr mSegmentDirectory;
    config::MergeConfig mMergeConfig;
    index_base::SegmentMergeInfos mSegMergeInfos;
    merger::DumpStrategyPtr mDumpStrategy;
    index::ReclaimMapCreatorPtr mReclaimMapCreator;
    SplitSegmentStrategyFactory* mSplitStrategyFactory;
    plugin::PluginManagerPtr mPluginManager;
    uint32_t mInstanceCount;

private:
    friend class IndexTableMergeMetaCreatorTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexTableMergeMetaCreator);
}} // namespace indexlib::merger
