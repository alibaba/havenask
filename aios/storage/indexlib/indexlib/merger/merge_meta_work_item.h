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
#include <vector>

#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/index/merger_util/truncate/bucket_map.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/merger/index_merge_meta.h"
#include "indexlib/merger/merge_plan.h"
#include "indexlib/merger/multi_part_segment_directory.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/util/resource_control_work_item.h"

DECLARE_REFERENCE_CLASS(index, ReclaimMapCreator);
DECLARE_REFERENCE_CLASS(merger, SplitSegmentStrategy);

namespace indexlib { namespace merger {

class MergeMetaWorkItem : public util::ResourceControlWorkItem
{
public:
    // TODO
    // too many arguments
    MergeMetaWorkItem(const config::IndexPartitionSchemaPtr& schema, const config::MergeConfig& mergeConfig,
                      const merger::SegmentDirectoryPtr& segDir, const MergePlan& mergePlan,
                      const index_base::SegmentMergeInfos& segMergeInfos,
                      const index::DeletionMapReaderPtr& delMapReader,
                      const index::ReclaimMapCreatorPtr& reclaimMapCreator,
                      const SplitSegmentStrategyPtr& splitStrategy,
                      const index_base::SegmentMergeInfos& subSegMergeInfos,
                      const index::DeletionMapReaderPtr& subDelMapReader, uint32_t planId,
                      merger::IndexMergeMeta* mergeMeta);

    ~MergeMetaWorkItem();

public:
    void Process() override;

    void Destroy() override;

    // TODO: Control merge mem use
    int64_t GetRequiredResource() const override { return 0; }

public:
    static void GetLastSegmentInfosForMultiPartitionMerge(const MultiPartSegmentDirectoryPtr& multiPartSegDir,
                                                          const index_base::SegmentMergeInfos& segMergeInfos,
                                                          std::vector<index_base::SegmentInfo>& segInfos);

private:
    void CreateMergePlanMetaAndResource(const MergePlan& plan, uint32_t planId,
                                        const index_base::SegmentMergeInfos& segMergeInfos,
                                        const index::DeletionMapReaderPtr& deletionMapReader,
                                        const index_base::SegmentMergeInfos& subSegMergeInfos,
                                        const index::DeletionMapReaderPtr& subDeletionMapReader,
                                        merger::IndexMergeMeta* meta);
    void CreateInPlanMergeInfos(index_base::SegmentMergeInfos& inPlanSegMergeInfos,
                                const index_base::SegmentMergeInfos& segMergeInfos, const MergePlan& plan) const;

    index::ReclaimMapPtr CreateSubPartReclaimMap(const index::ReclaimMapPtr& mainReclaimMap,
                                                 const index_base::SegmentMergeInfos& mainSegMergeInfos,
                                                 const index::DeletionMapReaderPtr& delMapReader,
                                                 const index_base::SegmentMergeInfos& segMergeInfos);
    index::legacy::BucketMaps CreateBucketMaps(const index_base::SegmentMergeInfos& mergeInfos,
                                               const index::ReclaimMapPtr& reclaimMap) const;
    index_base::SegmentInfo CreateTargetSegmentInfo(const index_base::SegmentMergeInfos& inPlanSegMergeInfos,
                                                    const index_base::SegmentMergeInfos& segMergeInfos,
                                                    uint32_t deletedDocCount) const;
    void SplitTargetSegment(MergePlan& plan, const index_base::SegmentInfo& targetSegInfo,
                            const index_base::SegmentInfo& subTargetSegInfo,
                            const SplitSegmentStrategyPtr& splitStrategy, const index::ReclaimMapPtr& reclaimMap,
                            const index::ReclaimMapPtr& subReclaimMap);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::MergeConfig mMergeConfig;
    merger::SegmentDirectoryPtr mSegmentDirectory;
    MergePlan mMergePlan;
    index_base::SegmentMergeInfos mSegMergeInfos;
    index::DeletionMapReaderPtr mDelMapReader;
    const index::ReclaimMapCreatorPtr mReclaimMapCreator;
    const SplitSegmentStrategyPtr mSplitStrategy;
    index_base::SegmentMergeInfos mSubSegMergeInfos;
    index::DeletionMapReaderPtr mSubDelMapReader;
    uint32_t mPlanId;
    merger::IndexMergeMeta* mMergeMeta;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeMetaWorkItem);
}} // namespace indexlib::merger
