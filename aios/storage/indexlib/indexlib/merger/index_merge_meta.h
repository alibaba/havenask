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

#include <algorithm>
#include <assert.h>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "indexlib/base/Types.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/index/merger_util/truncate/bucket_map.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_meta.h"
#include "indexlib/merger/merge_plan.h"
#include "indexlib/merger/merge_plan_resource.h"
#include "indexlib/merger/merge_task_item.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

DECLARE_REFERENCE_CLASS(index_base, MergeTaskResource);
DECLARE_REFERENCE_CLASS(index_base, MergeTaskResourceManager);
DECLARE_REFERENCE_CLASS(file_system, IFileSystem);

namespace indexlib { namespace merger {

class IndexMergeMeta : public MergeMeta
{
public:
    static const std::string MERGE_PLAN_DIR_PREFIX;
    static const std::string MERGE_META_VERSION;
    static const std::string MERGE_TIMESTAMP_FILE;
    static const std::string MERGE_TASK_RESOURCE_FILE;

public:
    IndexMergeMeta();
    ~IndexMergeMeta();

public:
    const std::vector<MergeTaskItems>& GetMergeTaskItemsVec() const { return mMergeTaskItemsVec; }
    void SetMergeTaskItems(const std::vector<MergeTaskItems>& mergeTaskItemsVec)
    {
        mMergeTaskItemsVec = mergeTaskItemsVec;
    }

    void SetMergeTaskResourceVec(const std::vector<index_base::MergeTaskResourcePtr>& resVec)
    {
        mMergeTaskResourceVec = resVec;
    }

    index_base::MergeTaskResourceManagerPtr CreateMergeTaskResourceManager() const;

    uint32_t GetMergeTaskItemsSize() const { return mMergeTaskItemsVec.size(); }
    const MergeTaskItems& GetMergeTaskItems(uint32_t instanceId) const
    {
        assert(instanceId < mMergeTaskItemsVec.size());
        return mMergeTaskItemsVec[instanceId];
    }
    const std::vector<MergePlan>& GetMergePlans() const { return mMergePlans; }
    void SetMergePlans(const std::vector<MergePlan>& mergePlanMetas) { mMergePlans = mergePlanMetas; }
    void AddMergePlan(const MergePlan& mergePlan) { mMergePlans.push_back(mergePlan); }

    bool NeedMerge() const { return mMergePlans.size() != 0; }

    void AddMergePlanResource(const index::ReclaimMapPtr& reclaimMap, const index::ReclaimMapPtr& subReclaimMap,
                              const index::legacy::BucketMaps& bucketMaps)
    {
        mMergePlanResources.push_back(MergePlanResource(reclaimMap, subReclaimMap, bucketMaps));
    }

    std::vector<index::ReclaimMapPtr> GetReclaimMapVec() const;

    std::vector<index::ReclaimMapPtr> GetSubPartReclaimMapVec() const;

    const index::ReclaimMapPtr& GetReclaimMap(size_t idx) const { return mMergePlanResources[idx].reclaimMap; }
    const index::ReclaimMapPtr& GetSubReclaimMap(size_t idx) const { return mMergePlanResources[idx].subReclaimMap; }

    const index::legacy::BucketMaps& GetBucketMaps(size_t idx) const { return mMergePlanResources[idx].bucketMaps; }

    const index_base::SegmentMergeInfos& GetMergeSegmentInfo(size_t idx) const
    {
        assert(idx < mMergePlans.size());
        return mMergePlans[idx].GetSegmentMergeInfos();
    }

    void Resize(uint32_t mergePlanCount)
    {
        mMergePlans.resize(mergePlanCount);
        mMergePlanResources.resize(mergePlanCount);
    }

    std::vector<MergePlan>& GetMergePlans() { return mMergePlans; }

    MergePlan& GetMergePlan(size_t idx)
    {
        assert(idx < mMergePlans.size());
        return mMergePlans[idx];
    }

    const merger::MergePlan& GetMergePlan(size_t idx) const
    {
        assert(idx < mMergePlans.size());
        return mMergePlans[idx];
    }

    MergePlanResource& GetMergePlanResouce(size_t idx)
    {
        assert(idx < mMergePlanResources.size());
        return mMergePlanResources[idx];
    }

    void SetTimestamp(int64_t ts) { mMergeTimestamp = ts; }
    int64_t GetTimestamp() const { return mMergeTimestamp; }

    int64_t EstimateMemoryUse() const override;
    void SetCounterMap(const std::string& counterMapContent) { mCounterMapContent = counterMapContent; }
    std::string GetCounterMapContent() const { return mCounterMapContent; }

public:
    void Store(const std::string& mergeMetaPath, file_system::FenceContext* fenceContext) const final override;
    bool Load(const std::string& mergeMetaPath) override;
    bool LoadBasicInfo(const std::string& mergeMetaPath) override;
    size_t GetMergePlanCount() const override { return mMergePlans.size(); }

    std::vector<segmentid_t> GetTargetSegmentIds(size_t planIdx) const override
    {
        const auto& plan = mMergePlans[planIdx];
        std::vector<segmentid_t> ret;
        ret.reserve(plan.GetTargetSegmentCount());
        for (size_t i = 0; i < plan.GetTargetSegmentCount(); ++i) {
            ret.push_back(plan.GetTargetSegmentId(i));
        }
        return ret;
    }

    void TEST_AddMergePlanMeta(MergePlan plan, segmentid_t targetSegmentId,
                               const index_base::SegmentInfo& targetSegmentInfo,
                               const index_base::SegmentInfo& subTargetSegmentInfo,
                               const index_base::SegmentMergeInfos& segMergeInfos,
                               const index_base::SegmentMergeInfos& subSegMergeInfos)
    {
        plan.ClearTargetSegments();
        plan.AddTargetSegment();
        plan.SetTargetSegmentId(0, targetSegmentId);
        plan.SetTargetSegmentInfo(0, targetSegmentInfo);
        plan.SetSubTargetSegmentInfo(0, subTargetSegmentInfo);
        plan.SetSubSegmentMergeInfos(subSegMergeInfos);
        plan.TEST_SetSegmentMergeInfos(segMergeInfos);
        plan.TEST_SetSubSegmentMergeInfos(subSegMergeInfos);
        mMergePlans.push_back(plan);
    }

protected:
    virtual void InnerStore(const std::string& mergeMetaPath) const;
    void StoreMergeMetaVersion(const file_system::DirectoryPtr& directory) const;
    void StoreTargetVersion(const file_system::DirectoryPtr& directory) const;
    void StoreMergeTaskItems(const file_system::DirectoryPtr& directory) const;
    void StoreMergeTaskResourceMeta(const file_system::DirectoryPtr& directory) const;

    bool LoadMergeTaskItems(const file_system::DirectoryPtr& directory);
    void LoadMergeTaskResourceMeta(const file_system::DirectoryPtr& directory);
    bool Load(const std::string& rootPath, bool loadFullMeta);
    bool LoadMergeMetaVersion(const file_system::DirectoryPtr& directory, int64_t& mergeMetaBinaryVersion);

    file_system::IFileSystemPtr CreateFileSystem(const std::string& rootPath, bool isOverride = false) const;

private:
    static void StoreBucketMaps(const file_system::DirectoryPtr& directory,
                                const index::legacy::BucketMaps& bucketMaps);
    static bool LoadBucketMaps(const file_system::DirectoryPtr& directory, index::legacy::BucketMaps& bucketMaps);

protected:
    std::vector<MergeTaskItems> mMergeTaskItemsVec; //  mMergeTaskItemsVec[instanceId]

    std::vector<merger::MergePlan> mMergePlans;                          //  mMergePlan[mergePlanIdx]
    std::vector<MergePlanResource> mMergePlanResources;                  //  mMergePlanResources[mergePlanIdx]
    std::vector<index_base::MergeTaskResourcePtr> mMergeTaskResourceVec; //  mMergeTaskResourceVec[resourceId]

    int64_t mMergeTimestamp;
    std::string mCounterMapContent;
    bool mIsPackMeta;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexMergeMeta);
}} // namespace indexlib::merger
