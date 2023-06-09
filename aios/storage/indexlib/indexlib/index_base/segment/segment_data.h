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
#ifndef __INDEXLIB_SEGMENT_DATA_H
#define __INDEXLIB_SEGMENT_DATA_H

#include <memory>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/segment_data_base.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, PartitionPatchIndexAccessor);

namespace indexlib { namespace index_base {

class SegmentData;
DEFINE_SHARED_PTR(SegmentData);

class SegmentData : public SegmentDataBase
{
public:
    SegmentData();
    SegmentData(const SegmentData& segData);
    virtual ~SegmentData();

public:
    static const std::string LIFECYCLE_IN_MEM;
    static bool CompareToSegmentId(const SegmentData& lhs, const segmentid_t segId)
    {
        return lhs.GetSegmentId() < segId;
    }

public:
    void SetSegmentInfo(const SegmentInfo& segmentInfo) { mSegmentInfo.reset(new SegmentInfo(segmentInfo)); }
    const std::shared_ptr<const SegmentInfo>& GetSegmentInfo() const { return mSegmentInfo; }
    void SetSegmentMetrics(const indexlib::framework::SegmentMetrics& segmentMetrics)
    {
        mSegmentMetrics.reset(new indexlib::framework::SegmentMetrics(segmentMetrics));
    }
    void SetSegmentMetrics(indexlib::framework::SegmentMetrics&& segmentMetrics)
    {
        mSegmentMetrics.reset(new indexlib::framework::SegmentMetrics(std::move(segmentMetrics)));
    }
    const std::shared_ptr<const indexlib::framework::SegmentMetrics>& GetSegmentMetrics() const
    {
        return mSegmentMetrics;
    }

    void SetPatchIndexAccessor(const PartitionPatchIndexAccessorPtr& patchAccessor) { mPatchAccessor = patchAccessor; }

    const PartitionPatchIndexAccessorPtr& GetPatchIndexAccessor() const { return mPatchAccessor; }

    file_system::DirectoryPtr GetSummaryDirectory(bool throwExceptionIfNotExist) const;
    file_system::DirectoryPtr GetSourceDirectory(bool throwExceptionIfNotExist) const;

    file_system::DirectoryPtr GetIndexDirectory(const std::string& indexName, bool throwExceptionIfNotExist) const;

    file_system::DirectoryPtr GetSectionAttributeDirectory(const std::string& indexName,
                                                           bool throwExceptionIfNotExist) const;

    file_system::DirectoryPtr GetAttributeDirectory(const std::string& attrName, bool throwExceptionIfNotExist) const;
    file_system::DirectoryPtr GetOperationDirectory(bool throwExceptionIfNotExist) const;

    void SetSubSegmentData(const SegmentDataPtr& segmentData) { mSubSegmentData = segmentData; }
    const SegmentDataPtr& GetSubSegmentData() const { return mSubSegmentData; }

    SegmentData& operator=(const SegmentData& segData);

    std::string GetShardingDirInSegment(uint32_t shardingIdx) const;
    SegmentData CreateShardingSegmentData(uint32_t shardingIdx) const;

    uint32_t GetShardId() const { return mSegmentInfo->shardId; }

    virtual bool IsBuildingSegment() const { return false; }

    void SetDirectory(const file_system::DirectoryPtr& directory);

    const file_system::DirectoryPtr& GetDirectory() const { return mDirectory; }

    std::string GetOriginalTemperature() const;

private:
    file_system::DirectoryPtr GetIndexDirectory(bool throwExceptionIfNotExist) const;
    file_system::DirectoryPtr GetAttributeDirectory(bool throwExceptionIfNotExist) const;

protected:
    std::shared_ptr<const SegmentInfo> mSegmentInfo;
    std::shared_ptr<const indexlib::framework::SegmentMetrics> mSegmentMetrics;
    file_system::DirectoryPtr mDirectory;

private:
    mutable file_system::DirectoryPtr mIndexDirectory;
    mutable file_system::DirectoryPtr mAttrDirectory;
    mutable file_system::DirectoryPtr mSummaryDirectory;
    mutable file_system::DirectoryPtr mSourceDirectory;
    mutable file_system::DirectoryPtr mOperationDirectory;
    mutable autil::ThreadMutex mLock;

protected:
    SegmentDataPtr mSubSegmentData;
    PartitionPatchIndexAccessorPtr mPatchAccessor;
    std::string mLifecycle;

private:
    IE_LOG_DECLARE();
};

typedef std::vector<SegmentData> SegmentDataVector;
}} // namespace indexlib::index_base

#endif //__INDEXLIB_SEGMENT_DATA_H
