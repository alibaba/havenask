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
#ifndef __INDEXLIB_ONLINE_SEGMENT_DIRECTORY_H
#define __INDEXLIB_ONLINE_SEGMENT_DIRECTORY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/LifecycleConfig.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index_base {

class LifecycleStrategy;

class OnlineSegmentDirectory : public SegmentDirectory
{
public:
    OnlineSegmentDirectory(bool enableRecover = false,
                           std::shared_ptr<file_system::LifecycleConfig> lifecycleConfig = nullptr)
        : SegmentDirectory(lifecycleConfig)
        , mEnableRecover(enableRecover)
    {
    }

    OnlineSegmentDirectory(const OnlineSegmentDirectory& other);
    ~OnlineSegmentDirectory();

public:
    SegmentDirectory* Clone() override;

    void AddSegment(segmentid_t segId, timestamp_t ts) override;
    void AddSegment(segmentid_t segId, timestamp_t ts,
                    const std::shared_ptr<indexlibv2::framework::SegmentStatistics>& segmentStats) override;

    void RemoveSegments(const std::vector<segmentid_t>& segIds) override;

    void IncLastSegmentId() override
    {
        autil::ScopedLock scopedLock(mLock);
        mRtSegDir->IncLastSegmentId();
    };

    bool IsVersionChanged() const override
    {
        autil::ScopedLock scopedLock(mLock);
        return mRtSegDir->IsVersionChanged() || mJoinSegDir->IsVersionChanged();
    }

    void CommitVersion(const config::IndexPartitionOptions& options) override;

    file_system::DirectoryPtr GetSegmentParentDirectory(segmentid_t segId) const override;

    void SetSubSegmentDir() override;

    BuildingSegmentData CreateNewSegmentData(const config::BuildConfig& buildConfig) override;

    segmentid_t CreateNewSegmentId() override
    {
        autil::ScopedLock scopedLock(mLock);
        return mRtSegDir->CreateNewSegmentId();
    }

    segmentid_t GetLastValidRtSegmentInLinkDirectory() const override;
    bool SwitchToLinkDirectoryForRtSegments(segmentid_t lastLinkRtSegId) override;

    std::string GetLastLocator() const override
    {
        autil::ScopedLock scopedLock(mLock);
        return mRtSegDir->GetLastLocator();
    }

    virtual segmentid_t CreateJoinSegmentId()
    {
        autil::ScopedLock scopedLock(mLock);
        return mJoinSegDir->CreateNewSegmentId();
    }

    const SegmentDirectoryPtr& GetRtSegmentDirectory() const
    {
        autil::ScopedLock scopedLock(mLock);
        return mRtSegDir;
    }

    const SegmentDirectoryPtr& GetJoinSegmentDirectory() const
    {
        autil::ScopedLock scopedLock(mLock);
        return mJoinSegDir;
    }

    static bool IsIncSegmentId(segmentid_t segId);
    static bool CheckSegmentInfoFlushed(const file_system::DirectoryPtr& segmentDir);

protected:
    void DoInit(const file_system::DirectoryPtr& directory) override;
    void DoReopen() override;
    void RefreshSegmentLifecycle(bool isNewSegment, SegmentData* segmentData) override;

    SegmentDirectoryPtr GetSegmentDirectory(segmentid_t segId) const;
    void MergeVersion(const index_base::Version& version);
    virtual void RefreshVersion();

private:
    bool mEnableRecover;
    SegmentDirectoryPtr mRtSegDir;
    SegmentDirectoryPtr mJoinSegDir;

private:
    friend class OnlineSegmentDirectoryTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnlineSegmentDirectory);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_ONLINE_SEGMENT_DIRECTORY_H
