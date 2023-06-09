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
#include "indexlib/index_base/segment/online_segment_directory.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/LinkDirectory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/lifecycle_strategy.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/segment/join_segment_directory.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::config;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, OnlineSegmentDirectory);

OnlineSegmentDirectory::OnlineSegmentDirectory(const OnlineSegmentDirectory& other)
    : SegmentDirectory(other)
    , mEnableRecover(other.mEnableRecover)
{
    // TODO: test clone
    mRtSegDir.reset(other.mRtSegDir->Clone());
    mJoinSegDir.reset(other.mJoinSegDir->Clone());
}

OnlineSegmentDirectory::~OnlineSegmentDirectory() {}

bool OnlineSegmentDirectory::IsIncSegmentId(segmentid_t segId)
{
    return !(RealtimeSegmentDirectory::IsRtSegmentId(segId) || JoinSegmentDirectory::IsJoinSegmentId(segId));
}

void OnlineSegmentDirectory::DoInit(const file_system::DirectoryPtr& directory)
{
    mJoinSegDir.reset(new JoinSegmentDirectory);
    if (!directory->IsExist(JOIN_INDEX_PARTITION_DIR_NAME)) {
        directory->MakeDirectory(JOIN_INDEX_PARTITION_DIR_NAME, DirectoryOption::Mem());
    }
    Version invalidVersion;
    invalidVersion.SetFormatVersion(mOnDiskVersion.GetFormatVersion());
    invalidVersion.SetSchemaVersionId(mOnDiskVersion.GetSchemaVersionId());
    mJoinSegDir->Init(directory->GetDirectory(JOIN_INDEX_PARTITION_DIR_NAME, true), invalidVersion);
    MergeVersion(mJoinSegDir->GetVersion());

    if (!directory->IsExist(RT_INDEX_PARTITION_DIR_NAME)) {
        directory->MakeDirectory(RT_INDEX_PARTITION_DIR_NAME, DirectoryOption::Mem());
        mRtSegDir.reset(new RealtimeSegmentDirectory(false));
    } else {
        mRtSegDir.reset(new RealtimeSegmentDirectory(mEnableRecover));
    }
    mRtSegDir->Init(directory->GetDirectory(RT_INDEX_PARTITION_DIR_NAME, true), invalidVersion);
    MergeVersion(mRtSegDir->GetVersion());
    mLifecycleStrategy = InitLifecycleStrategy();
}

void OnlineSegmentDirectory::DoReopen() { RefreshVersion(); }

void OnlineSegmentDirectory::MergeVersion(const index_base::Version& version)
{
    for (size_t i = 0; i < version.GetSegmentCount(); ++i) {
        mVersion.AddSegment(version[i]);
    }

    for (size_t i = 0; i < version.GetSegmentCount(); ++i) {
        auto segId = version[i];
        SegmentTemperatureMeta meta;
        if (version.GetSegmentTemperatureMeta(segId, meta)) {
            mVersion.AddSegTemperatureMeta(meta);
        }
        indexlibv2::framework::SegmentStatistics segStats;
        if (version.GetSegmentStatistics(segId, &segStats)) {
            mVersion.AddSegmentStatistics(segStats);
        }
    }

    if (version.GetTimestamp() > mVersion.GetTimestamp()) {
        mVersion.SetTimestamp(version.GetTimestamp());
    }
    const document::Locator& locator = version.GetLocator();
    if (locator.IsValid()) {
        mVersion.SetLocator(locator);
    }
}

SegmentDirectory* OnlineSegmentDirectory::Clone()
{
    ScopedLock scopedLock(mLock);
    return new OnlineSegmentDirectory(*this);
}

file_system::DirectoryPtr OnlineSegmentDirectory::GetSegmentParentDirectory(segmentid_t segId) const
{
    autil::ScopedLock scopedLock(mLock);
    SegmentDirectoryPtr segDir = GetSegmentDirectory(segId);
    if (segDir) {
        return segDir->GetRootDirectory();
    }
    return GetRootDirectory();
}

void OnlineSegmentDirectory::AddSegment(segmentid_t segId, timestamp_t ts)
{
    ScopedLock scopedLock(mLock);
    SegmentDirectoryPtr segDir = GetSegmentDirectory(segId);
    if (!segDir) {
        INDEXLIB_FATAL_ERROR(UnSupported, "not support to add on disk segment");
    }
    segDir->AddSegment(segId, ts);
    RefreshVersion();
    InitSegmentDatas(mVersion);
}

void OnlineSegmentDirectory::AddSegment(segmentid_t segId, timestamp_t ts,
                                        const std::shared_ptr<indexlibv2::framework::SegmentStatistics>& segmentStats)
{
    ScopedLock scopedLock(mLock);
    SegmentDirectoryPtr segDir = GetSegmentDirectory(segId);
    if (!segDir) {
        INDEXLIB_FATAL_ERROR(UnSupported, "not support to add on disk segment");
    }
    segDir->AddSegment(segId, ts, segmentStats);
    RefreshVersion();
    InitSegmentDatas(mVersion);
}

void OnlineSegmentDirectory::RefreshSegmentLifecycle(bool isNewSegment, SegmentData* segmentData)
{
    assert(segmentData != nullptr);
    auto segId = segmentData->GetSegmentId();
    auto segDir = segmentData->GetDirectory();
    auto currentLifecycle =
        (mLifecycleStrategy == nullptr) ? "" : mLifecycleStrategy->GetSegmentLifecycle(mVersion, segmentData);

    if (isNewSegment &&
        (RealtimeSegmentDirectory::IsRtSegmentId(segId) || JoinSegmentDirectory::IsJoinSegmentId(segId))) {
        segmentData->SetLifecycle(SegmentData::LIFECYCLE_IN_MEM);
    }
    if (segmentData->GetLifecycle() == SegmentData::LIFECYCLE_IN_MEM) {
        if (mLifecycleStrategy != nullptr) {
            segDir->SetLifecycle(currentLifecycle);
        }
        if (CheckSegmentInfoFlushed(segDir)) {
            IE_LOG(INFO, "detected: segment[%s] is flushed, update Lifecycle to [%s]", segDir->GetLogicalPath().c_str(),
                   currentLifecycle.c_str());
            segmentData->SetLifecycle(currentLifecycle);
        }
        return;
    }
    if (currentLifecycle != segmentData->GetLifecycle()) {
        IE_LOG(INFO, "detected: segment[%s] lifecycle updated, pre lifecycle=[%s], current lifecycle=[%s]",
               segDir->GetLogicalPath().c_str(), segmentData->GetLifecycle().c_str(), currentLifecycle.c_str());
        segmentData->SetLifecycle(currentLifecycle);
        segDir->SetLifecycle(currentLifecycle);
    }
}

bool OnlineSegmentDirectory::CheckSegmentInfoFlushed(const DirectoryPtr& segmentDir)
{
    std::string segInfoFilePath = segmentDir->GetPhysicalPath(SEGMENT_INFO_FILE_NAME);
    auto ret = FslibWrapper::IsExist(segInfoFilePath);
    if (ret.ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "check file IsExist [%s] failed, ec [%d]", segInfoFilePath.c_str(), ret.ec);
        return false;
    }
    return ret.result;
}

void OnlineSegmentDirectory::RemoveSegments(const vector<segmentid_t>& segIds)
{
    ScopedLock scopedLock(mLock);
    vector<segmentid_t> rtSegIds;
    vector<segmentid_t> joinSegIds;

    for (size_t i = 0; i < segIds.size(); i++) {
        segmentid_t segId = segIds[i];
        if (mRtSegDir->IsMatchSegmentId(segId)) {
            rtSegIds.push_back(segId);
            continue;
        }

        if (mJoinSegDir->IsMatchSegmentId(segId)) {
            joinSegIds.push_back(segId);
            continue;
        }
        INDEXLIB_FATAL_ERROR(UnSupported, "not support to remove on disk segment[%d]", segId);
    }

    mRtSegDir->RemoveSegments(rtSegIds);
    mJoinSegDir->RemoveSegments(joinSegIds);
    SegmentDirectory::RemoveSegments(segIds);
}

SegmentDirectoryPtr OnlineSegmentDirectory::GetSegmentDirectory(segmentid_t segId) const
{
    autil::ScopedLock scopedLock(mLock);
    if (mRtSegDir->IsMatchSegmentId(segId)) {
        return mRtSegDir;
    }
    if (mJoinSegDir->IsMatchSegmentId(segId)) {
        return mJoinSegDir;
    }
    return SegmentDirectoryPtr();
}

void OnlineSegmentDirectory::RefreshVersion()
{
    // on disk -> join -> realtime
    mVersion = mOnDiskVersion;
    MergeVersion(mJoinSegDir->GetVersion());
    MergeVersion(mRtSegDir->GetVersion());
}

void OnlineSegmentDirectory::CommitVersion(const config::IndexPartitionOptions& options)
{
    autil::ScopedLock scopedLock(mLock);
    if (!IsVersionChanged()) {
        return;
    }
    // mRtSegDir & mJoinSegDir will not DumpDeployIndexMeta through override CommitVersion
    mRtSegDir->CommitVersion(options);
    mJoinSegDir->CommitVersion(options);
}

void OnlineSegmentDirectory::SetSubSegmentDir()
{
    autil::ScopedLock scopedLock(mLock);
    SegmentDirectory::SetSubSegmentDir();
    mRtSegDir->SetSubSegmentDir();
    mJoinSegDir->SetSubSegmentDir();
}

BuildingSegmentData OnlineSegmentDirectory::CreateNewSegmentData(const BuildConfig& buildConfig)
{
    autil::ScopedLock scopedLock(mLock);
    BuildingSegmentData newSegmentData = mRtSegDir->CreateNewSegmentData(buildConfig);
    const SegmentDataVector& segmentDatas = GetSegmentDatas();
    SegmentDataVector::const_reverse_iterator it = segmentDatas.rbegin();
    if (it != segmentDatas.rend()) {
        SegmentInfo lastSegmentInfo = *(it->GetSegmentInfo());
        newSegmentData.SetBaseDocId(it->GetBaseDocId() + lastSegmentInfo.docCount);
    }
    return newSegmentData;
}

segmentid_t OnlineSegmentDirectory::GetLastValidRtSegmentInLinkDirectory() const
{
    autil::ScopedLock scopedLock(mLock);
    DirectoryPtr rootDir = GetRootDirectory();
    assert(rootDir);
    if (rootDir->GetRootLinkPath().empty() || mSegmentDatas.empty()) {
        return INVALID_SEGMENTID;
    }

    for (int64_t i = (int64_t)mSegmentDatas.size() - 1; i >= 0; i--) {
        const SegmentData& segData = mSegmentDatas[i];
        segmentid_t segId = segData.GetSegmentId();
        if (!RealtimeSegmentDirectory::IsRtSegmentId(segId)) {
            break;
        }
        DirectoryPtr segDir = segData.GetDirectory();
        assert(segDir);
        DirectoryPtr linkDir = segDir->CreateLinkDirectory();
        if (linkDir && linkDir->IsExist(SEGMENT_INFO_FILE_NAME)) {
            IE_LOG(DEBUG, "Get last valid rt segment [%d] in link directory", segId);
            return segId;
        }
    }
    return INVALID_SEGMENTID;
}

bool OnlineSegmentDirectory::SwitchToLinkDirectoryForRtSegments(segmentid_t lastLinkRtSegId)
{
    autil::ScopedLock scopedLock(mLock);
    DirectoryPtr rootDir = GetRootDirectory();
    assert(rootDir);
    if (rootDir->GetRootLinkPath().empty() || lastLinkRtSegId == INVALID_SEGMENTID) {
        return false;
    }

    for (size_t i = 0; i < mSegmentDatas.size(); i++) {
        SegmentData& segData = mSegmentDatas[i];
        segmentid_t segId = segData.GetSegmentId();
        if (!RealtimeSegmentDirectory::IsRtSegmentId(segId)) {
            continue;
        }

        if (segId > lastLinkRtSegId) {
            break;
        }

        DirectoryPtr segDir = segData.GetDirectory();
        assert(segDir);
        auto alreadyLinkDir = std::dynamic_pointer_cast<LinkDirectory>(segDir->GetIDirectory());
        if (alreadyLinkDir != nullptr) {
            // maybe cloned from last partition data, mDirectory is already link dir
            continue;
        }

        DirectoryPtr linkDir = segDir->CreateLinkDirectory();
        if (!linkDir || !linkDir->IsExist(SEGMENT_INFO_FILE_NAME)) {
            IE_LOG(ERROR, "switch rt segment [%d] to link directory fail! segDir[%s]", segId,
                   segDir->DebugString().c_str());
            return false;
        }
        IE_LOG(INFO, "switch rt segment [%d] to link directory!", segId);
        segData.SetDirectory(linkDir);
    }
    IE_LOG(INFO, "SwitchToLinkDirectoryForRtSegments to lastLinkRtSegmentId [%d]!", lastLinkRtSegId);
    return true;
}
}} // namespace indexlib::index_base
