#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/index_base/segment/join_segment_directory.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/link_directory.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, OnlineSegmentDirectory);

OnlineSegmentDirectory::OnlineSegmentDirectory(bool enableRecover)
    : mEnableRecover(enableRecover)
{
}

OnlineSegmentDirectory::OnlineSegmentDirectory(const OnlineSegmentDirectory& other)
    : SegmentDirectory(other)
    , mEnableRecover(other.mEnableRecover)
{
    //TODO: test clone
    mRtSegDir.reset(other.mRtSegDir->Clone());
    mJoinSegDir.reset(other.mJoinSegDir->Clone());
}

OnlineSegmentDirectory::~OnlineSegmentDirectory() 
{
}

bool OnlineSegmentDirectory::IsIncSegmentId(segmentid_t segId)
{
    return !(RealtimeSegmentDirectory::IsRtSegmentId(segId)
             || JoinSegmentDirectory::IsJoinSegmentId(segId));
}

void OnlineSegmentDirectory::DoInit(const file_system::DirectoryPtr& directory)
{
    mJoinSegDir.reset(new JoinSegmentDirectory);
    if (!directory->IsExist(JOIN_INDEX_PARTITION_DIR_NAME))
    {
        directory->MakeInMemDirectory(JOIN_INDEX_PARTITION_DIR_NAME);
    }
    Version invalidVersion;
    invalidVersion.SetFormatVersion(mOnDiskVersion.GetFormatVersion());
    invalidVersion.SetSchemaVersionId(mOnDiskVersion.GetSchemaVersionId());
    mJoinSegDir->Init(directory->GetDirectory(
                    JOIN_INDEX_PARTITION_DIR_NAME, true), invalidVersion);
    MergeVersion(mJoinSegDir->GetVersion());

    if (!directory->IsExist(RT_INDEX_PARTITION_DIR_NAME))
    {
        directory->MakeInMemDirectory(RT_INDEX_PARTITION_DIR_NAME);
        mRtSegDir.reset(new RealtimeSegmentDirectory(false));
    }
    else
    {
        mRtSegDir.reset(new RealtimeSegmentDirectory(mEnableRecover));
    }
    mRtSegDir->Init(directory->GetDirectory(
                    RT_INDEX_PARTITION_DIR_NAME, true), invalidVersion);
    MergeVersion(mRtSegDir->GetVersion());
}

void OnlineSegmentDirectory::MergeVersion(const index_base::Version& version)
{
    for (size_t i = 0; i < version.GetSegmentCount(); ++i)
    {
        mVersion.AddSegment(version[i]);
    }

    if (version.GetTimestamp() > mVersion.GetTimestamp())
    {
        mVersion.SetTimestamp(version.GetTimestamp());
    }
    const document::Locator& locator = version.GetLocator();
    if (locator.IsValid())
    {
        mVersion.SetLocator(locator);
    }
}

SegmentDirectory* OnlineSegmentDirectory::Clone()
{
    ScopedLock scopedLock(mLock);
    return new OnlineSegmentDirectory(*this);
}

file_system::DirectoryPtr OnlineSegmentDirectory::GetSegmentParentDirectory(
        segmentid_t segId) const
{
    autil::ScopedLock scopedLock(mLock);
    SegmentDirectoryPtr segDir = GetSegmentDirectory(segId);
    if (segDir)
    {
        return segDir->GetRootDirectory();
    }
    return GetRootDirectory();
}

void OnlineSegmentDirectory::AddSegment(segmentid_t segId, timestamp_t ts)
{
    ScopedLock scopedLock(mLock);
    SegmentDirectoryPtr segDir = GetSegmentDirectory(segId);
    if (!segDir)
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "not support to add on disk segment");
    }
    segDir->AddSegment(segId, ts);
    RefreshVersion();
    InitSegmentDatas(mVersion);
}

void OnlineSegmentDirectory::RemoveSegments(const vector<segmentid_t>& segIds)
{
    ScopedLock scopedLock(mLock);
    vector<segmentid_t> rtSegIds;
    vector<segmentid_t> joinSegIds;

    for (size_t i = 0; i < segIds.size(); i++)
    {
        segmentid_t segId = segIds[i];
        if (mRtSegDir->IsMatchSegmentId(segId))
        {
            rtSegIds.push_back(segId);
            continue;
        }

        if (mJoinSegDir->IsMatchSegmentId(segId))
        {
            joinSegIds.push_back(segId);
            continue;
        }
        INDEXLIB_FATAL_ERROR(UnSupported,
                             "not support to remove on disk segment[%d]", segId);
    }

    mRtSegDir->RemoveSegments(rtSegIds);
    mJoinSegDir->RemoveSegments(joinSegIds);
    SegmentDirectory::RemoveSegments(segIds);
}

SegmentDirectoryPtr OnlineSegmentDirectory::GetSegmentDirectory(
        segmentid_t segId) const
{
    autil::ScopedLock scopedLock(mLock);
    if (mRtSegDir->IsMatchSegmentId(segId))
    {
        return mRtSegDir;
    }
    if (mJoinSegDir->IsMatchSegmentId(segId))
    {
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

void OnlineSegmentDirectory::CommitVersion()
{
    autil::ScopedLock scopedLock(mLock);
    if (!IsVersionChanged())
    {
        return;
    }
    // mRtSegDir & mJoinSegDir will not DumpDeployIndexMeta through override CommitVersion
    mRtSegDir->CommitVersion();
    mJoinSegDir->CommitVersion();
}

void OnlineSegmentDirectory::SetSubSegmentDir()
{
    autil::ScopedLock scopedLock(mLock);
    SegmentDirectory::SetSubSegmentDir();
    mRtSegDir->SetSubSegmentDir();
    mJoinSegDir->SetSubSegmentDir();
}

BuildingSegmentData OnlineSegmentDirectory::CreateNewSegmentData(
        const BuildConfig& buildConfig)
{
    autil::ScopedLock scopedLock(mLock);
    BuildingSegmentData newSegmentData = mRtSegDir->CreateNewSegmentData(buildConfig);
    const SegmentDataVector& segmentDatas = GetSegmentDatas();
    SegmentDataVector::const_reverse_iterator it = segmentDatas.rbegin();
    if (it != segmentDatas.rend())
    {
        SegmentInfo lastSegmentInfo = it->GetSegmentInfo();
        newSegmentData.SetBaseDocId(
                it->GetBaseDocId() + lastSegmentInfo.docCount);
    }
    return newSegmentData;
}

segmentid_t OnlineSegmentDirectory::GetLastValidRtSegmentInLinkDirectory() const
{
    autil::ScopedLock scopedLock(mLock);
    DirectoryPtr rootDir = GetRootDirectory();
    assert(rootDir);
    if (!rootDir->GetFileSystem()->UseRootLink() || mSegmentDatas.empty())
    {
        return INVALID_SEGMENTID;
    }
    
    for (int64_t i = (int64_t)mSegmentDatas.size() - 1; i >= 0; i--)
    {
        const SegmentData& segData = mSegmentDatas[i];
        segmentid_t segId = segData.GetSegmentId();
        if (!RealtimeSegmentDirectory::IsRtSegmentId(segId))
        {
            break;
        }
        DirectoryPtr segDir = segData.GetDirectory();
        assert(segDir);
        LinkDirectoryPtr linkDir = segDir->CreateLinkDirectory();
        if (linkDir && linkDir->IsExist(SEGMENT_INFO_FILE_NAME))
        {
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
    if (!rootDir->GetFileSystem()->UseRootLink() || lastLinkRtSegId == INVALID_SEGMENTID)
    {
        return false;
    }

    for (size_t i = 0; i < mSegmentDatas.size(); i++)
    {
        SegmentData& segData = mSegmentDatas[i];
        segmentid_t segId = segData.GetSegmentId();
        if (!RealtimeSegmentDirectory::IsRtSegmentId(segId))
        {
            continue;
        }

        if (segId > lastLinkRtSegId)
        {
            break;
        }

        DirectoryPtr segDir = segData.GetDirectory();
        assert(segDir);
        LinkDirectoryPtr linkDir = segDir->CreateLinkDirectory();
        if (!linkDir || !linkDir->IsExist(SEGMENT_INFO_FILE_NAME))
        {
            IE_LOG(ERROR, "switch rt segment [%d] to link directory[%s] fail!",
                   segId, linkDir->GetPath().c_str());
            return false;
        }

        linkDir->MountPackageFile(PACKAGE_FILE_PREFIX);
        IE_LOG(DEBUG, "switch rt segment [%d] to link directory!", segId);
        segData.SetDirectory(linkDir);
    }
    IE_LOG(INFO, "SwitchToLinkDirectoryForRtSegments to lastLinkRtSegmentId [%d]!",
           lastLinkRtSegId);
    return true;
}

IE_NAMESPACE_END(index_base);

