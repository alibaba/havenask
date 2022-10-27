#include "indexlib/index_base/segment/parallel_offline_segment_directory.h"
#include "indexlib/index_base/offline_recover_strategy.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/file_system/directory.h"

using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, ParallelOfflineSegmentDirectory);

ParallelOfflineSegmentDirectory::ParallelOfflineSegmentDirectory(bool enableRecover)
    : OfflineSegmentDirectory(enableRecover)
    , mStartBuildingSegId(INVALID_SEGMENTID)
{
}

ParallelOfflineSegmentDirectory::ParallelOfflineSegmentDirectory(
        const ParallelOfflineSegmentDirectory& other)
    : OfflineSegmentDirectory(other)
    , mParallelBuildInfo(other.mParallelBuildInfo)
    , mStartBuildingSegId(other.mStartBuildingSegId)
{
}

ParallelOfflineSegmentDirectory::~ParallelOfflineSegmentDirectory()
{
}

void ParallelOfflineSegmentDirectory::DoInit(const file_system::DirectoryPtr& directory)
{
    mParallelBuildInfo.Load(mRootDir);
    if (mOnDiskVersion.GetVersionId() != mParallelBuildInfo.GetBaseVersion())
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "base version not match: ondisk version [%d], parallel build info [%d]",
                             mVersion.GetVersionId(), mParallelBuildInfo.GetBaseVersion());
    }

    if (mOnDiskVersion.GetLastSegment() != INVALID_SEGMENTID)
    {
        mStartBuildingSegId = mOnDiskVersion.GetLastSegment() + 1 + mParallelBuildInfo.instanceId;
    }
    else
    {
        mStartBuildingSegId = mParallelBuildInfo.instanceId;
    }

    if (!mIsSubSegDir && mEnableRecover)
    {
        // only recover instance path segments
        string instanceRootPath = directory->GetPath();
        file_system::DirectoryPtr instanceRootDir = DirectoryCreator::Create(instanceRootPath);

        Version instanceVersion;
        VersionLoader::GetVersion(instanceRootDir, instanceVersion, INVALID_VERSION);
        if (instanceVersion.GetVersionId() != INVALID_VERSION)
        {
            if (instanceVersion.GetVersionId() <= mParallelBuildInfo.GetBaseVersion())
            {
                INDEXLIB_FATAL_ERROR(InconsistentState,
                        "instance version id [%d] should be greater than base version [%d]",
                        instanceVersion.GetVersionId(),
                        mParallelBuildInfo.GetBaseVersion());
            }
            mOnDiskVersion = instanceVersion;
            mVersion = instanceVersion;
        }
        mVersion = OfflineRecoverStrategy::Recover(mVersion, instanceRootDir);
    }
}

segmentid_t ParallelOfflineSegmentDirectory::CreateNewSegmentId()
{
    segmentid_t newSegmentId =
        mVersion.GetLastSegment() >= mStartBuildingSegId ?
        mVersion.GetLastSegment() + mParallelBuildInfo.parallelNum : mStartBuildingSegId;
    return newSegmentId;
}

ParallelOfflineSegmentDirectory* ParallelOfflineSegmentDirectory::Clone()
{
    return new ParallelOfflineSegmentDirectory(*this);
}

void ParallelOfflineSegmentDirectory::UpdateCounterMap(
        const CounterMapPtr& counterMap) const
{
    if (1 == mParallelBuildInfo.parallelNum)
    {
        SegmentDirectory::UpdateCounterMap(counterMap);
    }
    // Reset counter into zero when parallel number of incremental build is
    // bigger than one, and add them together when BEGIN MERGE.
    // So do nothing at this.
}


Version ParallelOfflineSegmentDirectory::GetLatestVersion(
        const DirectoryPtr& directory, const Version& emptyVersion) const
{
    ParallelBuildInfo parallelBuildInfo;
    parallelBuildInfo.Load(directory->GetPath());

    if (parallelBuildInfo.GetBaseVersion() == INVALID_VERSION)
    {
        return emptyVersion;
    }

    Version version;
    VersionLoader::GetVersion(directory, version, parallelBuildInfo.GetBaseVersion());
    return version;
}

IE_NAMESPACE_END(index_base);

