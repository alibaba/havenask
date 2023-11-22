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
#include "indexlib/index_base/segment/parallel_offline_segment_directory.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/offline_recover_strategy.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::document;
using namespace indexlib::util;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, ParallelOfflineSegmentDirectory);

ParallelOfflineSegmentDirectory::ParallelOfflineSegmentDirectory(bool enableRecover)
    : OfflineSegmentDirectory(enableRecover)
    , mStartBuildingSegId(INVALID_SEGMENTID)
{
}

ParallelOfflineSegmentDirectory::ParallelOfflineSegmentDirectory(const ParallelOfflineSegmentDirectory& other)
    : OfflineSegmentDirectory(other)
    , mParallelBuildInfo(other.mParallelBuildInfo)
    , mStartBuildingSegId(other.mStartBuildingSegId)
{
}

ParallelOfflineSegmentDirectory::~ParallelOfflineSegmentDirectory() {}

void ParallelOfflineSegmentDirectory::DoInit(const file_system::DirectoryPtr& directory)
{
    mParallelBuildInfo.Load(directory);
    if (mOnDiskVersion.GetVersionId() != mParallelBuildInfo.GetBaseVersion()) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "base version not match: ondisk version [%d], parallel build info [%d]",
                             mVersion.GetVersionId(), mParallelBuildInfo.GetBaseVersion());
    }

    if (mOnDiskVersion.GetLastSegment() != INVALID_SEGMENTID) {
        mStartBuildingSegId = mOnDiskVersion.GetLastSegment() + 1 + mParallelBuildInfo.instanceId;
    } else {
        mStartBuildingSegId = mParallelBuildInfo.instanceId;
    }

    if (!mIsSubSegDir && mEnableRecover) {
        // only recover instance path segments
        // string instanceRootPath = directory->GetOutputPath();
        Version instanceVersion;
        VersionLoader::GetVersion(directory, instanceVersion, INVALID_VERSIONID);
        if (instanceVersion.GetVersionId() != INVALID_VERSIONID) {
            if (instanceVersion.GetVersionId() < mParallelBuildInfo.GetBaseVersion()) {
                INDEXLIB_FATAL_ERROR(InconsistentState,
                                     "instance version id [%d] should not be less than base version [%d]",
                                     instanceVersion.GetVersionId(), mParallelBuildInfo.GetBaseVersion());
            }
            mOnDiskVersion = instanceVersion;
            mVersion = instanceVersion;
        }
        mVersion = OfflineRecoverStrategy::Recover(mVersion, directory);
    }
}

segmentid_t ParallelOfflineSegmentDirectory::CreateNewSegmentId()
{
    return GetNextSegmentId(mVersion.GetLastSegment());
}

segmentid_t ParallelOfflineSegmentDirectory::GetNextSegmentId(segmentid_t baseSegmentId) const
{
    segmentid_t newSegmentId =
        baseSegmentId >= mStartBuildingSegId ? baseSegmentId + mParallelBuildInfo.parallelNum : mStartBuildingSegId;
    return newSegmentId;
}

ParallelOfflineSegmentDirectory* ParallelOfflineSegmentDirectory::Clone()
{
    return new ParallelOfflineSegmentDirectory(*this);
}

void ParallelOfflineSegmentDirectory::UpdateCounterMap(const CounterMapPtr& counterMap) const
{
    if (1 == mParallelBuildInfo.parallelNum) {
        SegmentDirectory::UpdateCounterMap(counterMap);
    }
    // Reset counter into zero when parallel number of incremental build is
    // bigger than one, and add them together when BEGIN MERGE.
    // So do nothing at this.
}

Version ParallelOfflineSegmentDirectory::GetLatestVersion(const DirectoryPtr& directory,
                                                          const Version& emptyVersion) const
{
    ParallelBuildInfo parallelBuildInfo;
    parallelBuildInfo.Load(directory);

    if (parallelBuildInfo.GetBaseVersion() == INVALID_VERSIONID) {
        return emptyVersion;
    }

    Version version;
    VersionLoader::GetVersion(directory, version, parallelBuildInfo.GetBaseVersion());
    return version;
}

void ParallelOfflineSegmentDirectory::CommitVersion(const config::IndexPartitionOptions& options)
{
    return DoCommitVersion(options, false);
}
}} // namespace indexlib::index_base
