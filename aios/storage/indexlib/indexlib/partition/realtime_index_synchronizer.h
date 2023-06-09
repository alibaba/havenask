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
#ifndef __INDEXLIB_REALTIME_INDEX_SYNCHRONIZER_H
#define __INDEXLIB_REALTIME_INDEX_SYNCHRONIZER_H

#include <memory>

#include "autil/Lock.h"
#include "autil/ThreadPool.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"

namespace indexlib { namespace partition {

class RealtimeIndexSynchronizer
{
public:
    const static size_t DEFAULT_SYNC_THREAD_COUNT;
    const static std::string SYNC_INC_VERSION_TIMESTAMP;
    enum SyncMode { READ_ONLY, READ_WRITE };

public:
    RealtimeIndexSynchronizer(SyncMode mode);
    ~RealtimeIndexSynchronizer();

public:
    bool Init(const std::string& remoteRtDir, util::PartitionMemoryQuotaControllerPtr partitionMemController);
    bool SyncFromRemote(const file_system::DirectoryPtr& localRtDir, const index_base::Version& incVersion);
    bool SyncToRemote(const index_base::PartitionDataPtr& localPartitionData, autil::ThreadPoolPtr& syncThreadPool);

private:
    void TrimObsoleteSegments(const index_base::Version& incVersion, const OnDiskPartitionDataPtr& partitionData,
                              index_base::Version* version, bool isRemote);
    void TrimNotMatchSegments(const OnDiskPartitionDataPtr& localPartitionData,
                              const OnDiskPartitionDataPtr& remotePartitionData,
                              const index_base::Version& remoteVersion, index_base::Version* localVersion);
    bool SegmentEqual(const index_base::SegmentData& leftSegData, const index_base::SegmentData& rightSegData) const;
    void PullDiffSegments(const OnDiskPartitionDataPtr& remotePartitionData, const index_base::Version& remoteVersion,
                          const index_base::Version& localVersion, const file_system::DirectoryPtr& localRtDir);
    void SyncSegment(const autil::ThreadPoolPtr& threadPool, const index_base::SegmentData& segmentData,
                     const file_system::DirectoryPtr& targetDir);
    OnDiskPartitionDataPtr GetRemotePartitionData(bool cleanUseless);
    bool CheckLocalPartitionData(const index_base::PartitionDataPtr& localPartitionData);
    file_system::IFileSystemPtr GetRemoteFileSystem();
    void Reset();
    bool CanSync() const;
    void NotifySyncError();
    void ResetSyncError();

private:
    std::string mRemoteRtDir;
    util::PartitionMemoryQuotaControllerPtr mPartitionMemoryQuotaController;

    file_system::IFileSystemPtr mRemoteFileSystem;
    OnDiskPartitionDataPtr mRemotePartitionData;
    index_base::Version mLastSyncVersion;
    segmentid_t mLastSyncSegment;
    SyncMode mSyncMode;
    mutable autil::ThreadMutex mLock;

    int64_t mLastSyncFailTimestamp; // unit: second
    int64_t mCurrentRetryTimes;
    bool mEnableSleep;
    bool mRemoveOldIndex;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RealtimeIndexSynchronizer);
}} // namespace indexlib::partition

#endif //__INDEXLIB_REALTIME_INDEX_SYNCHRONIZER_H
