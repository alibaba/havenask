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
#include "indexlib/partition/realtime_index_synchronizer.h"

#include "autil/TimeUtility.h"
#include "beeper/beeper.h"
#include "indexlib/common/file_system_factory.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/FileBlockCache.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/offline_recover_strategy.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/segment/segment_sync_item.h"
#include "indexlib/util/PathUtil.h"

using namespace std;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, RealtimeIndexSynchronizer);

const size_t RealtimeIndexSynchronizer::DEFAULT_SYNC_THREAD_COUNT = 10;
const string RealtimeIndexSynchronizer::SYNC_INC_VERSION_TIMESTAMP = "sync_inc_version_timestamp";
RealtimeIndexSynchronizer::RealtimeIndexSynchronizer(SyncMode mode)
    : mLastSyncVersion(INVALID_VERSION)
    , mLastSyncSegment(INVALID_SEGMENTID)
    , mSyncMode(mode)
    , mLastSyncFailTimestamp(-1)
    , mCurrentRetryTimes(-1)
    , mEnableSleep(true)
    , mRemoveOldIndex(true)
{
}

RealtimeIndexSynchronizer::~RealtimeIndexSynchronizer()
{
    mRemotePartitionData.reset();
    mRemoteFileSystem.reset();
}

bool RealtimeIndexSynchronizer::Init(const std::string& remoteRtPath,
                                     util::PartitionMemoryQuotaControllerPtr partitionMemController)
{
    mRemoteRtDir = remoteRtPath;
    mPartitionMemoryQuotaController = partitionMemController;
    char* envParam = getenv("INDEXLIB_REALTIME_SYNC_ENABLE_SLEEP");
    if (envParam && string(envParam) == "false") {
        mEnableSleep = false;
    }

    envParam = getenv("INDEXLIB_REALTIME_SYNC_REMOVE_OLD_INDEX");
    if (envParam && string(envParam) == "false") {
        mRemoveOldIndex = false;
    }
    auto ec = file_system::FslibWrapper::MkDirIfNotExist(remoteRtPath).Code();
    if (ec != file_system::FSEC_OK) {
        IE_LOG(ERROR, "Init failed, ec[%d]", ec);
        return false;
    }
    return true;
}

file_system::IFileSystemPtr RealtimeIndexSynchronizer::GetRemoteFileSystem()
{
    if (mRemoteFileSystem) {
        return mRemoteFileSystem;
    }
    try {
        config::IndexPartitionOptions options;
        file_system::FileSystemOptions fsOptions = common::FileSystemFactory::CreateFileSystemOptions(
            mRemoteRtDir, options, mPartitionMemoryQuotaController, file_system::FileBlockCacheContainerPtr(),
            "rertIndexSync");
        fsOptions.outputStorage = file_system::FSST_DISK;
        fsOptions.isOffline = true; // make root entry meta lazy
        mRemoteFileSystem =
            file_system::FileSystemCreator::CreateForRead("remote_realtime", mRemoteRtDir, fsOptions).GetOrThrow();
    } catch (const util::ExceptionBase& e) {
        IE_LOG(ERROR, "Init failed, exception [%s]", e.what());
        mRemoteFileSystem.reset();
    }
    return mRemoteFileSystem;
}

void RealtimeIndexSynchronizer::Reset()
{
    mRemotePartitionData.reset();
    mRemoteFileSystem.reset();
    mLastSyncSegment = INVALID_SEGMENTID;
    mLastSyncVersion = index_base::Version(INVALID_VERSION);
}

bool RealtimeIndexSynchronizer::CanSync() const
{
    if (!mEnableSleep) {
        return true;
    }
    if (mLastSyncFailTimestamp == -1) {
        return true;
    }
    assert(mCurrentRetryTimes >= 1);
    int64_t duration = autil::TimeUtility::currentTimeInSeconds() - mLastSyncFailTimestamp;
    int64_t limit = mCurrentRetryTimes >= 6 ? 60 : (1 << mCurrentRetryTimes);
    return duration >= limit;
}

void RealtimeIndexSynchronizer::NotifySyncError()
{
    if (mLastSyncFailTimestamp == -1) {
        mLastSyncFailTimestamp = autil::TimeUtility::currentTimeInSeconds();
        mCurrentRetryTimes = 1;
        return;
    }
    mLastSyncFailTimestamp = autil::TimeUtility::currentTimeInSeconds();
    mCurrentRetryTimes++;
}

void RealtimeIndexSynchronizer::ResetSyncError()
{
    mLastSyncFailTimestamp = -1;
    mCurrentRetryTimes = -1;
}

bool RealtimeIndexSynchronizer::SyncFromRemote(const file_system::DirectoryPtr& localRtDir,
                                               const index_base::Version& incVersion)
{
    IE_LOG(INFO, "begin sync realtime index from remote");
    BEEPER_REPORT(INDEXLIB_BUILD_INFO_COLLECTOR_NAME, "begin sync realtime index from remote");
    autil::ScopedLock lock(mLock);
    try {
        // this directory maybe not exist
        // file_system::ErrorCode ec = file_system::FslibWrapper::MkDirIfNotExist(localRtDir->GetOutputPath()).Code();
        // THROW_IF_FS_ERROR(ec, "MkDirIfNotExist for [%s] failed", localRtDir->DebugString().c_str());
        OnDiskPartitionDataPtr localPartitionData =
            OnDiskPartitionData::CreateRealtimePartitionData(localRtDir, true, plugin::PluginManagerPtr(), false);
        OnDiskPartitionDataPtr remotePartitionData = GetRemotePartitionData(false);
        if (!remotePartitionData || !localPartitionData) {
            Reset();
            return false;
        }
        index_base::Version localRtVersion = localPartitionData->GetVersion();
        index_base::Version originRtVersion = localRtVersion;
        index_base::Version remoteRtVersion = remotePartitionData->GetVersion();
        index_base::Version originRemoteVersion = remoteRtVersion;

        int64_t incVersionTs = incVersion.GetTimestamp();
        int64_t remoteIncTs = -1;
        string remoteTsStr;
        if (!originRemoteVersion.GetDescription(SYNC_INC_VERSION_TIMESTAMP, remoteTsStr) ||
            !autil::StringUtil::fromString(remoteTsStr, remoteIncTs) || incVersionTs < remoteIncTs) {
            remoteRtVersion = index_base::Version(INVALID_VERSION);
            originRemoteVersion = index_base::Version(INVALID_VERSION);
            stringstream ss;
            ss << "inc ts [" << incVersionTs << "] smaller than remote ts [" << remoteTsStr
               << "], clean all local segment, not use remote index";
            IE_LOG(INFO, "%s", ss.str().c_str());
            BEEPER_REPORT(INDEXLIB_BUILD_INFO_COLLECTOR_NAME, ss.str());
        }

        // remove useless obsolete segments
        TrimObsoleteSegments(incVersion, localPartitionData, &localRtVersion, false);
        TrimObsoleteSegments(incVersion, remotePartitionData, &remoteRtVersion, true);
        if (remoteRtVersion.GetSegmentCount() == 0 && originRemoteVersion.GetSegmentCount() > 0) {
            segmentid_t segId = originRemoteVersion.GetLastSegment();
            if (originRtVersion.HasSegment(segId) &&
                SegmentEqual(localPartitionData->GetSegmentData(segId), remotePartitionData->GetSegmentData(segId))) {
                // inc cover all remote rt, just remove local useless segments and return
                localPartitionData.reset();
                index_base::OfflineRecoverStrategy::RemoveUselessSegments(localRtVersion, localRtDir);
                Reset();
                IE_LOG(INFO, "inc cover all remote rt, just remove local useless segments");
                BEEPER_REPORT(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                              "inc cover all remote rt, just remove local useless segments");
                return true;
            }
        }
        // trim not match remote segments
        TrimNotMatchSegments(localPartitionData, remotePartitionData, remoteRtVersion, &localRtVersion);
        // remove useless local segments
        index_base::OfflineRecoverStrategy::RemoveUselessSegments(localRtVersion, localRtDir);
        // pull diff segments
        PullDiffSegments(remotePartitionData, remoteRtVersion, localRtVersion, localRtDir);
    } catch (const util::ExceptionBase& e) {
        Reset();
        IE_LOG(ERROR, "sync from remote failed, exception [%s]", e.what());
        return false;
    }
    Reset();
    IE_LOG(INFO, "end sync realtime index from remote");
    BEEPER_REPORT(INDEXLIB_BUILD_INFO_COLLECTOR_NAME, "end sync realtime index from remote");
    return true;
}

OnDiskPartitionDataPtr RealtimeIndexSynchronizer::GetRemotePartitionData(bool cleanUseless)
{
    if (mRemotePartitionData) {
        return mRemotePartitionData;
    }
    auto remoteFileSystem = GetRemoteFileSystem();
    if (!remoteFileSystem) {
        return OnDiskPartitionDataPtr();
    }
    mRemotePartitionData = OnDiskPartitionData::CreateOnDiskPartitionData(remoteFileSystem);
    if (cleanUseless) {
        file_system::DirectoryPtr rtDir = mRemotePartitionData->GetRootDirectory();
        if (mRemoveOldIndex) {
            index_base::OfflineRecoverStrategy::RemoveUselessSegments(mRemotePartitionData->GetVersion(), rtDir);
            fslib::FileList fileList;
            index_base::VersionLoader::ListVersion(rtDir, fileList);
            for (size_t i = 0; i + 1 < fileList.size(); i++) {
                rtDir->RemoveFile(fileList[i]);
            }
        } else {
            // we just clean segments whose segemnt id is larger than the remote version's lastest segment
            // so we can keep old segments
            index_base::OfflineRecoverStrategy::RemoveLostSegments(mRemotePartitionData->GetVersion(), rtDir);
        }
    }
    return mRemotePartitionData;
}

bool RealtimeIndexSynchronizer::CheckLocalPartitionData(const index_base::PartitionDataPtr& localPartitionData)
{
    if (!localPartitionData) {
        return false;
    }

    index_base::Version localVersion = localPartitionData->GetVersion();
    for (size_t i = 0; i < localVersion.GetSegmentCount(); i++) {
        if (index_base::RealtimeSegmentDirectory::IsRtSegmentId(localVersion[i])) {
            return true;
        }
    }
    return false;
}

// TODO (yiping.typ) : fix sync failed when env "INDEXLIB_REALTIME_SYNC_REMOVE_OLD_INDEX" set false
// and index has been rolled back remote version not contain old segment (such as segment 1)
// local segment (1) will sync, which causes ExistFileException
bool RealtimeIndexSynchronizer::SyncToRemote(const index_base::PartitionDataPtr& partitionData,
                                             autil::ThreadPoolPtr& syncThreadPool)
{
    if (mSyncMode == READ_ONLY) {
        return true;
    }
    autil::ScopedLock lock(mLock);
    if (!CanSync()) {
        return false;
    }
    index_base::PartitionDataPtr localPartitionData(partitionData->Clone());
    if (!CheckLocalPartitionData(localPartitionData)) {
        ResetSyncError();
        return true;
    }
    index_base::Version localVersion = localPartitionData->GetVersion();
    segmentid_t lastLinkSegment = localPartitionData->GetLastValidRtSegmentInLinkDirectory();
    if (lastLinkSegment == INVALID_SEGMENTID) {
        return true;
    }
    if (localVersion == mLastSyncVersion && mLastSyncSegment == lastLinkSegment) {
        ResetSyncError();
        return true;
    }
    localPartitionData->SwitchToLinkDirectoryForRtSegments(lastLinkSegment);
    IE_LOG(INFO, "begin sync realtime index to remote");
    BEEPER_REPORT(INDEXLIB_BUILD_INFO_COLLECTOR_NAME, "begin sync realtime index to remote");
    autil::ThreadPoolPtr usingThreadPool = syncThreadPool;
    if (!usingThreadPool) {
        usingThreadPool.reset(new autil::ThreadPool(1, 32, true));
        if (!usingThreadPool->start("SyncToRemote")) {
            usingThreadPool->stop();
            return false;
        }
    }
    try {
        // push diff
        OnDiskPartitionDataPtr remotePartitionData = GetRemotePartitionData(true);
        if (!remotePartitionData) {
            Reset();
            NotifySyncError();
            return false;
        }
        index_base::Version remoteRtVersion = remotePartitionData->GetVersion();
        file_system::DirectoryPtr remoteDirectory = remotePartitionData->GetRootDirectory();
        index_base::Version targetVersion = localVersion;
        targetVersion.Clear();
        targetVersion.SetLastSegmentId(INVALID_SEGMENTID);

        for (size_t i = 0; i < localVersion.GetSegmentCount(); i++) {
            segmentid_t segId = localVersion[i];
            if (!index_base::RealtimeSegmentDirectory::IsRtSegmentId(segId)) {
                continue;
            }
            // only sync flush segment
            if (segId > lastLinkSegment) {
                continue;
            }

            targetVersion.AddSegment(segId);
            index_base::SegmentTemperatureMeta meta;
            if (localVersion.GetSegmentTemperatureMeta(segId, meta)) {
                targetVersion.AddSegTemperatureMeta(meta);
            }
            indexlibv2::framework::SegmentStatistics segStats;
            if (localVersion.GetSegmentStatistics(segId, &segStats)) {
                targetVersion.AddSegmentStatistics(segStats);
            }
            if (remoteRtVersion.HasSegment(segId)) {
                // check segment equal, not equal delete, example: rollback case
                if (SegmentEqual(localPartitionData->GetSegmentData(segId),
                                 remotePartitionData->GetSegmentData(segId))) {
                    continue;
                } else {
                    remoteDirectory->RemoveDirectory(remoteRtVersion.GetSegmentDirName(segId));
                    stringstream ss;
                    ss << "delete useless remote segment[" << segId << "]  which is not same with local";
                    IE_LOG(INFO, "%s", ss.str().c_str());
                    BEEPER_REPORT(INDEXLIB_BUILD_INFO_COLLECTOR_NAME, ss.str());
                }
            }
            // parallel process between segments
            SyncSegment(usingThreadPool, localPartitionData->GetSegmentData(segId), remoteDirectory);
        }
        usingThreadPool->waitFinish();
        targetVersion.AddDescription(
            SYNC_INC_VERSION_TIMESTAMP,
            autil::StringUtil::toString(localPartitionData->GetOnDiskVersion().GetTimestamp()));
        targetVersion.Store(remoteDirectory, true);

        if (mRemoveOldIndex) {
            for (size_t i = 0; i < remoteRtVersion.GetSegmentCount(); i++) {
                if (targetVersion.HasSegment(remoteRtVersion[i])) {
                    continue;
                }
                auto segmentData = remotePartitionData->GetSegmentData(remoteRtVersion[i]);
                remoteDirectory->RemoveDirectory(remoteRtVersion.GetSegmentDirName(remoteRtVersion[i]));
            }

            if (remoteRtVersion.GetVersionId() != INVALID_VERSION &&
                targetVersion.GetVersionId() != remoteRtVersion.GetVersionId()) {
                remoteDirectory->RemoveFile(remoteRtVersion.GetVersionFileName());
            }
        }
        mRemotePartitionData = OnDiskPartitionData::CreateOnDiskPartitionData(GetRemoteFileSystem());
    } catch (util::ExceptionBase& e) {
        if (syncThreadPool) {
            size_t threadNum = syncThreadPool->getThreadNum();
            syncThreadPool.reset(new autil::ThreadPool(threadNum, 32, true));
            syncThreadPool->start("syncRealtimeIndex");
        }
        Reset();
        IE_LOG(ERROR, "exception [%s] happened", e.what());
        NotifySyncError();
        return false;
    }
    mLastSyncVersion = localVersion;
    mLastSyncSegment = lastLinkSegment;
    BEEPER_REPORT(INDEXLIB_BUILD_INFO_COLLECTOR_NAME, "end sync realtime index to remote");
    IE_LOG(INFO, "end sync realtime index to remote");
    ResetSyncError();
    return true;
}

void RealtimeIndexSynchronizer::TrimObsoleteSegments(const index_base::Version& incVersion,
                                                     const OnDiskPartitionDataPtr& partitionData,
                                                     index_base::Version* version, bool isRemote)
{
    index_base::Version targetVersion = *version;
    for (size_t i = 0; i < version->GetSegmentCount(); ++i) {
        segmentid_t segId = (*version)[i];
        const std::shared_ptr<const index_base::SegmentInfo> segInfo =
            partitionData->GetSegmentData(segId).GetSegmentInfo();
        if (segInfo->timestamp < incVersion.GetTimestamp()) {
            stringstream ss;
            ss << "remove ";
            if (isRemote) {
                ss << "remote ";
            } else {
                ss << "local ";
            }
            ss << "segment [" << segId << "] in version";
            IE_LOG(INFO, "%s", ss.str().c_str());
            BEEPER_REPORT(INDEXLIB_BUILD_INFO_COLLECTOR_NAME, ss.str());
            targetVersion.RemoveSegment(segId);
        }
    }
    *version = targetVersion;
}

void RealtimeIndexSynchronizer::TrimNotMatchSegments(const OnDiskPartitionDataPtr& localPartitionData,
                                                     const OnDiskPartitionDataPtr& remotePartitionData,
                                                     const index_base::Version& remoteRtVersion,
                                                     index_base::Version* localVersion)
{
    if (remoteRtVersion.GetSegmentCount() == 0) {
        localVersion->Clear();
        return;
    }

    for (size_t i = 0; i < remoteRtVersion.GetSegmentCount(); ++i) {
        if (i >= localVersion->GetSegmentCount()) {
            return;
        }

        if (!SegmentEqual(localPartitionData->GetSegmentData((*localVersion)[i]),
                          remotePartitionData->GetSegmentData(remoteRtVersion[i]))) {
            index_base::Version targetargetVersion = *localVersion;
            for (size_t j = i; j < localVersion->GetSegmentCount(); ++j) {
                auto segId = (*localVersion)[j];
                targetargetVersion.RemoveSegment(segId);
            }
            *localVersion = targetargetVersion;
            return;
        }
    }
}

bool RealtimeIndexSynchronizer::SegmentEqual(const index_base::SegmentData& leftSegData,
                                             const index_base::SegmentData& rightSegData) const
{
    if (leftSegData.GetSegmentId() != rightSegData.GetSegmentId()) {
        return false;
    }
    string leftStoreTs;
    bool exist = leftSegData.GetSegmentInfo()->GetDescription(SEGMENT_INIT_TIMESTAMP, leftStoreTs);
    if (!exist) {
        return false;
    }
    string rightStoreTs;
    exist = rightSegData.GetSegmentInfo()->GetDescription(SEGMENT_INIT_TIMESTAMP, rightStoreTs);
    if (!exist) {
        return false;
    }
    return leftStoreTs == rightStoreTs;
}

void RealtimeIndexSynchronizer::PullDiffSegments(const OnDiskPartitionDataPtr& remotePartitionData,
                                                 const index_base::Version& remoteRtVersion,
                                                 const index_base::Version& localVersion,
                                                 const file_system::DirectoryPtr& localRtDir)
{
    autil::ThreadPoolPtr threadPool(new autil::ThreadPool(DEFAULT_SYNC_THREAD_COUNT, 32, true));
    threadPool->start("PullDiffSegments");
    for (size_t i = localVersion.GetSegmentCount(); i < remoteRtVersion.GetSegmentCount(); ++i) {
        SyncSegment(threadPool, remotePartitionData->GetSegmentData(remoteRtVersion[i]), localRtDir);
    }
    threadPool->waitFinish();
    threadPool->stop();
}

void RealtimeIndexSynchronizer::SyncSegment(const autil::ThreadPoolPtr& threadPool,
                                            const index_base::SegmentData& segmentData,
                                            const file_system::DirectoryPtr& targetDir)
{
    // parallel process files in one segment
    // the segment info file will be processed after all other files have been processed
    const file_system::DirectoryPtr segDir = segmentData.GetDirectory();
    index_base::SegmentInfo segmentInfo = *segmentData.GetSegmentInfo();
    index_base::Version tmpVersion;
    segmentid_t segId = segmentData.GetSegmentId();
    string segDirName = tmpVersion.GetNewSegmentDirName(segId);
    if (mRemoveOldIndex == false) {
        indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
        targetDir->RemoveDirectory(segDirName, removeOption);
    }
    auto remoteSegmentDir = targetDir->MakeDirectory(segDirName);
    stringstream ss;
    ss << "sync segment [" << segId << "] to dir [" << targetDir->DebugString() << "]";
    IE_LOG(INFO, "%s", ss.str().c_str());
    BEEPER_REPORT(INDEXLIB_BUILD_INFO_COLLECTOR_NAME, ss.str());
    file_system::IndexFileList fileList;
    index_base::SegmentFileListWrapper::Load(segDir, fileList);
    fileList.Append(file_system::FileInfo(SEGMENT_FILE_LIST));
    file_system::FileInfo segmentInfoFieldInfo;
    SegmentInfoSynchronizerPtr synchronizer(new SegmentInfoSynchronizer(segmentInfo, fileList.deployFileMetas.size()));
    for (const auto& fileInfo : fileList.deployFileMetas) {
        if (fileInfo.filePath != SEGMENT_INFO_FILE_NAME) {
            SegmentSyncItem* syncItem = new SegmentSyncItem(synchronizer, fileInfo, segDir, remoteSegmentDir);
            if (threadPool->pushWorkItem(syncItem, true) != autil::ThreadPool::ERROR_NONE) {
                autil::ThreadPool::dropItemIgnoreException(syncItem);
                INDEXLIB_FATAL_ERROR(InconsistentState, "push sync file item failed");
            }
        }
    }
}
}} // namespace indexlib::partition
