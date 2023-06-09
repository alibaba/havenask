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
#include "indexlib/index_base/segment/segment_directory.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/config/build_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/LifecycleConfig.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/lifecycle_strategy.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, SegmentDirectory);

bool SegmentDirectory::SegmentDataFinder::Find(segmentid_t segId, SegmentData& segData)
{
    if (mSegDataVec.empty() || segId > mSegDataVec.rbegin()->GetSegmentId()) {
        return false;
    }
    if (mCursor >= mSegDataVec.size() || mSegDataVec[mCursor].GetSegmentId() > segId) {
        mCursor = 0;
    }
    for (; mCursor < mSegDataVec.size(); ++mCursor) {
        segmentid_t curSegId = mSegDataVec[mCursor].GetSegmentId();
        if (curSegId < segId) {
            continue;
        }
        if (curSegId > segId) {
            return false;
        }
        if (curSegId == segId) {
            segData = mSegDataVec[mCursor++];
            return true;
        }
    }
    return false;
}

SegmentDirectory::SegmentDirectory(std::shared_ptr<file_system::LifecycleConfig> lifecycleConfig)
    : mLifecycleConfig(lifecycleConfig)
    , mIndexFormatVersion(INDEX_FORMAT_VERSION)
    , mIsSubSegDir(false)
{
}

SegmentDirectory::SegmentDirectory(const SegmentDirectory& other)
    : mLifecycleConfig(other.mLifecycleConfig)
    , mLifecycleStrategy(other.mLifecycleStrategy)
    , mVersion(other.mVersion)
    , mOnDiskVersion(other.mOnDiskVersion)
    , mRootDirectory(other.mRootDirectory)
    , mSegmentDatas(other.mSegmentDatas)
    , mIndexFormatVersion(other.mIndexFormatVersion)
    , mIsSubSegDir(other.mIsSubSegDir)
    , mPatchAccessor(other.mPatchAccessor)
    , mOnDiskVersionDpDesc(other.mOnDiskVersionDpDesc)
{
    if (other.mSubSegmentDirectory) {
        mSubSegmentDirectory.reset(other.mSubSegmentDirectory->Clone());
    }
}

SegmentDirectory::~SegmentDirectory() {}

SegmentDirectory* SegmentDirectory::Clone()
{
    autil::ScopedLock scopedLock(mLock);
    return new SegmentDirectory(*this);
}

void SegmentDirectory::Init(const file_system::DirectoryPtr& directory, index_base::Version version, bool hasSub)
{
    mRootDirectory = directory;

    mVersion = version;
    if (mVersion.GetVersionId() == INVALID_VERSION) {
        mVersion = GetLatestVersion(directory, mVersion);
    }

    mOnDiskVersion = mVersion;
    InitIndexFormatVersion(mRootDirectory);
    mLifecycleStrategy = InitLifecycleStrategy();
    DoInit(directory);
    InitSegmentDatas(mVersion);

    if (hasSub) {
        mSubSegmentDirectory.reset(Clone());
        mSubSegmentDirectory->SetSubSegmentDir();
    }
}

std::shared_ptr<LifecycleStrategy> SegmentDirectory::InitLifecycleStrategy()
{
    if (mLifecycleConfig == nullptr) {
        return nullptr;
    }
    if (!mRootDirectory) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Failed due to mRootDirectory is NULL");
    }
    bool needRefreshLifecycleInFilesystem = false;
    const FileSystemOptions& fsOptions = mRootDirectory->GetFileSystem()->GetFileSystemOptions();
    auto loadConfigs = fsOptions.loadConfigList.GetLoadConfigs();
    for (const LoadConfig& config : loadConfigs) {
        if (!config.GetLifecycle().empty()) {
            needRefreshLifecycleInFilesystem = true;
            break;
        }
    }
    if (needRefreshLifecycleInFilesystem == false) {
        return nullptr;
    }
    auto newLifecycleStrategy = LifecycleStrategyFactory::CreateStrategy(
        *mLifecycleConfig,
        {{LifecyclePatternBase::CURRENT_TIME, std::to_string(LifecycleConfig::CurrentTimeInSeconds())}});
    if (newLifecycleStrategy == nullptr) {
        IE_LOG(ERROR, "init lifecycle strategy failed in SegmentDirectory[%s]",
               mRootDirectory->GetLogicalPath().c_str());
        return nullptr;
    }
    return std::shared_ptr<LifecycleStrategy>(newLifecycleStrategy.release());
}

void SegmentDirectory::Reopen(const index_base::Version& version)
{
    assert(version.GetVersionId() != INVALID_VERSION);
    mVersion = version;
    mOnDiskVersion = mVersion;
    DoReopen();
    InitSegmentDatas(mVersion);

    if (mSubSegmentDirectory) {
        mSubSegmentDirectory.reset(Clone());
        mSubSegmentDirectory->SetSubSegmentDir();
    }
}

void SegmentDirectory::IncLastSegmentId()
{
    autil::ScopedLock scopedLock(mLock);
    segmentid_t segId = CreateNewSegmentId();
    mVersion.SetLastSegmentId(segId);
}

void SegmentDirectory::AddSegment(segmentid_t segId, timestamp_t ts)
{
    autil::ScopedLock scopedLock(mLock);
    mVersion.AddSegment(segId);
    if (ts > mVersion.GetTimestamp()) {
        mVersion.SetTimestamp(ts);
    }
    InitSegmentDatas(mVersion);
}

void SegmentDirectory::AddSegment(segmentid_t segId, timestamp_t ts,
                                  const std::shared_ptr<indexlibv2::framework::SegmentStatistics>& segmentStats)
{
    autil::ScopedLock scopedLock(mLock);
    AddSegment(segId, ts);
    if (segmentStats != nullptr) {
        mVersion.AddSegmentStatistics(*segmentStats);
    }
}

void SegmentDirectory::AddSegmentTemperatureMeta(const SegmentTemperatureMeta& temperatureMeta)
{
    autil::ScopedLock scopedLock(mLock);
    mVersion.AddSegTemperatureMeta(temperatureMeta);
}

void SegmentDirectory::UpdateSchemaVersionId(schemavid_t id)
{
    autil::ScopedLock scopedLock(mLock);
    mVersion.SetSchemaVersionId(id);
}

void SegmentDirectory::SetOngoingModifyOperations(const vector<schema_opid_t>& opIds)
{
    autil::ScopedLock scopedLock(mLock);
    mVersion.SetOngoingModifyOperations(opIds);
}

void SegmentDirectory::RemoveSegments(const vector<segmentid_t>& segIds)
{
    autil::ScopedLock scopedLock(mLock);
    for (size_t i = 0; i < segIds.size(); i++) {
        mVersion.RemoveSegment(segIds[i]);
    }
    InitSegmentDatas(mVersion);
}

segmentid_t SegmentDirectory::CreateNewSegmentId()
{
    autil::ScopedLock scopedLock(mLock);
    segmentid_t newSegmentId = FormatSegmentId(0);
    if (mVersion.GetLastSegment() != INVALID_SEGMENTID) {
        newSegmentId = mVersion.GetLastSegment() + 1;
    }
    return newSegmentId;
}

segmentid_t SegmentDirectory::GetNextSegmentId(segmentid_t baseSegmentId) const
{
    if (baseSegmentId != INVALID_SEGMENTID) {
        return baseSegmentId + 1;
    }
    return FormatSegmentId(0);
}

std::string SegmentDirectory::GetSegmentDirName(segmentid_t segId) const
{
    autil::ScopedLock scopedLock(mLock);
    return mVersion.GetSegmentDirName(segId);
}

bool SegmentDirectory::IsVersionChanged() const
{
    autil::ScopedLock scopedLock(mLock);
    return mVersion != mOnDiskVersion;
}

void SegmentDirectory::RollbackToCurrentVersion(bool needRemoveSegment)
{
    autil::ScopedLock scopedLock(mLock);
    fslib::FileList versionList;
    VersionLoader::ListVersion(mRootDirectory, versionList);
    for (size_t i = 0; i < versionList.size(); ++i) {
        Version version;
        version.Load(mRootDirectory, versionList[i]);
        if (mVersion.GetVersionId() < version.GetVersionId()) {
            mRootDirectory->RemoveFile(versionList[i]);
        }
    }

    if (!needRemoveSegment) {
        return;
    }
    segmentid_t lastSegment = mVersion.GetLastSegment();
    fslib::FileList segments;
    VersionLoader::ListSegments(mRootDirectory, segments);
    for (size_t i = 0; i < segments.size(); ++i) {
        segmentid_t currentSegment = Version::GetSegmentIdByDirName(segments[i]);
        if (lastSegment < currentSegment) {
            mRootDirectory->RemoveDirectory(segments[i]);
        }
    }
}

void SegmentDirectory::CommitVersion(const IndexPartitionOptions& options)
{
    autil::ScopedLock scopedLock(mLock);
    return DoCommitVersion(options, true);
}

void SegmentDirectory::DoCommitVersion(const IndexPartitionOptions& options, bool dumpDeployIndexMeta)
{
    if (!IsVersionChanged()) {
        return;
    }
    SegmentDataVector::reverse_iterator it = mSegmentDatas.rbegin();
    if (it != mSegmentDatas.rend()) {
        mVersion.SetTimestamp(it->GetSegmentInfo()->timestamp);
        document::Locator locator(it->GetSegmentInfo()->GetLocator().Serialize());
        mVersion.SetLocator(locator);
    }

    versionid_t preVersion = mVersion.GetVersionId();
    mVersion.IncVersionId();
    mVersion.SetDescriptions(options.GetVersionDescriptions());
    mVersion.SetUpdateableSchemaStandards(options.GetUpdateableSchemaStandards());
    if (dumpDeployIndexMeta) {
        VersionCommitter::DumpPartitionPatchMeta(mRootDirectory, mVersion);
        DeployIndexWrapper::DumpDeployMeta(mRootDirectory, mVersion);
    }
    mVersion.Store(mRootDirectory, false);
    mOnDiskVersion = mVersion;
    if (dumpDeployIndexMeta) {
        IndexSummary ret = IndexSummary::Load(mRootDirectory, mVersion, preVersion);
        ret.Commit(mRootDirectory);
    }
}

file_system::DirectoryPtr SegmentDirectory::GetSegmentFsDirectory(segmentid_t segId) const
{
    autil::ScopedLock scopedLock(mLock);
    file_system::DirectoryPtr segmentParentDirectory = GetSegmentParentDirectory(segId);
    if (!segmentParentDirectory) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Failed to get segment parent dir, segId [%d]", segId);
    }

    string segDirName = GetSegmentDirName(segId);
    file_system::DirectoryPtr segDirectory = segmentParentDirectory->GetDirectory(segDirName, false);
    if (mIsSubSegDir) {
        if (!segDirectory) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "Failed to get sub segment, dir [%s] in root [%s]", segDirName.c_str(),
                                 segmentParentDirectory->DebugString().c_str());
        }
        segDirectory = segDirectory->GetDirectory(SUB_SEGMENT_DIR_NAME, false);
    }

    if (segDirectory) {
        segDirectory->MountPackage();
    }
    return segDirectory;
}

SegmentDirectoryPtr SegmentDirectory::GetSubSegmentDirectory()
{
    autil::ScopedLock scopedLock(mLock);
    return mSubSegmentDirectory;
}

file_system::DirectoryPtr SegmentDirectory::GetSegmentParentDirectory(segmentid_t segId) const
{
    autil::ScopedLock scopedLock(mLock);
    return mRootDirectory;
}

void SegmentDirectory::InitSegmentDatas(index_base::Version& version)
{
    UpdatePatchAccessor(version);
    UpdateLifecycleStrategy();
    SegmentDataVector newSegmentDatas;
    exdocid_t baseDocId = 0;
    SegmentDataFinder segDataFinder(mSegmentDatas);
    for (size_t i = 0; i < version.GetSegmentCount(); ++i) {
        segmentid_t segId = version[i];
        SegmentData segmentData;
        if (segDataFinder.Find(segId, segmentData)) {
            IE_LOG(DEBUG, "reuse segmentData for segment [%d]", segId);
            segmentData.SetBaseDocId(baseDocId);
            baseDocId += segmentData.GetSegmentInfo()->docCount;
            segmentData.SetPatchIndexAccessor(mPatchAccessor);
            RefreshSegmentLifecycle(false, &segmentData);
        } else {
            IE_LOG(DEBUG, "load new segmentData for segment [%d]", segId);
            DirectoryPtr segDirectory = GetSegmentFsDirectory(segId);
            if (!segDirectory) {
                INDEXLIB_FATAL_ERROR(IndexCollapsed, "Failed to get segment directory [%d]", segId);
            }

            SegmentInfo segmentInfo;
            if (!segmentInfo.Load(segDirectory)) {
                INDEXLIB_FATAL_ERROR(IndexCollapsed, "Failed to load segment info [%s]",
                                     segDirectory->DebugString().c_str());
            }

            indexlib::framework::SegmentMetrics segmentMetrics;
            if (segDirectory->IsExist(SEGMETN_METRICS_FILE_NAME)) {
                segmentMetrics.Load(segDirectory);
            }

            segmentData.SetBaseDocId(baseDocId);
            baseDocId += segmentInfo.docCount;
            segmentData.SetSegmentInfo(segmentInfo);
            segmentData.SetSegmentMetrics(segmentMetrics);
            segmentData.SetSegmentId(segId);
            segmentData.SetDirectory(segDirectory);
            segmentData.SetPatchIndexAccessor(mPatchAccessor);
            RefreshSegmentLifecycle(true, &segmentData);
        }
        newSegmentDatas.push_back(segmentData);
    }

    mSegmentDatas.swap(newSegmentDatas);
    SegmentDataVector::reverse_iterator it = mSegmentDatas.rbegin();
    if (it != mSegmentDatas.rend()) {
        version.SetTimestamp(it->GetSegmentInfo()->timestamp);
        document::Locator locator(it->GetSegmentInfo()->GetLocator().Serialize());
        version.SetLocator(locator);
    }
}

void SegmentDirectory::RefreshSegmentLifecycle(bool isNewSegment, SegmentData* segmentData)
{
    assert(segmentData != nullptr);
    if (mLifecycleStrategy == nullptr) {
        return;
    }
    auto segDir = segmentData->GetDirectory();
    auto currentLifecycle =
        (mLifecycleStrategy == nullptr) ? "" : mLifecycleStrategy->GetSegmentLifecycle(mVersion, segmentData);

    if (currentLifecycle != segmentData->GetLifecycle()) {
        IE_LOG(INFO, "detected: segment[%s] lifecycle updated, pre lifecycle=[%s], current lifecycle=[%s]",
               segDir->GetLogicalPath().c_str(), segmentData->GetLifecycle().c_str(), currentLifecycle.c_str());
        segmentData->SetLifecycle(currentLifecycle);
        segDir->SetLifecycle(currentLifecycle);
    }
}

void SegmentDirectory::UpdateLifecycleStrategy()
{
    if ((mLifecycleStrategy == nullptr) ||
        (mLifecycleConfig->GetStrategy() == file_system::LifecycleConfig::STATIC_STRATEGY)) {
        return;
    }
    auto newLifecycleStrategy = LifecycleStrategyFactory::CreateStrategy(
        *mLifecycleConfig,
        {{LifecyclePatternBase::CURRENT_TIME, std::to_string(LifecycleConfig::CurrentTimeInSeconds())}});
    if (newLifecycleStrategy == nullptr) {
        mLifecycleStrategy.reset();
    } else {
        mLifecycleStrategy.reset(newLifecycleStrategy.release());
    }
}

SegmentData SegmentDirectory::GetSegmentData(segmentid_t segId) const
{
    autil::ScopedLock scopedLock(mLock);
    for (size_t i = 0; i < mSegmentDatas.size(); ++i) {
        if (mSegmentDatas[i].GetSegmentId() == segId) {
            return mSegmentDatas[i];
        }
    }

    INDEXLIB_FATAL_ERROR(NonExist, "segment data [%d] not exist", segId);
    return SegmentData();
}

std::pair<bool, std::string> SegmentDirectory::GetSegmentLifecycle(segmentid_t segId) const
{
    autil::ScopedLock scopedLock(mLock);
    auto it = std::lower_bound(mSegmentDatas.begin(), mSegmentDatas.end(), segId, SegmentData::CompareToSegmentId);
    if ((it != mSegmentDatas.end()) && (it->GetSegmentId() == segId)) {
        return std::make_pair(true, it->GetLifecycle());
    }
    return std::make_pair(false, string(""));
}

void SegmentDirectory::SetSubSegmentDir()
{
    autil::ScopedLock scopedLock(mLock);
    mIsSubSegDir = true;
    mSegmentDatas.clear();
    InitSegmentDatas(mVersion);
}

BuildingSegmentData SegmentDirectory::CreateNewSegmentData(const BuildConfig& buildConfig)
{
    autil::ScopedLock scopedLock(mLock);
    BuildingSegmentData newSegmentData(buildConfig);
    segmentid_t segId = CreateNewSegmentId();
    newSegmentData.SetSegmentId(segId);
    newSegmentData.SetBaseDocId(0);
    newSegmentData.SetSegmentDirName(mVersion.GetNewSegmentDirName(segId));

    const SegmentDataVector& segmentDatas = GetSegmentDatas();
    SegmentDataVector::const_reverse_iterator it = segmentDatas.rbegin();
    if (it != segmentDatas.rend()) {
        const std::shared_ptr<const SegmentInfo> lastSegmentInfo = it->GetSegmentInfo();
        SegmentInfo newSegmentInfo;
        newSegmentInfo.SetLocator(lastSegmentInfo->GetLocator());
        newSegmentInfo.timestamp = lastSegmentInfo->timestamp;
        newSegmentData.SetSegmentInfo(newSegmentInfo);
        newSegmentData.SetBaseDocId(it->GetBaseDocId() + lastSegmentInfo->docCount);
    } else {
        SegmentInfo newSegmentInfo;
        indexlibv2::framework::Locator locator;
        locator.Deserialize(mVersion.GetLocator().ToString());
        newSegmentInfo.SetLocator(locator);
        newSegmentInfo.timestamp = mVersion.GetTimestamp();
        newSegmentData.SetSegmentInfo(newSegmentInfo);
    }
    return newSegmentData;
}

void SegmentDirectory::InitIndexFormatVersion(const DirectoryPtr& directory)
{
    assert(directory);
    bool mayNotExist = true;
    if (mIndexFormatVersion.Load(directory, mayNotExist)) {
        return;
    }
    IE_LOG(INFO, "%s not exist in rootDir [%s]!", INDEX_FORMAT_VERSION_FILE_NAME, directory->DebugString().c_str());
}

void SegmentDirectory::UpdatePatchAccessor(const Version& version)
{
    autil::ScopedLock scopedLock(mLock);
    if (version.GetSchemaVersionId() == DEFAULT_SCHEMAID) {
        return;
    }

    if (mPatchAccessor && mPatchAccessor->GetVersionId() == version.GetVersionId()) {
        return;
    }
    mPatchAccessor.reset(new PartitionPatchIndexAccessor);
    mPatchAccessor->Init(mRootDirectory, version);
    if (mIsSubSegDir) {
        mPatchAccessor->SetSubSegmentDir();
    }
}

void SegmentDirectory::UpdateCounterMap(const CounterMapPtr& counterMap) const
{
    autil::ScopedLock scopedLock(mLock);
    const SegmentDataVector& segDataVec = GetSegmentDatas();
    for (auto it = segDataVec.rbegin(); it != segDataVec.rend(); ++it) {
        if (OnlineSegmentDirectory::IsIncSegmentId(it->GetSegmentId())) {
            const DirectoryPtr& lastIncSegDir = it->GetDirectory();
            assert(lastIncSegDir);

            string counterString;
            try {
                lastIncSegDir->Load(COUNTER_FILE_NAME, counterString, true);
            } catch (const util::NonExistException& e) {
                IE_LOG(WARN, "counter file not exists in directory[%s]", lastIncSegDir->DebugString().c_str());
                return;
            } catch (...) {
                throw;
            }

            counterMap->Merge(counterString, util::CounterBase::FJT_OVERWRITE);
            return;
        }
    }
}

Version SegmentDirectory::GetLatestVersion(const DirectoryPtr& directory, const Version& emptyVersion) const
{
    Version version;
    VersionLoader::GetVersion(directory, version, INVALID_VERSION);
    return version.GetVersionId() == INVALID_VERSION ? emptyVersion : version;
}
}} // namespace indexlib::index_base
