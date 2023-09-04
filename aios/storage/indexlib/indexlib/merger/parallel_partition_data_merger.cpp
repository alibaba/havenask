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
#include "indexlib/merger/parallel_partition_data_merger.h"

#include "autil/EnvUtil.h"
#include "beeper/beeper.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FenceDirectory.h"
#include "indexlib/file_system/MergeDirsOption.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_segment_writer.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/index_base/index_meta/progress_synchronizer.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/offline_recover_strategy.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/merger/merge_partition_data_creator.h"
#include "indexlib/merger/merger_branch_hinter.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/StateCounter.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::common;
using namespace indexlib::util;
using namespace indexlib::index;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, ParallelPartitionDataMerger);

ParallelPartitionDataMerger::ParallelPartitionDataMerger(const std::string& mergeRoot, const std::string& epochId,
                                                         const config::IndexPartitionSchemaPtr& schema,
                                                         const config::IndexPartitionOptions& options)
    : mSchema(schema)
    , mOptions(options)
    , mEpochId(epochId)
{
    mFsOptions.enableAsyncFlush = false;
    mFsOptions.isOffline = true;
    mFsOptions.useCache = false;
    auto fs = BranchFS::Create(mergeRoot, "");
    fs->Init(nullptr, mFsOptions);
    mRoot = Directory::ConstructByFenceContext(fs->GetRootDirectory(),
                                               FenceContextPtr(FslibWrapper::CreateFenceContext(mergeRoot, epochId)),
                                               fs->GetFileSystem()->GetFileSystemMetricsReporter());
    if (!mSchema) {
        mSchema = SchemaAdapter::LoadSchema(mRoot);
    }
}

ParallelPartitionDataMerger::ParallelPartitionDataMerger(const file_system::DirectoryPtr& mergeDirectory,
                                                         const std::string& epochId,
                                                         const IndexPartitionSchemaPtr& schema,
                                                         const IndexPartitionOptions& options)
    : mRoot(mergeDirectory)
    , mSchema(schema)
    , mOptions(options)
    , mEpochId(epochId)
{
    mFsOptions.enableAsyncFlush = false;
    mFsOptions.isOffline = true;
    mFsOptions.useCache = false;
}

ParallelPartitionDataMerger::~ParallelPartitionDataMerger() {}

Version ParallelPartitionDataMerger::MergeSegmentData(const vector<std::string>& srcPaths)
{
    IE_LOG(INFO, "Begin merge all data of parallel inc build into merge path.");
    Version lastVersion;
    VersionLoader::GetVersion(mRoot, lastVersion, INVALID_VERSION);
    IE_LOG(INFO, "lastest version [%d] in parent path : [%s]", lastVersion.GetVersionId(),
           lastVersion.ToString().c_str());

    string parentPath = GetParentPath(srcPaths);
    FSResult<bool> ret = FslibWrapper::IsExist(parentPath);
    THROW_IF_FS_ERROR(ret.ec, "check is exist failed [%s]", parentPath.c_str());
    if (!ret.result) {
        IE_LOG(WARN,
               "parallel path [%s] does not exist. maybe merge legacy non-parallel build index, no data to merge.",
               parentPath.c_str());
        return lastVersion;
    }

    DirectoryVector branchDirectorys;
    for (const std::string& srcPath : srcPaths) {
        FSResult<bool> ret = FslibWrapper::IsExist(srcPath);
        THROW_IF_FS_ERROR(ret.ec, "check is exist failed [%s]", srcPath.c_str());
        // to prevent failOver case
        if (!ret.result) {
            IE_LOG(WARN, "parallel path [%s] does not exist. maybe merge legacy build index, no data to merge.",
                   srcPath.c_str());
        } else {
            string branchName;
            MergerBranchHinter hinter(CommonBranchHinterOption::Normal(0, mEpochId));
            branchDirectorys.emplace_back(
                BranchFS::GetDirectoryFromDefaultBranch(srcPath, mFsOptions, &hinter, &branchName));
            if (!hinter.CanOperateBranch(branchName)) {
                INDEXLIB_FATAL_ERROR(Runtime,
                                     "old worker with epochId [%s] cannot move new branch [%s] with epochId [%s]",
                                     mEpochId.c_str(), branchName.c_str(),
                                     CommonBranchHinter::ExtractEpochFromBranch(branchName).c_str());
            }
        }
    }
    versionid_t baseVersionId = GetBaseVersionIdFromParallelBuildInfo(branchDirectorys);
    auto versions = GetVersionList(branchDirectorys);
    if (baseVersionId == INVALID_VERSION) {
        if (lastVersion.GetVersionId() != INVALID_VERSION) {
            baseVersionId = GetBaseVersionFromLagecyParallelBuild(versions);
            if (baseVersionId == INVALID_VERSION) {
                // empty instance versions, use root last version
                baseVersionId = lastVersion.GetVersionId();
            }
        }
    }

    Version baseVersion;
    if (baseVersionId != INVALID_VERSION) {
        const string versionFileName = Version::GetVersionFileName(baseVersionId);
        VersionLoader::GetVersion(mRoot, baseVersion, baseVersionId);
    }
    IE_LOG(INFO, "base version [%d] in parent path : [%s]", baseVersionId, baseVersion.ToString().c_str());

    if (IsEmptyVersions(versions, baseVersion)) {
        // empty parent path & empty parallel inc path
        IE_LOG(INFO, "make empty version.");
        Version emptyVersion = versions[0];
        emptyVersion.SyncSchemaVersionId(mSchema);
        emptyVersion.SetOngoingModifyOperations(mOptions.GetOngoingModifyOperationIds());
        emptyVersion.SetDescriptions(mOptions.GetVersionDescriptions());
        emptyVersion.SetUpdateableSchemaStandards(mOptions.GetUpdateableSchemaStandards());
        VersionCommitter versionCommiter(mRoot, emptyVersion, INVALID_KEEP_VERSION_COUNT);
        versionCommiter.Commit();
        return emptyVersion;
    }

    auto versionInfos = GetValidVersions(versions, baseVersion);
    if (NoNeedMerge(versionInfos, lastVersion, baseVersionId)) {
        IE_LOG(INFO, "merge work has done. Do nothing and exist directly");
        if (versions.size() > 0) {
            ProgressSynchronizer ps;
            ps.Init(versions);
            lastVersion.SetTimestamp(ps.GetTimestamp());
            lastVersion.SetFormatVersion(ps.GetFormatVersion());
        }
        RemoveParallBuildDirectory();
        return lastVersion;
    }

    for (auto& versionPair : versionInfos) {
        const Version& version = versionPair.first;
        baseVersion.MergeDescriptions(version);
        MoveParallelInstanceSegments(version, baseVersion, branchDirectorys[versionPair.second]);
        MoveParallelInstanceSchema(version, branchDirectorys[versionPair.second]);
    }
    bool hasNewSegment = false;
    Version version = MakeNewVersion(baseVersion, versionInfos, hasNewSegment);
    if (branchDirectorys.size() > 1 && hasNewSegment && mSchema->GetTableType() != tt_customized) {
        CreateMergeSegmentForParallelInstance(versionInfos, baseVersion.GetLastSegment(), version);
    }

    version.SyncSchemaVersionId(mSchema);
    version.SetOngoingModifyOperations(mOptions.GetOngoingModifyOperationIds());
    version.SetDescriptions(mOptions.GetVersionDescriptions());
    version.SetUpdateableSchemaStandards(mOptions.GetUpdateableSchemaStandards());

    VersionCommitter versionCommiter(mRoot, version, INVALID_KEEP_VERSION_COUNT);
    versionCommiter.Commit();
    RemoveParallBuildDirectory();
    mRoot->Close();
    IE_LOG(INFO, "End Merged all data of parallel inc build into merge path, generate version [%d]",
           version.GetVersionId());
    return version;
}

void ParallelPartitionDataMerger::RemoveParallBuildDirectory()
{
    fslib::FileList fileList;
    mRoot->ListDir("", fileList);
    for (const auto& dir : fileList) {
        if (ParallelBuildInfo::IsParallelBuildPath(dir)) {
            indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
            mRoot->RemoveDirectory(dir, /*mayNonExist=*/removeOption);
            IE_LOG(INFO, "remove parallel inc build path [%s] in [%s].", dir.c_str(), mRoot->DebugString().c_str());
        }
    }
}

versionid_t ParallelPartitionDataMerger::GetBaseVersionIdFromParallelBuildInfo(const DirectoryVector& directorys)
{
    if (directorys.empty()) {
        INDEXLIB_FATAL_ERROR(BadParameter, "number of parallel build instance path must not be zero.");
    }
    ParallelBuildInfo info;
    info.Load(directorys[0]);
    if (info.parallelNum != directorys.size()) {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "number of parallel build instance path must be same to "
                             "parallNum in ParallelBuildInfo in path[%s].",
                             directorys[0]->DebugString().c_str());
    }
    for (size_t i = 1; i < directorys.size(); i++) {
        ParallelBuildInfo parallelInfo;
        parallelInfo.Load(directorys[i]);
        if (parallelInfo.parallelNum != info.parallelNum || parallelInfo.batchId != info.batchId) {
            INDEXLIB_FATAL_ERROR(BadParameter,
                                 "ParallelBuildInfo.parallelNum and"
                                 "ParallelBuildInfo.batchId of each parallel build instance"
                                 " must be same. [%s] and [%s] are different",
                                 ToJsonString(info).c_str(), ToJsonString(parallelInfo).c_str());
        }

        if (parallelInfo.GetBaseVersion() != info.GetBaseVersion()) {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                                 "ParallelBuildInfo.base_version of each parallel build instance"
                                 " must be same. [%s] and [%s] are different",
                                 ToJsonString(info).c_str(), ToJsonString(parallelInfo).c_str());
        }
    }
    return info.GetBaseVersion();
}

vector<Version> ParallelPartitionDataMerger::GetVersionList(const DirectoryVector& directorys)
{
    vector<Version> versions;
    for (const auto& directory : directorys) {
        Version version;
        VersionLoader::GetVersion(directory, version, INVALID_VERSION);
        Version recoverVersion = OfflineRecoverStrategy::Recover(version, directory);
        Version diffVersion = recoverVersion - version;
        if (diffVersion.GetSegmentCount() > 0) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "[%s] has valid segment not in latest version!",
                                 directory->DebugString().c_str());
        }

        versions.push_back(version);
        if (version.GetVersionId() == INVALID_VERSION) {
            IE_LOG(INFO, "build parallel [%s] has no data.", directory->DebugString().c_str());
        }
    }
    return versions;
}

vector<ParallelPartitionDataMerger::VersionPair>
ParallelPartitionDataMerger::GetValidVersions(const vector<Version>& versions, const Version& baseVersion)
{
    vector<VersionPair> validVersions;
    set<segmentid_t> newSegments;
    for (size_t i = 0; i < versions.size(); i++) {
        const Version& version = versions[i];
        if (version.GetVersionId() == INVALID_VERSION) {
            continue;
        }
        /**
        // if inc builder has built empty data, we still need to generate a new version
        if (version.GetSegmentVector() == baseVersion.GetSegmentVector())
        {
            IE_LOG(WARN, "segments in instance [%zu] version file just equal to parent version.", i);
            continue;
        }
        **/
        const Version::SegmentIdVec& buildSegments = version.GetSegmentVector();
        for (auto& segment : buildSegments) {
            if (baseVersion.HasSegment(segment)) {
                continue;
            }
            if (newSegments.find(segment) != newSegments.end()) {
                INDEXLIB_FATAL_ERROR(InconsistentState,
                                     "parallel build segment must not be repeat. "
                                     "repeat segment is segment_%d in parallel %lu",
                                     segment, i);
            }
            newSegments.insert(segment);
        }
        validVersions.emplace_back(version, i);
        IE_LOG(INFO, "found valid version [%d] in inc parallel %lu, version [%s]", version.GetVersionId(), i,
               version.ToString().c_str());
    }
    return validVersions;
}

bool ParallelPartitionDataMerger::NoNeedMerge(const vector<VersionPair>& versionInfos, const Version& lastVersion,
                                              versionid_t baseVersionId)
{
    if (versionInfos.empty()) {
        return true;
    }

    if (lastVersion.GetVersionId() < baseVersionId) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "latestVersionId [%d] less than baseVersionId [%d]",
                             lastVersion.GetVersionId(), baseVersionId);
    }

    if (lastVersion.GetVersionId() == baseVersionId) {
        return false;
    }

    for (auto& versionInfo : versionInfos) {
        const Version::SegmentIdVec& buildSegments = versionInfo.first.GetSegmentVector();
        for (auto& segment : buildSegments) {
            if (!lastVersion.HasSegment(segment)) {
                IE_LOG(WARN, "segment [%d] not in version.%d, not a valid version, will be replaced!", segment,
                       lastVersion.GetVersionId());
                return false;
            }
        }
    }
    return true;
}

void ParallelPartitionDataMerger::MoveParallelInstanceSegments(const Version& version, const Version& baseVersion,
                                                               const file_system::DirectoryPtr& instDir)
{
    IE_LOG(INFO, "Begin MoveParallelInstanceSegments.vid[%d] baseVersion[%d] dir[%s]", version.GetVersionId(),
           baseVersion.GetVersionId(), instDir->DebugString().c_str());
    const Version::SegmentIdVec& buildSegments = version.GetSegmentVector();
    for (auto& segment : buildSegments) {
        if (!baseVersion.HasSegment(segment)) {
            string segmentDirName = version.GetSegmentDirName(segment);
            auto fileSystsm = instDir->GetFileSystem();
            auto ret = fileSystsm->GetPhysicalPath(segmentDirName);
            if (!ret.OK()) {
                THROW_IF_FS_ERROR(ret.ec, "get instance seg dir from file_system failed, file may not exist [%s]",
                                  segmentDirName.c_str());
            }
            const string& srcPath = ret.result;
            FSResult<bool> isExistRet = FslibWrapper::IsExist(srcPath);
            THROW_IF_FS_ERROR(isExistRet.ec, "check instance seg dir exist failed [%s]", srcPath.c_str());
            if (isExistRet.result) {
                THROW_IF_FS_ERROR(mRoot->GetFileSystem()->MergeDirs(
                                      {srcPath}, segmentDirName,
                                      MergeDirsOption::MergePackageWithFence(mRoot->GetFenceContext().get())),
                                  "merge dirs failed");
                IE_LOG(INFO, "move parallel build segment of version.%d from [%s] to [%s/%s]", version.GetVersionId(),
                       srcPath.c_str(), mRoot->DebugString().c_str(), segmentDirName.c_str());
            } else if (!mRoot->IsExist(segmentDirName)) {
                INDEXLIB_FATAL_ERROR(SegmentFile,
                                     "parallel build segments"
                                     "[%s] should have be moved into partition path[%s/%s], but it was lost.",
                                     srcPath.c_str(), mRoot->DebugString().c_str(), segmentDirName.c_str());
            }
        }
    }
    IE_LOG(INFO, "End MoveParallelInstanceSegments.");
}

void ParallelPartitionDataMerger::MoveParallelInstanceSchema(const Version& version,
                                                             const file_system::DirectoryPtr& instDir)
{
    IE_LOG(INFO, "Begin MoveParallelInstanceSchema. version [%d], dir [%s]", version.GetVersionId(),
           instDir->DebugString().c_str());
    if (version.GetSchemaVersionId() != DEFAULT_SCHEMAID) {
        const std::string schemaFileName = version.GetSchemaFileName();
        if (mRoot->IsExist(schemaFileName)) {
            return;
        }
        std::string srcSchemaFilePath = instDir->GetPhysicalPath(schemaFileName);
        FSResult<bool> isExistRet = FslibWrapper::IsExist(srcSchemaFilePath);
        THROW_IF_FS_ERROR(isExistRet.ec, "check src schema file exist failed [%s]", srcSchemaFilePath.c_str());
        if (isExistRet.result) {
            // copy to partitionDir
            // TODO (yiping.typ) copy makes fence impossible
            const std::string dstSchemaFilePath = PathUtil::JoinPath(mRoot->GetOutputPath(), schemaFileName);
            auto ec = FslibWrapper::Copy(srcSchemaFilePath, dstSchemaFilePath).Code();
            THROW_IF_FS_ERROR(ec, "copy file failed, srcFile[%s] -> dstFile[%s]", srcSchemaFilePath.c_str(),
                              dstSchemaFilePath.c_str());
            THROW_IF_FS_ERROR(mRoot->GetFileSystem()->MountFile(mRoot->GetOutputPath(), schemaFileName, schemaFileName,
                                                                FSMT_READ_WRITE, -1,
                                                                /*mayNonExist=*/false),
                              "mount schema file failed");
        }
    }
    IE_LOG(INFO, "End MoveParallelInstanceSchema.");
}

Version ParallelPartitionDataMerger::MakeNewVersion(const Version& baseVersion, const vector<VersionPair>& versionInfos,
                                                    bool& hasNewSegment)
{
    Version newVersion = baseVersion;
    vector<segmentid_t> segments;
    vector<Version> versions;
    vector<SegmentTemperatureMeta> temperatureMetas;
    vector<indexlibv2::framework::SegmentStatistics> segStatsVec;
    for (auto& versionPair : versionInfos) {
        const Version& version = versionPair.first;
        versions.push_back(version);
        if (version.GetLastSegment() < baseVersion.GetLastSegment()) {
            INDEXLIB_FATAL_ERROR(SegmentFile,
                                 "parallel build segment id[%d] should "
                                 "be larger than segment[%d] in last merged version",
                                 version.GetLastSegment(), baseVersion.GetLastSegment());
        }
        const Version::SegmentIdVec& buildSegments = version.GetSegmentVector();

        for (auto& segmentId : buildSegments) {
            if (!baseVersion.HasSegment(segmentId)) {
                segments.push_back(segmentId);
                SegmentTemperatureMeta meta;
                if (version.GetSegmentTemperatureMeta(segmentId, meta)) {
                    temperatureMetas.push_back(meta);
                }
                indexlibv2::framework::SegmentStatistics segStats;
                if (version.GetSegmentStatistics(segmentId, &segStats)) {
                    segStatsVec.push_back(segStats);
                }
            }
        }
    }
    hasNewSegment = segments.size() > 0;
    sort(segments.begin(), segments.end());
    sort(temperatureMetas.begin(), temperatureMetas.end());
    sort(segStatsVec.begin(), segStatsVec.end(), indexlibv2::framework::SegmentStatistics::CompareBySegmentId);

    for (const auto& segment : segments) {
        newVersion.AddSegment(segment);
    }
    for (const auto& temperatureMeta : temperatureMetas) {
        newVersion.AddSegTemperatureMeta(temperatureMeta);
    }
    for (const auto& segStats : segStatsVec) {
        newVersion.AddSegmentStatistics(segStats);
    }

    ProgressSynchronizer ps;
    ps.Init(versions);
    newVersion.SetVersionId(baseVersion.GetVersionId() + 1);
    newVersion.SetTimestamp(ps.GetTimestamp());
    newVersion.SetLocator(ps.GetLocator());
    newVersion.SetFormatVersion(ps.GetFormatVersion());
    return newVersion;
}

void ParallelPartitionDataMerger::CreateMergeSegmentForParallelInstance(const vector<VersionPair>& versionInfos,
                                                                        segmentid_t lastParentSegId,
                                                                        Version& newVersion)
{
    IE_LOG(INFO, "Begin create merged segment for BEGIN MERGE");
    segmentid_t mergeNewSegment = newVersion.GetLastSegment() + 1;
    Version version = newVersion;
    newVersion.AddSegment(mergeNewSegment);
    if (!newVersion.GetSegTemperatureMetas().empty()) {
        SegmentTemperatureMeta meta = SegmentTemperatureMeta::GenerateDefaultSegmentMeta();
        meta.segmentId = mergeNewSegment;
        newVersion.AddSegTemperatureMeta(meta);
    }
    string segmentDir = newVersion.GetSegmentDirName(mergeNewSegment);
    if (mRoot->IsExist(segmentDir)) {
        mRoot->RemoveDirectory(segmentDir);
    }
    DirectoryPtr directory = mRoot->MakeDirectory(segmentDir);
    if (mSchema->GetTableType() == tt_index) {
        MergeDeletionMapFile(versionInfos, directory, lastParentSegId);
    }

    SegmentInfo segmentInfo = CreateMergedSegmentInfo(versionInfos);
    if (!newVersion.GetSegTemperatureMetas().empty()) {
        SegmentTemperatureMeta meta = SegmentTemperatureMeta::GenerateDefaultSegmentMeta();
        meta.segmentId = mergeNewSegment;
        segmentInfo.AddDescription(SEGMENT_CUSTOMIZE_METRICS_GROUP, ToJsonString(meta.GenerateCustomizeMetric()));
    }
    if (mSchema->GetFileCompressSchema()) {
        uint64_t fingerPrint = mSchema->GetFileCompressSchema()->GetFingerPrint();
        segmentInfo.AddDescription(SEGMENT_COMPRESS_FINGER_PRINT, autil::StringUtil::toString(fingerPrint));
    }

    segmentInfo.Store(directory);
    if (mSchema->GetSubIndexPartitionSchema()) {
        // make sub segment directory if not exist.
        DirectoryPtr subDirectory = directory->MakeDirectory(SUB_SEGMENT_DIR_NAME);
        segmentInfo.Store(subDirectory);
        subDirectory->Close();
    }
    directory->Close();
    directory = mRoot->GetDirectory(segmentDir, true);

    // write counter after segment info, to load last segment deletionmap
    MergeCounterFile(newVersion, versionInfos, directory, lastParentSegId);
    // write deploy_index file after counter, to write counter info into deploy_index
    DeployIndexWrapper::DumpSegmentDeployIndex(directory, "");
    directory->Close();
    IE_LOG(INFO, "End Create merged segment [%d] of parallel build index", mergeNewSegment);
    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                                      "Create merged segment [%d] for parallel build index, segment info : %s",
                                      mergeNewSegment, segmentInfo.ToString(true).c_str());
}

void ParallelPartitionDataMerger::MergeDeletionMapFile(const vector<VersionPair>& versionInfos,
                                                       const DirectoryPtr& directory, segmentid_t lastParentSegId)
{
    DoMergeDeletionMapFile(versionInfos, directory, lastParentSegId, false);
    if (mSchema->GetSubIndexPartitionSchema() != NULL) {
        DirectoryPtr subDirectory = directory->MakeDirectory(SUB_SEGMENT_DIR_NAME);
        DoMergeDeletionMapFile(versionInfos, subDirectory, lastParentSegId, true);
    }
}

void ParallelPartitionDataMerger::DoMergeDeletionMapFile(const vector<VersionPair>& versionInfos,
                                                         const DirectoryPtr& directory, segmentid_t lastParentSegId,
                                                         bool isSubPartition)
{
    IE_LOG(INFO, "Begin Merge deletion map files in [%s]", directory->DebugString().c_str());
    file_system::DirectoryPtr delMapDirectory = directory->MakeDirectory(DELETION_MAP_DIR_NAME);
    map<segmentid_t, vector<PatchFileInfo>> toDeleteSegments;
    for (auto& versionPair : versionInfos) {
        DeletePatchInfos segments;
        CollectSegmentDeletionFileInfo(versionPair.first, segments, isSubPartition);
        for (auto& p : segments) {
            if (p.first > lastParentSegId) {
                continue;
            }
            toDeleteSegments[p.first].push_back(p.second);
        }
    }
    for (auto& infos : toDeleteSegments) {
        MergeSegmentDeletionMapFile(infos.first, delMapDirectory, infos.second);
    }
    IE_LOG(INFO, "End Merge deletion map files in [%s]", directory->DebugString().c_str());
}

void ParallelPartitionDataMerger::MergeSegmentDeletionMapFile(segmentid_t segmentId, const DirectoryPtr& directory,
                                                              const vector<index_base::PatchFileInfo>& files)
{
    if (files.size() == 1) {
        return;
    }
    DeletionMapSegmentWriter segmentWriter;
    segmentWriter.Init(files[0].patchDirectory, files[0].patchFileName, false);
    for (size_t i = 1; i < files.size(); i++) {
        auto fileReader = files[i].patchDirectory->CreateFileReader(files[i].patchFileName, file_system::FSOT_SLICE);
        if (!fileReader) {
            fileReader = files[i].patchDirectory->CreateFileReader(files[i].patchFileName, file_system::FSOT_MEM);
        }
        segmentWriter.MergeDeletionMapFile(fileReader);
    }
    segmentWriter.Dump(directory, segmentId, true);
}

void ParallelPartitionDataMerger::MergeCounterFile(const Version& newVersion, const vector<VersionPair>& versionInfos,
                                                   const DirectoryPtr& directory, segmentid_t lastParentSegId)
{
    const Version::SegmentIdVec& segIdVec = newVersion.GetSegmentVector();

    // segment(lastParentSegId) will not exist for recovered index
    auto GetValidParentLastSegmentId = [segIdVec](segmentid_t lastParentSegId) {
        int cursor = -1;
        for (auto segId : segIdVec) {
            if (segId > lastParentSegId) {
                break;
            }
            cursor++;
        }
        return cursor >= 0 ? segIdVec[cursor] : INVALID_SEGMENTID;
    };

    segmentid_t lastValidParentSegId = GetValidParentLastSegmentId(lastParentSegId);
    CounterMapPtr lastCounterMap(new CounterMap);
    if (lastValidParentSegId != INVALID_SEGMENTID) {
        std::string lastParentSegPath = newVersion.GetSegmentDirName(lastValidParentSegId);
        lastCounterMap = LoadCounterMap(lastParentSegPath);
    }
    DoMergeCounterFile(lastCounterMap, versionInfos);
    auto partData = CreateInstancePartitionData(newVersion, false);
    UpdateStateCounter(lastCounterMap, partData, false, mSchema->GetTableType() == tt_index);

    WriterOption counterWriterOption = WriterOption::AtomicDump();
    counterWriterOption.notInPackage = true;
    if (mSchema->GetSubIndexPartitionSchema() != NULL) {
        DirectoryPtr subDirectory = directory->MakeDirectory(SUB_SEGMENT_DIR_NAME);
        UpdateStateCounter(lastCounterMap, partData->GetSubPartitionData(), true, mSchema->GetTableType() == tt_index);
        subDirectory->Store(COUNTER_FILE_NAME, lastCounterMap->ToJsonString(), counterWriterOption);
    }
    directory->Store(COUNTER_FILE_NAME, lastCounterMap->ToJsonString(), counterWriterOption);
}

void ParallelPartitionDataMerger::DoMergeCounterFile(const CounterMapPtr& lastCounterMap,
                                                     const std::vector<VersionPair>& versionInfos)
{
    for (auto& versionPair : versionInfos) {
        const Version& version = versionPair.first;
        if (version.GetSegmentCount() == 0) {
            continue;
        }
        string segDir = version.GetSegmentDirName(version.GetSegmentVector().back());
        CounterMapPtr counterMap = LoadCounterMap(segDir);
        lastCounterMap->Merge(counterMap->ToJsonString(false), CounterBase::FJT_MERGE);
    }
}

void ParallelPartitionDataMerger::UpdateStateCounter(const CounterMapPtr& counterMap, const PartitionDataPtr& partData,
                                                     bool isSubPartition, bool needDeletionMap)
{
    assert(partData);
    auto partitionDocCounter = GetStateCounter(counterMap, "partitionDocCount", isSubPartition);
    auto deletedDocCounter = GetStateCounter(counterMap, "deletedDocCount", isSubPartition);

    const SegmentDataVector& segmentDatas = partData->GetSegmentDirectory()->GetSegmentDatas();
    SegmentDataVector::const_reverse_iterator it = segmentDatas.rbegin();
    int64_t docCount = 0;
    if (it != segmentDatas.rend()) {
        docCount = it->GetBaseDocId() + it->GetSegmentInfo()->docCount;
    }
    partitionDocCounter->Set(docCount);
    uint32_t delDocCount = 0;
    if (needDeletionMap) {
        DeletionMapReaderPtr deletionMapReader(new DeletionMapReader);
        deletionMapReader->Open(partData.get());
        delDocCount = deletionMapReader->GetDeletedDocCount();
    }
    deletedDocCounter->Set(delDocCount);

    if (isSubPartition) {
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                                          "sub table [%s]: partitionDocCount [%ld], deletedDocCount [%u]",
                                          mSchema->GetSchemaName().c_str(), docCount, delDocCount);
    } else {
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                                          "table [%s]: partitionDocCount [%ld], deletedDocCount [%u]",
                                          mSchema->GetSchemaName().c_str(), docCount, delDocCount);
    }
}

StateCounterPtr ParallelPartitionDataMerger::GetStateCounter(const CounterMapPtr& counterMap, const string& counterName,
                                                             bool isSub)
{
    string prefix = isSub ? "offline.sub_" : "offline.";
    string counterPath = prefix + counterName;
    auto stateCounter = counterMap->GetStateCounter(counterPath);
    if (!stateCounter) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "init counter[%s] failed", counterPath.c_str());
    }
    return stateCounter;
}

CounterMapPtr ParallelPartitionDataMerger::LoadCounterMap(const string& segmentDir)
{
    DirectoryPtr segDir = mRoot->GetDirectory(segmentDir, true);
    string counterString;
    segDir->Load(COUNTER_FILE_NAME, counterString, true);
    CounterMapPtr counterMap(new CounterMap);
    counterMap->FromJsonString(counterString);
    return counterMap;
}

void ParallelPartitionDataMerger::CollectSegmentDeletionFileInfo(const Version& version, DeletePatchInfos& segments,
                                                                 bool isSubPartition)
{
    PartitionDataPtr partData = CreateInstancePartitionData(version, isSubPartition);
    PatchFileFinderPtr patchFinder = PatchFileFinderCreator::Create(partData.get());
    patchFinder->FindDeletionMapFiles(segments);
}

SegmentInfo ParallelPartitionDataMerger::CreateMergedSegmentInfo(const vector<VersionPair>& versionInfos)
{
    vector<SegmentInfo> segInfos;
    uint32_t maxTTL = 0;
    size_t shardCount = 1;
    // set max TTL
    for (auto vp : versionInfos) {
        const Version& version = vp.first;
        auto levelInfo = version.GetLevelInfo();
        shardCount = std::max(shardCount, levelInfo.GetShardCount());

        if (version.GetSegmentCount() == 0) {
            continue;
        }
        string segDir = version.GetSegmentDirName(version.GetSegmentVector().back());
        DirectoryPtr segmentDirectory = mRoot->GetDirectory(segDir, true);
        SegmentInfo info;
        if (!info.Load(segmentDirectory)) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "segment [%d] has no segment info",
                                 version.GetSegmentVector().back());
        }
        maxTTL = max(maxTTL, info.maxTTL);
        segInfos.push_back(info);
    }

    SegmentInfo segInfo;
    segInfo.maxTTL = maxTTL;
    segInfo.shardCount = shardCount;

    // set ts & locator
    ProgressSynchronizer ps;
    ps.Init(segInfos);
    segInfo.timestamp = ps.GetTimestamp();
    indexlibv2::framework::Locator locator;
    locator.Deserialize(ps.GetLocator().ToString());
    segInfo.SetLocator(locator);
    segInfo.schemaId = mSchema->GetSchemaVersionId();
    return segInfo;
}

PartitionDataPtr ParallelPartitionDataMerger::CreateInstancePartitionData(const Version& version, bool isSubPartition)
{
    PartitionDataPtr partData = merger::MergePartitionDataCreator::CreateMergePartitionData(mRoot, mSchema, version);
    return isSubPartition ? partData->GetSubPartitionData() : partData;
}

vector<string> ParallelPartitionDataMerger::GetParallelInstancePaths(const string& rootDir,
                                                                     const ParallelBuildInfo& parallelInfo)
{
    vector<string> mergeSrc;
    for (size_t i = 0; i < parallelInfo.parallelNum; i++) {
        ParallelBuildInfo info = parallelInfo;
        info.instanceId = i;
        mergeSrc.push_back(info.GetParallelInstancePath(rootDir));
    }
    return mergeSrc;
}

string ParallelPartitionDataMerger::GetParentPath(const vector<string>& srcPaths)
{
    if (srcPaths.empty()) {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "number of parallel build instance path "
                             "must not be zero. partition path is [%s].",
                             mRoot->DebugString().c_str());
    }
    string parentPath = PathUtil::GetParentDirPath(srcPaths[0]);
    for (size_t i = 1; i < srcPaths.size(); i++) {
        string tmpParentPath = PathUtil::GetParentDirPath(srcPaths[i]);
        if (parentPath != tmpParentPath) {
            INDEXLIB_FATAL_ERROR(BadParameter, "parallel instance path [%s] does not has same parent path with [%s].",
                                 srcPaths[i].c_str(), srcPaths[0].c_str());
        }
    }
    return parentPath;
}

bool ParallelPartitionDataMerger::IsEmptyVersions(const vector<Version>& versions, const Version& lastVersion)
{
    if (lastVersion.GetVersionId() != INVALID_VERSION) {
        return false;
    }

    for (auto version : versions) {
        if (!version.GetSegmentVector().empty()) {
            return false;
        }
    }
    return true;
}

// todo : remove legacy code
versionid_t ParallelPartitionDataMerger::GetBaseVersionFromLagecyParallelBuild(const vector<Version>& versions)
{
    versionid_t vid = INVALID_VERSION;
    for (auto& version : versions) {
        if (version.GetVersionId() == INVALID_VERSION) {
            continue;
        }

        if (vid == INVALID_VERSION) {
            vid = version.GetVersionId();
        } else if (vid != version.GetVersionId()) {
            INDEXLIB_FATAL_ERROR(BadParameter,
                                 "legacy parallel build version must be same. "
                                 "version.%u and version.%u  are different",
                                 version.GetVersionId(), vid);
        }
    }
    if (vid != INVALID_VERSION) {
        return (vid - 1);
    }
    return INVALID_VERSION;
}
}} // namespace indexlib::merger
