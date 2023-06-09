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
#include "indexlib/table/index_task/merger/MergedVersionCommitOperation.h"

#include "autil/EnvUtil.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/config/BackgroundTaskConfig.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/file_system/package/MergePackageUtil.h"
#include "indexlib/file_system/package/PackageFileMeta.h"
#include "indexlib/file_system/package/PackageFileTagConfigList.h"
#include "indexlib/framework/SegmentStatistics.h"
#include "indexlib/framework/VersionCleaner.h"
#include "indexlib/framework/VersionCommitter.h"
#include "indexlib/framework/VersionCoord.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/framework/VersionValidator.h"
#include "indexlib/framework/cleaner/DropIndexCleaner.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/TruncateIndexNameMapper.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/MergePlan.h"
#include "indexlib/table/index_task/merger/MergeUtil.h"
#include "indexlib/util/PathUtil.h"

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, MergedVersionCommitOperation);

MergedVersionCommitOperation::MergedVersionCommitOperation(const framework::IndexOperationDescription& desc)
    : framework::IndexOperation(desc.GetId(), desc.UseOpFenceDir())
    , _desc(desc)
{
}

MergedVersionCommitOperation::~MergedVersionCommitOperation() {}

Status
MergedVersionCommitOperation::MountResourceDir(const std::shared_ptr<indexlib::file_system::Directory>& indexRoot,
                                               const std::string& resourceName)
{
    auto physicalDirectory = indexlib::file_system::Directory::GetPhysicalDirectory(indexRoot->GetPhysicalPath(""));
    Status status;
    bool isExist = false;
    std::tie(status, isExist) = physicalDirectory->GetIDirectory()->IsExist(resourceName).StatusWith();
    if (!status.IsOK()) {
        return status;
    }
    if (isExist) {
        return toStatus(indexRoot->GetFileSystem()->MountDir(indexRoot->GetPhysicalPath(""), resourceName, resourceName,
                                                             indexlib::file_system::FSMT_READ_WRITE, true));
    }
    return Status::OK();
}

framework::IndexOperationDescription MergedVersionCommitOperation::CreateOperationDescription(
    framework::IndexOperationId opId, const framework::Version& version, const std::string& patchIndexDir,
    const std::vector<DropIndexInfo>& dropIndexes)
{
    framework::IndexOperationDescription opDesc(opId, OPERATION_TYPE);
    opDesc.AddParameter(PARAM_TARGET_VERSION, version.ToString());
    if (!patchIndexDir.empty()) {
        opDesc.AddParameter(PATCH_INDEX_DIR, patchIndexDir);
    }
    if (dropIndexes.size() > 0) {
        opDesc.AddParameter(DROP_INDEX_INFO, ToJsonString(dropIndexes));
    }
    return opDesc;
}

Status MergedVersionCommitOperation::CollectInfoInDir(const std::shared_ptr<indexlib::file_system::IDirectory>& dir,
                                                      indexlib::file_system::MergePackageMeta* mergePackageMeta)
{
    std::vector<std::string> fileList;
    RETURN_IF_STATUS_ERROR(dir->ListDir("", indexlib::file_system::ListOption::Recursive(), fileList).Status(),
                           "list dir failed in dir[%s]", dir->GetPhysicalPath("").c_str());
    for (const auto& fileName : fileList) {
        auto [st, isDir] = dir->IsDir(fileName).StatusWith();
        RETURN_IF_STATUS_ERROR(st, "check dir[%s] failed in dir[%s]", fileName.c_str(),
                               dir->GetPhysicalPath("").c_str());
        if (!isDir) {
            ASSIGN_OR_RETURN(size_t size, dir->GetFileLength(fileName).StatusWith());
            std::map<std::string, size_t>& taggedFileSizeMap = (mergePackageMeta->packageTag2File2SizeMap)[/*tag=*/""];
            taggedFileSizeMap.emplace(dir->GetPhysicalPath(fileName), size);
        }
        std::string dirName = indexlib::util::PathUtil::GetDirectoryPath(fileName);
        if (!dirName.empty()) {
            mergePackageMeta->dirSet.emplace(dirName);
        }
    }
    return Status::OK();
}

Status MergedVersionCommitOperation::ConvertResourceDirToPackage(
    const framework::IndexTaskContext& context, const std::shared_ptr<indexlib::file_system::IDirectory>& resourceDir,
    const std::string& resourceDirName, const std::shared_ptr<indexlib::file_system::IDirectory>& currentOpFenceDir)
{
    indexlib::file_system::MergePackageMeta mergePackageMeta;
    RETURN_IF_STATUS_ERROR(CollectInfoInDir(resourceDir, &mergePackageMeta), "collect resource in dir[%s] failed",
                           resourceDir->GetPhysicalPath("").c_str());
    auto mergeConfig = context.GetTabletOptions()->GetOfflineConfig().GetMergeConfig();
    RETURN_IF_STATUS_ERROR(indexlib::file_system::MergePackageUtil::ConvertDirToPackage(
                               /*workingDirectory=*/currentOpFenceDir,
                               /*outputDirectory=*/resourceDir, /*parentDirName=*/resourceDirName, mergePackageMeta,
                               mergeConfig.GetPackageFileSizeThresholdBytes())
                               .Status(),
                           "convert resource[%s] to package failed", resourceDir->GetPhysicalPath("").c_str());
    return Status::OK();
}

Status MergedVersionCommitOperation::MountAndMaybePackageResourceDir(const framework::IndexTaskContext& context,
                                                                     const std::string& resourceDirName)
{
    auto indexRoot = context.GetIndexRoot();
    auto status = MountResourceDir(indexRoot, resourceDirName);
    RETURN_IF_STATUS_ERROR(status, "mount resource dir[%s] failed", resourceDirName.c_str());
    auto mergeConfig = context.GetTabletOptions()->GetOfflineConfig().GetMergeConfig();
    if (!mergeConfig.IsPackageFileEnabled()) {
        return Status::OK();
    }
    // Convert from resource dir to package only needs to be done once during full merge as of 2023/04. Inc merge
    // should not trigger this. This assumption can be changed if we need to support inc merge truncate
    // meta/adaptive bitmap update in the future.
    auto rootDirectory = context.GetIndexRoot()->GetIDirectory();
    ASSIGN_OR_RETURN(bool isExist, rootDirectory->IsExist(resourceDirName).StatusWith());
    if (!isExist) {
        AUTIL_LOG(INFO, "empty resource dir[%s], skip convert to package", resourceDirName.c_str());
        return Status::OK();
    }
    ASSIGN_OR_RETURN(std::shared_ptr<indexlib::file_system::IDirectory> resourceDir,
                     rootDirectory->GetDirectory(resourceDirName).StatusWith());
    auto [st1, currentOpFenceDir] = context.GetOpFenceRoot(_desc.GetId(), /*useOpFenceDir=*/true);
    if (!st1.IsOK()) {
        AUTIL_LOG(WARN, "get op fence dir failed, skip convert to package");
        return Status::OK();
    }
    auto [st2, isDone] = indexlib::file_system::MergePackageUtil::IsPackageConversionDone(resourceDir).StatusWith();
    RETURN_IF_STATUS_ERROR(st2, "check package conversion done failed");
    if (!isDone) {
        RETURN_IF_STATUS_ERROR(ConvertResourceDirToPackage(context, resourceDir, resourceDirName, currentOpFenceDir),
                               "convert resource[%s] to package failed", resourceDirName.c_str());
    }
    RETURN_IF_STATUS_ERROR(indexlib::file_system::MergePackageUtil::CleanSrcIndexFiles(
                               /*directoryToClean=*/resourceDir, /*directoryWithPackagingPlan=*/resourceDir)
                               .Status(),
                           "clean resource[%s] failed", resourceDirName.c_str());
    RETURN_IF_STATUS_ERROR(MountResourceDir(indexRoot, resourceDirName), "mount resource dir[%s] failed",
                           resourceDirName.c_str());
    return Status::OK();
}

Status MergedVersionCommitOperation::Execute(const framework::IndexTaskContext& context)
{
    RETURN_IF_STATUS_ERROR(MergeUtil::RewriteMergeConfig(context), "rewrite merge config failed");

    std::string versionStr;
    if (!_desc.GetParameter(PARAM_TARGET_VERSION, versionStr)) {
        AUTIL_LOG(ERROR, "get target segment dir from desc failed");
        return Status::Corruption("get target segment dir from desc failed");
    }
    framework::Version version;
    auto result = indexlib::file_system::JsonUtil::FromString(versionStr, &version);
    RETURN_IF_STATUS_ERROR(result.Status(), "parse version [%s] failed", versionStr.c_str());
    RETURN_IF_STATUS_ERROR(UpdateSegmentDescriptions(context.GetResourceManager(), &version),
                           "update segment description failed");
    auto indexRoot = context.GetIndexRoot();
    auto status = MountAndMaybePackageResourceDir(context, ADAPTIVE_DICT_DIR_NAME);
    RETURN_IF_STATUS_ERROR(status, "process adaptive dict dir failed");
    status = MountAndMaybePackageResourceDir(context, TRUNCATE_META_DIR_NAME);
    RETURN_IF_STATUS_ERROR(status, "process index meta dir failed");

    auto [st, exist] = indexRoot->GetIDirectory()->IsExist(TRUNCATE_META_DIR_NAME).StatusWith();
    RETURN_IF_STATUS_ERROR(st, "get truncate meta dir failed");
    if (exist) {
        auto truncateMetaDir = indexRoot->GetIDirectory()->GetDirectory(TRUNCATE_META_DIR_NAME).Value();
        config::TruncateIndexNameMapper truncateIndexMapper(truncateMetaDir);
        auto schema = context.GetTabletSchema();
        status = truncateIndexMapper.Dump(schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR));
        RETURN_IF_STATUS_ERROR(status, "dump truncate index mapper failed.");
    }

    auto tabletData = context.GetTabletData();
    const auto onDiskVersion = tabletData->GetOnDiskVersion();
    if (!context.NeedSwitchIndexPath() && onDiskVersion.GetVersionId() != INVALID_VERSIONID) {
        const auto versionRoot = PathUtil::JoinPath(indexRoot->GetPhysicalPath(""), onDiskVersion.GetFenceName());
        status = indexRoot->GetFileSystem()
                     ->MountVersion(versionRoot, onDiskVersion.GetVersionId(), /*rawLogicalPath*/ "",
                                    indexlib::file_system::FSMT_READ_ONLY, nullptr)
                     .Status();
        RETURN_IF_STATUS_ERROR(status, "mount disk version[%d] failed. version root[%s]", onDiskVersion.GetVersionId(),
                               versionRoot.data());
    }
    auto targetSchema = tabletData->GetTabletSchema(version.GetReadSchemaId());
    status = framework::DropIndexCleaner::CleanIndexInLogical(tabletData, targetSchema, indexRoot->GetIDirectory());
    RETURN_IF_STATUS_ERROR(status, "unmount useless index failed");
    status = MountPatchIndexDir(indexRoot);
    RETURN_IF_STATUS_ERROR(status, "mount patch dir failed");

    for (auto [segmentId, _] : version) {
        if (!onDiskVersion.HasSegment(segmentId)) {
            const auto segDir = version.GetSegmentDirName(segmentId);
            auto status = toStatus(indexRoot->GetFileSystem()->MountDir(indexRoot->GetPhysicalPath(""), segDir, segDir,
                                                                        indexlib::file_system::FSMT_READ_WRITE, true));
            RETURN_IF_STATUS_ERROR(status, "mount segment [%s] failed", segDir.c_str());
        }
    }

    auto iIndexRoot = indexRoot->GetIDirectory();
    std::stringstream ss;
    ss << VERSION_FILE_NAME_PREFIX << "." << version.GetVersionId();
    auto [existStatus, isExist] = iIndexRoot->IsExist(ss.str()).StatusWith();
    RETURN_IF_STATUS_ERROR(existStatus, "check version name exist failed");
    std::string indexRootPath = context.GetIndexRoot()->GetPhysicalPath("");
    if (isExist) {
        AUTIL_LOG(INFO, "version already exist, return directly");
        return ValidateVersion(indexRootPath, targetSchema, version.GetVersionId());
    }
    version.SetCommitTime(autil::TimeUtility::currentTime());
    status = framework::VersionCommitter::Commit(version, indexRoot, {});
    RETURN_IF_STATUS_ERROR(status, "commit version failed");

    bool needCleanOldVersions = true;
    context.GetParameter(NEED_CLEAN_OLD_VERSIONS, needCleanOldVersions);
    if (needCleanOldVersions) {
        const auto& options = context.GetTabletOptions();
        auto keepVersionCount = options->GetBuildConfig().GetKeepVersionCount();
        auto keepVersionHour = options->GetBuildConfig().GetKeepVersionHour();
        auto fenceTsTolerantDeviation = options->GetBuildConfig().GetFenceTsTolerantDeviation();
        std::string reservedVersionStr;
        std::set<framework::VersionCoord> reservedVersions;
        if (context.GetParameter(RESERVED_VERSION_COORD_SET, reservedVersionStr) && !reservedVersionStr.empty()) {
            auto result = indexlib::file_system::JsonUtil::FromString(reservedVersionStr, &reservedVersions);
            RETURN_IF_STATUS_ERROR(result.Status(), "parse reserved version set str [%s] failed",
                                   reservedVersionStr.c_str());
        }
        reservedVersions.emplace(version.GetVersionId(), version.GetFenceName());
        auto physicalDir = indexlib::file_system::Directory::GetPhysicalDirectory(indexRootPath);
        versionid_t maxBuildVersion = context.GetTabletData()->GetOnDiskVersion().GetVersionId();
        framework::VersionCleaner cleaner;
        framework::VersionCleaner::VersionCleanerOptions cleanerOptions;
        cleanerOptions.keepVersionCount = keepVersionCount;
        cleanerOptions.keepVersionHour = keepVersionHour;
        cleanerOptions.fenceTsTolerantDeviation = fenceTsTolerantDeviation;
        cleanerOptions.currentMaxVersionId = maxBuildVersion;
        status = cleaner.Clean(physicalDir->GetIDirectory(), cleanerOptions, reservedVersions);
        RETURN_IF_STATUS_ERROR(status, "clean versions failed");
        status = ValidateVersion(indexRootPath, targetSchema, version.GetVersionId());
    }
    RETURN_IF_STATUS_ERROR(status, "validate version failed");
    return Status::OK();
}

Status
MergedVersionCommitOperation::MountPatchIndexDir(const std::shared_ptr<indexlib::file_system::Directory>& indexRoot)
{
    std::string patchIndexDir;
    if (!_desc.GetParameter(PATCH_INDEX_DIR, patchIndexDir)) {
        return Status::OK();
    }
    // must use physical directory, not use file system
    auto root = indexlib::file_system::Directory::GetPhysicalDirectory(indexRoot->GetPhysicalPath(""));
    auto [status, patchDirectory] = root->GetIDirectory()->GetDirectory(patchIndexDir).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "get patch directory [%s] failed", patchIndexDir.c_str());
    fslib::FileList fileList;
    std::shared_ptr<indexlib::file_system::Directory> dir(new indexlib::file_system::Directory(patchDirectory));
    status = framework::VersionLoader::ListSegment(dir, &fileList);
    RETURN_IF_STATUS_ERROR(status, "list segment failed");
    auto mountOption = indexlib::file_system::MountDirOption(indexlib::file_system::FSMT_READ_WRITE);
    mountOption.conflictResolution = indexlib::file_system::ConflictResolution::SKIP;
    for (auto segmentDir : fileList) {
        std::string segmentPath = PathUtil::JoinPath(patchIndexDir, segmentDir);
        status = toStatus(indexRoot->GetFileSystem()->MountDir(indexRoot->GetPhysicalPath(""), segmentPath, segmentDir,
                                                               mountOption, /*enableLazyMount*/ false));
        RETURN_IF_STATUS_ERROR(status, "mount segment dir failed");
    }
    return Status::OK();
}

Status MergedVersionCommitOperation::ValidateVersion(const std::string& indexRootPath,
                                                     const std::shared_ptr<config::TabletSchema>& schema,
                                                     versionid_t versionId)
{
    if (!autil::EnvUtil::getEnv<bool>("VALIDATE_MERGED_VERSION", /*defaultValue*/ "true")) {
        AUTIL_LOG(INFO, "skip merged version validation");
        return Status::OK();
    }
    return framework::VersionValidator::Validate(indexRootPath, schema, versionId);
}

Status MergedVersionCommitOperation::UpdateSegmentDescriptions(
    const std::shared_ptr<framework::IndexTaskResourceManager>& resourceManager, framework::Version* targetVersion)
{
    // TODO(tianxiao) need to get stastics from dfs
    if (!resourceManager) {
        AUTIL_LOG(ERROR, "resource manager is nullptr");
        return Status::InvalidArgs("resource manager is nullptr");
    }
    std::shared_ptr<MergePlan> mergePlan;
    auto status = resourceManager->LoadResource<MergePlan>(/*name=*/MERGE_PLAN, /*type=*/MERGE_PLAN, mergePlan);
    if (status.IsNoEntry()) {
        AUTIL_LOG(INFO, "merge pkan not found");
        return Status::OK();
    }
    RETURN_IF_STATUS_ERROR(status, "load resouce failed");
    std::vector<framework::SegmentStatistics> toAppendStatistics;
    for (size_t i = 0; i < mergePlan->Size(); ++i) {
        auto segmentMergePlan = mergePlan->GetSegmentMergePlan(i);
        for (size_t segmentIdx = 0; segmentIdx < segmentMergePlan.GetTargetSegmentCount(); ++segmentIdx) {
            auto [status, segStat] = segmentMergePlan.GetTargetSegmentInfo(segmentIdx).GetSegmentStatistics();
            RETURN_IF_STATUS_ERROR(status, "collect segment statistics failed");
            if (!segStat.empty()) {
                toAppendStatistics.push_back(segStat);
            }
        }
    }
    std::sort(toAppendStatistics.begin(), toAppendStatistics.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.GetSegmentId() < rhs.GetSegmentId(); });
    auto mutableSegmentDescriptions = targetVersion->GetSegmentDescriptions();
    assert(mutableSegmentDescriptions);
    auto targetStats = mutableSegmentDescriptions->GetSegmentStatisticsVector();
    targetStats.insert(targetStats.end(), toAppendStatistics.begin(), toAppendStatistics.end());
    mutableSegmentDescriptions->SetSegmentStatistics(targetStats);
    return Status::OK();
}

}} // namespace indexlibv2::table
