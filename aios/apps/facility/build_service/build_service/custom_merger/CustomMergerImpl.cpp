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

#include "build_service/custom_merger/CustomMergerImpl.h"

#include "build_service/custom_merger/MergeResourceProvider.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/config/schema_differ.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/MergeDirsOption.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/merger/merger_branch_hinter.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace build_service::util;
using namespace indexlib::config;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::merger;

namespace build_service { namespace custom_merger {
BS_LOG_SETUP(custom_merger, CustomMergerImpl);

const std::string CustomMergerImpl::MERGE_INSTANCE_DIR_PREFIX = "instance_";

CustomMergerImpl::CustomMergerImpl(uint32_t backupId, const string& epochId) : _backupId(backupId), _epochId(epochId)
{
    _fileSystemOptions.isOffline = true;
    _fileSystemOptions.useCache = false;
}

CustomMergerImpl::~CustomMergerImpl() {}

bool CustomMergerImpl::init(CustomMergerInitParam& param)
{
    _param = param;
    return true;
}

bool CustomMergerImpl::merge(const CustomMergeMeta& mergeMeta, size_t instanceId, const std::string& indexPath)
{
    CustomMergePlan plan;
    if (!mergeMeta.getMergePlan(instanceId, plan)) {
        BS_LOG(INFO, "no merge plan for instance id : %ld.", instanceId);
        return true;
    }
    string mergeDir = prepareInstanceDir(instanceId, indexPath);

    MergerBranchHinter hinter(CommonBranchHinterOption::Normal(_backupId, _epochId));
    auto branchFileSystem = BranchFS::CreateWithAutoFork(mergeDir, _fileSystemOptions, nullptr, &hinter);

    auto mergeOutputDirectory = branchFileSystem->GetRootDirectory();
    if (!mergeOutputDirectory->IsExist(ALTER_FIELD_CHECK_POINT_DIR_NAME)) {
        mergeOutputDirectory->MakeDirectory(ALTER_FIELD_CHECK_POINT_DIR_NAME);
    }
    auto checkpointDir = mergeOutputDirectory->GetDirectory(ALTER_FIELD_CHECK_POINT_DIR_NAME, true);
    _checkpointFolder.reset(new ArchiveFolder(false));
    _checkpointFolder->Open(checkpointDir->GetIDirectory()).GetOrThrow();
    vector<CustomMergePlan::TaskItem> tasks;
    plan.getTaskItems(tasks);
    for (auto& item : tasks) {
        if (hasCheckpoint(item, _checkpointFolder)) {
            continue;
        }
        if (!doMergeTask(item, mergeOutputDirectory)) {
            return false;
        }
        if (!commitCheckpoint(item, _checkpointFolder)) {
            return false;
        }
    }
    branchFileSystem->UpdatePreloadDependence();
    branchFileSystem->CommitToDefaultBranch();
    _checkpointFolder->Close().GetOrThrow();
    return true;
}

bool CustomMergerImpl::hasCheckpoint(const CustomMergePlan::TaskItem& taskItem,
                                     const indexlib::file_system::ArchiveFolderPtr& checkpointFolder)
{
    if (taskItem.taskIdx == -1) {
        return false;
    }
    return checkpointFolder->IsExist(taskItem.getTaskCheckpointName()).GetOrThrow();
}

bool CustomMergerImpl::commitCheckpoint(const CustomMergePlan::TaskItem& taskItem,
                                        const indexlib::file_system::ArchiveFolderPtr& checkpointFolder)
{
    if (taskItem.taskIdx == -1) {
        return true; // do nothing
    }
    indexlib::file_system::FileWriterPtr fileWriter =
        checkpointFolder->CreateFileWriter(taskItem.getTaskCheckpointName()).GetOrThrow();
    fileWriter->Close().GetOrThrow();
    return true;
}

bool CustomMergerImpl::endMerge(const CustomMergeMeta& mergeMeta, const std::string& path, int32_t targetVersionId)
{
    MergeResourceProviderPtr provider = _param.resourceProvider;
    PartitionResourceProviderPtr partitionProvider = provider->getPartitionResourceProvider();

    IndexPartitionSchemaPtr newSchema = provider->getNewSchema();
    // string targetDir = FslibWrapper::JoinPath(path, partitionProvider->GetPatchRootDirName(newSchema));
    auto branchFs = BranchFS::Create(path);
    branchFs->Init(nullptr, _fileSystemOptions);
    auto rootDirectory = branchFs->GetRootDirectory();
    string targetDirName = partitionProvider->GetPatchRootDirName(newSchema);
    if (!rootDirectory->IsExist(targetDirName)) {
        rootDirectory->MakeDirectory(targetDirName);
    }
    auto targetDirectory = rootDirectory->GetDirectory(targetDirName, true);
    std::vector<std::string> instanceDirs = listInstanceDirs(path);
    indexlib::file_system::DirectoryVector instanceBranchDirectorys;

    MergerBranchHinter hinter(CommonBranchHinterOption::Normal(_backupId, _epochId));
    for (const std::string& instanceDirPathName : instanceDirs) {
        string branchName;
        instanceBranchDirectorys.emplace_back(
            BranchFS::GetDirectoryFromDefaultBranch(instanceDirPathName, _fileSystemOptions, &hinter, &branchName));
        if (!hinter.CanOperateBranch(branchName)) {
            IE_LOG(ERROR, "old worker with epochId [%s] cannot move new branch [%s] with epochId [%s]",
                   hinter.GetOption().epochId.c_str(), branchName.c_str(),
                   CommonBranchHinter::ExtractEpochFromBranch(branchName).c_str());
            return false;
        }
    }
    vector<TaskItemDispatcher::SegmentInfo> segmentInfos;
    provider->getIndexSegmentInfos(segmentInfos);
    Version targetVersion = partitionProvider->GetVersion();

    FenceContextPtr fenceContext(FslibWrapper::CreateFenceContext(path, hinter.GetOption().epochId));
    for (const auto& info : segmentInfos) {
        segmentid_t targetSegmentId = info.segmentId;
        string segName = targetVersion.GetSegmentDirName(targetSegmentId);
        for (const auto& instanceDir : instanceBranchDirectorys) {
            if (instanceDir->IsExist(segName)) {
                auto segmentDir = instanceDir->GetDirectory(segName, true);
                // moved file must be checkpoint level
                mergeSegmentDir(rootDirectory, segmentDir, PathUtil::JoinPath(targetDirName, segName),
                                fenceContext.get());
            }
        }
        THROW_IF_FS_ERROR(rootDirectory->GetFileSystem()->MergePackageFiles(PathUtil::JoinPath(targetDirName, segName),
                                                                            fenceContext.get()),
                          "merge package files failed");
        if (!instanceDirs.empty()) {
            partitionProvider->MountPatchIndex(path, partitionProvider->GetPatchRootDirName(newSchema));
        }
    }

    if (targetVersionId != INVALID_VERSION) {
        // TODO (yiping.typ) add fence option
        partitionProvider->StoreVersion(newSchema, targetVersionId);
    } else {
        // for ops alter field not create version, but generate segment list
        partitionProvider->DumpDeployIndexForPatchSegments(provider->getNewSchema());
    }
    partitionProvider->Sync();
    for (const auto& instanceDir : instanceDirs) {
        FslibWrapper::DeleteDirE(instanceDir, DeleteOption::Fence(fenceContext.get(), false));
    }
    return true;
}

void CustomMergerImpl::mergeSegmentDir(const indexlib::file_system::DirectoryPtr& rootDirectory,
                                       const indexlib::file_system::DirectoryPtr& segmentDirectory,
                                       const string& segmentPath, FenceContext* fenceContext)
{
    FileList fileList;
    segmentDirectory->ListDir("", fileList, false);
    for (const auto& fileName : fileList) {
        if (fileName == SUB_SEGMENT_DIR_NAME) {
            continue;
        } else {
            if (segmentDirectory->IsDir(fileName)) {
                if (!rootDirectory->IsExist(PathUtil::JoinPath(segmentPath, fileName))) {
                    rootDirectory->MakeDirectory(PathUtil::JoinPath(segmentPath, fileName));
                }
                FileList subFileList;
                segmentDirectory->ListDir(fileName, subFileList, false);
                for (const auto& subFileName : subFileList) {
                    string physicalPath = segmentDirectory->GetPhysicalPath(PathUtil::JoinPath(fileName, subFileName));
                    THROW_IF_FS_ERROR(rootDirectory->GetFileSystem()->MergeDirs(
                                          {physicalPath}, PathUtil::JoinPath(segmentPath, fileName, subFileName),
                                          MergeDirsOption::NoMergePackageWithFence(fenceContext)),
                                      "merge dirs failed");
                }
            } else {
                string physicalPath = segmentDirectory->GetPhysicalPath(fileName);
                THROW_IF_FS_ERROR(
                    rootDirectory->GetFileSystem()->MergeDirs({physicalPath}, PathUtil::JoinPath(segmentPath, fileName),
                                                              MergeDirsOption::NoMergePackageWithFence(fenceContext)),
                    "merge dirs failed");
            }
        }
    }
}

uint32_t CustomMergerImpl::getOperationId()
{
    auto newSchema = _param.resourceProvider->getNewSchema();
    if (newSchema->HasModifyOperations()) {
        return newSchema->GetSchemaVersionId();
    }
    return 0;
}

string CustomMergerImpl::getInstanceDirPrefix()
{
    uint32_t operationId = getOperationId();
    string instanceDirPrefix;
    if (operationId == 0) {
        instanceDirPrefix = MERGE_INSTANCE_DIR_PREFIX;
    } else {
        instanceDirPrefix = autil::StringUtil::toString(operationId) + "_" + MERGE_INSTANCE_DIR_PREFIX;
    }
    return instanceDirPrefix;
}

std::string CustomMergerImpl::prepareInstanceDir(size_t instanceId, const std::string& indexRoot)
{
    string instanceDir =
        FslibWrapper::JoinPath(indexRoot, getInstanceDirPrefix() + "_" + autil::StringUtil::toString(instanceId));
    if (!FslibWrapper::IsExist(instanceDir).GetOrThrow()) {
        if (_backupId) {
            AUTIL_LEGACY_THROW(indexlib::util::NonExistException,
                               "Instance Path [%s] not exist, wait for branch[0] worker to create");
        }
        FslibWrapper::MkDirE(instanceDir, true);
    }
    return instanceDir;
}

std::vector<std::string> CustomMergerImpl::listInstanceDirs(const std::string& rootPath)
{
    std::vector<std::string> fileList;
    FslibWrapper::ListDirE(rootPath, fileList);

    std::vector<std::string> instanceDirList;
    string instanceDirPrefix = getInstanceDirPrefix();
    for (size_t i = 0; i < fileList.size(); i++) {
        string path = FslibWrapper::JoinPath(rootPath, fileList[i]);
        if (FslibWrapper::IsDir(path).GetOrThrow() && 0 == fileList[i].find(instanceDirPrefix)) {
            instanceDirList.push_back(path);
        }
    }
    return instanceDirList;
}

map<string, AttributeConfigPtr> CustomMergerImpl::getNewAttributes() const
{
    IndexPartitionSchemaPtr newSchema = _param.resourceProvider->getNewSchema();
    IndexPartitionSchemaPtr oldSchema = _param.resourceProvider->getOldSchema();
    assert(newSchema.get() != nullptr);
    assert(oldSchema.get() != nullptr);
    vector<AttributeConfigPtr> newAttrs;
    vector<IndexConfigPtr> newIndexes;
    string errorMsg;
    SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, newAttrs, newIndexes, errorMsg);
    map<string, AttributeConfigPtr> newAttrMap;
    for (const auto& it : newAttrs) {
        newAttrMap[it->GetAttrName()] = it;
    }
    return newAttrMap;
}

MergeResourceProviderPtr CustomMergerImpl::getResourceProvider() const { return _param.resourceProvider; }

}} // namespace build_service::custom_merger
