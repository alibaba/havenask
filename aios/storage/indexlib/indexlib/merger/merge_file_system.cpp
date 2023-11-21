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
#include "indexlib/merger/merge_file_system.h"

#include <assert.h>
#include <iosfwd>
#include <utility>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/fslib/IoConfig.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/index_define.h"
#include "indexlib/merger/index_merge_meta.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/merge_scheduler.h"
#include "indexlib/merger/package_merge_file_system.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MergeFileSystem);

MergeFileSystemPtr MergeFileSystem::Create(const string& rootPath, const MergeConfig& mergeConfig, uint32_t instanceId,
                                           const IFileSystemPtr& fileSystem)
{
    assert(StringUtil::endsWith(rootPath,
                                IndexPartitionMerger::MERGE_INSTANCE_DIR_PREFIX + StringUtil::toString(instanceId)));
    if (mergeConfig.GetEnablePackageFile()) {
        PackageMergeFileSystemPtr packageMergeFileSystem(
            new PackageMergeFileSystem(rootPath, mergeConfig, instanceId, fileSystem));
        return packageMergeFileSystem;
    }
    MergeFileSystemPtr mergeFileSystem(new MergeFileSystem(rootPath, mergeConfig, instanceId, fileSystem));
    return mergeFileSystem;
}

MergeFileSystem::MergeFileSystem(const string& rootPath, const MergeConfig& mergeConfig, uint32_t instanceId,
                                 const IFileSystemPtr& fileSystem)
    : mRootPath(PathUtil::NormalizePath(rootPath))
    , mMergeConfig(mergeConfig)
    , mInstanceId(instanceId)
    , mFileSystem(fileSystem)
    , mThreadFileSystem(new ThreadLocalPtr([](void* p) { delete (ThreadFileSystem*)p; }))
{
}

MergeFileSystem::~MergeFileSystem() {}

void MergeFileSystem::Init(const vector<string>& targetSegmentPaths)
{
    for (auto segmentPath : targetSegmentPaths) {
        if (!mFileSystem->IsExist(segmentPath).GetOrThrow(segmentPath)) {
            THROW_IF_FS_ERROR(mFileSystem->MakeDirectory(segmentPath, DirectoryOption()), "mkdir [%s] failed",
                              segmentPath.c_str());
        }
    }
    if (!mFileSystem->IsExist(MERGE_ITEM_CHECK_POINT_DIR_NAME).GetOrThrow(MERGE_ITEM_CHECK_POINT_DIR_NAME)) {
        THROW_IF_FS_ERROR(mFileSystem->MakeDirectory(MERGE_ITEM_CHECK_POINT_DIR_NAME, DirectoryOption()),
                          "mkdir [%s] failed", MERGE_ITEM_CHECK_POINT_DIR_NAME);
    }
    Recover();
}

void MergeFileSystem::Commit() { IE_LOG(INFO, "commit file system"); }

void MergeFileSystem::Close()
{
    IE_LOG(INFO, "close file system");
    for (auto iter = mArchiveFolders.begin(); iter != mArchiveFolders.end(); iter++) {
        iter->second->Close().GetOrThrow();
    }
}

void MergeFileSystem::MakeDirectory(const string& relativePath)
{
    auto fs = GetFileSystem();
    THROW_IF_FS_ERROR(fs->MakeDirectory(relativePath, DirectoryOption()), "mkdir [%s] failed", relativePath.c_str());
}

DirectoryPtr MergeFileSystem::GetDirectory(const string& relativePath)
{
    return Directory::Get(GetFileSystem())->GetDirectory(relativePath, true);
}

void MergeFileSystem::MakeCheckpoint(const string& fileName)
{
    ArchiveFolderPtr folder = CreateArchiveFolder(GetDirectory(""), MERGE_ITEM_CHECK_POINT_DIR_NAME, false);
    FileWriterPtr fileWriter = folder->CreateFileWriter(fileName).GetOrThrow();
    fileWriter->Close().GetOrThrow();
}

bool MergeFileSystem::HasCheckpoint(const string& fileName)
{
    if (fileName.empty()) {
        return true;
    }
    ArchiveFolderPtr folder = CreateArchiveFolder(GetDirectory(""), MERGE_ITEM_CHECK_POINT_DIR_NAME, false);
    if (folder) {
        return folder->IsExist(fileName).GetOrThrow();
    }
    return false;
}

MergeFileSystem::ThreadFileSystem* MergeFileSystem::CreateFileSystem()
{
    auto tfs = std::make_unique<ThreadFileSystem>();
    tfs->fileSystem = mFileSystem->CreateThreadOwnFileSystem("").GetOrThrow();
    return tfs.release();
}

MergeFileSystem::ThreadFileSystem* MergeFileSystem::GetThreadFileSystem()
{
    auto tfs = static_cast<ThreadFileSystem*>(mThreadFileSystem->Get());
    if (tfs == nullptr) {
        tfs = CreateFileSystem();
        mThreadFileSystem->Reset(tfs);
    }
    return tfs;
}

ArchiveFolderPtr MergeFileSystem::CreateArchiveFolder(file_system::DirectoryPtr dir, const std::string& fileName,
                                                      bool forceUseArchiveFile)
{
    // checkPoint files shoule be each instance branch self-own
    return CreateArchiveFolder(PathUtil::JoinPath(dir->GetFileSystem()->GetOutputRootPath(), fileName),
                               forceUseArchiveFile);
}

ArchiveFolderPtr MergeFileSystem::CreateArchiveFolder(const std::string& absolutePath, bool forceUseArchiveFile)
{
    ScopedLock lock(mLock);
    file_system::ErrorCode ec = FslibWrapper::MkDirIfNotExist(absolutePath).Code();
    THROW_IF_FS_ERROR(ec, "Get archive folder [%s] failed", absolutePath.c_str());
    if (mArchiveFolders.find(absolutePath) != mArchiveFolders.end()) {
        return mArchiveFolders.find(absolutePath)->second;
    }
    bool legacyMode = forceUseArchiveFile ? false : !mMergeConfig.IsArchiveFileEnable();
    auto fs =
        FileSystemCreator::Create("archive-" + std::to_string(mInstanceId), absolutePath, FileSystemOptions::Offline())
            .GetOrThrow();
    ArchiveFolderPtr archiveFolder =
        Directory::Get(fs)->CreateArchiveFolder(legacyMode, StringUtil::toString(mInstanceId));
    if (!archiveFolder) {
        THROW_IF_FS_ERROR(FSEC_ERROR, "Get archive folder [%s] failed", absolutePath.c_str());
    }
    mArchiveFolders[absolutePath] = archiveFolder;
    return archiveFolder;
}

ArchiveFolderPtr MergeFileSystem::CreateLocalArchiveFolder(const std::string relativePath, bool forceUseArchiveFile)
{
    DirectoryPtr localRootDir = Directory::Get(mFileSystem);
    return CreateArchiveFolder(PathUtil::JoinPath(localRootDir->GetOutputPath(), relativePath), forceUseArchiveFile);
}

const IFileSystemPtr& MergeFileSystem::GetFileSystem() { return GetThreadFileSystem()->fileSystem; }

const string& MergeFileSystem::GetDescription() { return GetThreadFileSystem()->description; }
}} // namespace indexlib::merger
