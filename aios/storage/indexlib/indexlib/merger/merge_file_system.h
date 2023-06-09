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
#ifndef __INDEXLIB_MERGE_FILE_SYSTEM_H
#define __INDEXLIB_MERGE_FILE_SYSTEM_H

#include <memory>

#include "autil/ThreadLocal.h"
#include "indexlib/common_define.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/metrics/MetricProvider.h"

DECLARE_REFERENCE_CLASS(merger, MergeFileSystem);
DECLARE_REFERENCE_CLASS(index_base, BranchFS);
DECLARE_REFERENCE_CLASS(file_system, IFileSystem);

namespace indexlib { namespace merger {

class MergeFileSystem
{
public:
    static MergeFileSystemPtr Create(const std::string& rootPath, const config::MergeConfig& mergeConfig,
                                     uint32_t instanceId, const file_system::IFileSystemPtr& fileSystem);

public:
    MergeFileSystem(const std::string& rootPath, const config::MergeConfig& mergeConfig, uint32_t instanceId,
                    const file_system::IFileSystemPtr& fileSystem);
    virtual ~MergeFileSystem();

public:
    virtual void Init(const std::vector<std::string>& targetSegmentPaths); // relativePath
    virtual void Close();
    virtual void Commit();
    virtual void MakeDirectory(const std::string& relativePath);
    virtual void Recover() {}
    file_system::DirectoryPtr GetDirectory(const std::string& relativePath);
    const std::string& GetRootPath() { return mRootPath; }

    file_system::ArchiveFolderPtr CreateArchiveFolder(file_system::DirectoryPtr dir, const std::string& fileName,
                                                      bool forceUseArchiveFile);
    file_system::ArchiveFolderPtr CreateArchiveFolder(const std::string& absolutePath, bool forceUseArchiveFile);
    file_system::ArchiveFolderPtr CreateLocalArchiveFolder(const std::string relativePath, bool forceUseArchiveFile);

public:
    virtual void MakeCheckpoint(const std::string& fileName);
    virtual bool HasCheckpoint(const std::string& fileName);

protected:
    struct ThreadFileSystem {
        file_system::IFileSystemPtr fileSystem;
        std::string description;
    };

protected:
    virtual ThreadFileSystem* CreateFileSystem();

protected:
    const file_system::IFileSystemPtr& GetFileSystem();
    const std::string& GetDescription();
    ThreadFileSystem* GetThreadFileSystem();

protected:
    std::string mRootPath;
    config::MergeConfig mMergeConfig;
    util::MetricProviderPtr mMetricProvider;
    std::map<std::string, file_system::ArchiveFolderPtr> mArchiveFolders;
    autil::RecursiveThreadMutex mLock;
    uint32_t mInstanceId;
    file_system::IFileSystemPtr mFileSystem;
    std::unique_ptr<autil::ThreadLocalPtr> mThreadFileSystem;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeFileSystem);
}} // namespace indexlib::merger

#endif //__INDEXLIB_MERGE_FILE_SYSTEM_H
