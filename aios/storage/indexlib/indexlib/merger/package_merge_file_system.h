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
#pragma once

#include <map>
#include <memory>
#include <set>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/ThreadLocal.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "fslib/common/common_type.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/merger/merge_file_system.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace merger {

class PackageMergeFileSystem : public MergeFileSystem
{
public:
    PackageMergeFileSystem(const std::string& rootPath, const config::MergeConfig& mergeConfig, uint32_t instanceId,
                           const std::shared_ptr<file_system::IFileSystem>& fileSystem);
    ~PackageMergeFileSystem();

public:
    void Init(const std::vector<std::string>& targetSegmentPaths) override;
    void MakeDirectory(const std::string& absolutePath) override;
    void Commit() override;
    void Recover() override;

public:
    void MakeCheckpoint(const std::string& fileName) override;
    bool HasCheckpoint(const std::string& fileName) override;

private:
    uint32_t GetThreadId();
    MergeFileSystem::ThreadFileSystem* CreateFileSystem() override;
    int64_t GetLastTimestamp();
    void SetLastTimestamp(int64_t timestamp);
    void AddThreadCheckpoint(const std::string& fileName);
    void PopThreadCheckpoints(std::vector<std::string>& destition);
    int32_t Commit(MergeFileSystem::ThreadFileSystem* tfs);

private:
    static const std::string PACKAGE_MERGE_CHECKPOINT;
    class CheckpointManager : public autil::legacy::Jsonizable
    {
    public:
        void Recover(const file_system::ArchiveFolderPtr& folder, uint32_t threadCount);
        void MakeCheckpoint(const std::string& fileName);
        bool HasCheckpoint(const std::string& fileName) const;
        void Commit(const file_system::ArchiveFolderPtr& folder, const std::string& description, int32_t metaId);
        int32_t GetMetaId(const std::string& description) const;
        uint32_t GetThreadCount() const { return mThreadCount; }

    private:
        void Jsonize(JsonWrapper& json) override;
        void Store(const file_system::ArchiveFolderPtr& folder);
        void Load(const file_system::ArchiveFolderPtr& folder);

    private:
        std::set<std::string> mCheckpoints;
        std::map<std::string, int32_t> mDescriptionToMetaId;
        uint32_t mThreadCount;
    };

private:
    std::vector<std::string> mTargetSegmentPaths;
    std::vector<std::shared_ptr<file_system::IFileSystem>> mThreadFileSystems;
    std::map<std::string, fslib::FileList> mTargetSegmentFileList; // for optimize recover
    std::set<std::string> mNeedMakeDirectoryPaths;
    CheckpointManager mCheckpointManager;
    uint32_t mNextThreadId;
    bool mIsRecoverd;
    autil::RecursiveThreadMutex mLock;
    std::unique_ptr<autil::ThreadLocalPtr> mThreadLastTimestamp;
    std::unique_ptr<autil::ThreadLocalPtr> mThreadCheckpoints;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackageMergeFileSystem);
}} // namespace indexlib::merger
