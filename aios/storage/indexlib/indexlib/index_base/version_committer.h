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

#include <memory>
#include <set>

#include "fslib/common/common_type.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/PhysicalDirectory.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/index_summary.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/patch/partition_patch_meta_creator.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib { namespace index_base {

class VersionCommitter
{
public:
    // VersionCommitter(const std::string& rootDir,
    //                  const Version& version,
    //                  uint32_t keepVersionCount,
    //                  const std::set<versionid_t>& reservedVersionSet = {})
    //     : mVersion(version)
    //     , mKeepVersionCount(keepVersionCount)
    //     , mReservedVersionSet(reservedVersionSet)
    // {
    //     mRootDir = util::PathUtil::NormalizeDir(rootDir);
    //     mFsRootDir = file_system::DirectoryPtr();
    // }

    VersionCommitter(const file_system::DirectoryPtr& rootDir, const Version& version, uint32_t keepVersionCount,
                     const std::set<versionid_t>& reservedVersionSet = {})
        : mFsRootDir(rootDir)
        , mVersion(version)
        , mKeepVersionCount(keepVersionCount)
        , mReservedVersionSet(reservedVersionSet)
    {
    }

    ~VersionCommitter() {}

public:
    void Commit()
    {
        if (mFsRootDir) {
            DumpPartitionPatchMeta(mFsRootDir, mVersion);
            DeployIndexWrapper::DumpDeployMeta(mFsRootDir, mVersion);
            mVersion.Store(mFsRootDir, true);
            file_system::FileSystemMetricsReporter* reporter =
                mFsRootDir->GetFileSystem() != nullptr ? mFsRootDir->GetFileSystem()->GetFileSystemMetricsReporter()
                                                       : nullptr;
            file_system::DirectoryPtr physicalDir =
                file_system::Directory::GetPhysicalDirectory(mFsRootDir->GetOutputPath());
            file_system::DirectoryPtr directory =
                file_system::Directory::ConstructByFenceContext(physicalDir, mFsRootDir->GetFenceContext(), reporter);
            CleanVersions(directory, mKeepVersionCount, mReservedVersionSet);
            return;
        }
    }

public:
    static void CleanVersions(const file_system::DirectoryPtr& rootDir, uint32_t keepVersionCount,
                              const std::set<versionid_t>& reservedVersionSet = {});

    // will clean version and versions small than version, but can not clean all version.
    static void DumpPartitionPatchMeta(const file_system::DirectoryPtr& rootDir, const Version& version);

    static bool CleanVersionAndBefore(const file_system::DirectoryPtr& rootDir, versionid_t versionId,
                                      const std::set<versionid_t>& reservedVersionSet = {});

private:
    static void CleanVersions(IndexSummary& summary, const file_system::DirectoryPtr& rootDir,
                              const fslib::FileList& fileList, uint32_t keepVersionCount,
                              const std::set<versionid_t>& reservedVersionSet);

    static segmentid_t ConstructNeedKeepSegment(const file_system::DirectoryPtr& rootDir,
                                                const fslib::FileList& fileList,
                                                const std::set<versionid_t>& reservedVersionSet,
                                                uint32_t keepVersionCount, std::set<segmentid_t>& needKeepSegment,
                                                std::set<schemaid_t>& needKeepSchemaId);

    static void CleanVersionFiles(const file_system::DirectoryPtr& rootDir, const fslib::FileList& fileList,
                                  const std::set<versionid_t>& reservedVersionSet, uint32_t keepVersionCount);

    static std::vector<segmentid_t> CleanSegmentFiles(const file_system::DirectoryPtr& rootDir,
                                                      segmentid_t maxSegInVersion,
                                                      const std::set<segmentid_t>& needKeepSegment);

    static void CleanPatchIndexSegmentFiles(const file_system::DirectoryPtr& normRootDir, segmentid_t maxSegInVersion,
                                            const std::set<segmentid_t>& needKeepSegment);

    static void CleanUselessSchemaFiles(const file_system::DirectoryPtr& normRootDir,
                                        const std::set<schemaid_t>& needKeepSchemaId);

private:
    file_system::DirectoryPtr mFsRootDir;
    Version mVersion;
    uint32_t mKeepVersionCount;
    std::set<versionid_t> mReservedVersionSet;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VersionCommitter);

////////////////////////////////////////////////////////////////////////////////
}} // namespace indexlib::index_base
