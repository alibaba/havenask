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
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "indexlib/base/Types.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/patch/patch_file_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, ParallelBuildInfo);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, StateCounter);
DECLARE_REFERENCE_CLASS(file_system, IFileSystem);

namespace indexlib { namespace merger {

class ParallelPartitionDataMerger
{
public:
    static std::vector<std::string> GetParallelInstancePaths(const std::string& rootPath,
                                                             const index_base::ParallelBuildInfo& info);

public:
    ParallelPartitionDataMerger(const std::string& mergeRoot, const std::string& epochId,
                                const config::IndexPartitionSchemaPtr& schema = nullptr,
                                const config::IndexPartitionOptions& options = config::IndexPartitionOptions());
    ParallelPartitionDataMerger(const file_system::DirectoryPtr& mergeDirectory, const std::string& epochId,
                                const config::IndexPartitionSchemaPtr& schema,
                                const config::IndexPartitionOptions& options = config::IndexPartitionOptions());
    ~ParallelPartitionDataMerger();

    index_base::Version MergeSegmentData(const std::vector<std::string>& srcDirs);
    void RemoveParallBuildDirectory();

private:
    static std::vector<index_base::Version>
    GetVersionList(const std::vector<std::shared_ptr<file_system::Directory>>& directorys);
    static versionid_t
    GetBaseVersionIdFromParallelBuildInfo(const std::vector<std::shared_ptr<file_system::Directory>>& directorys);
    typedef std::pair<index_base::Version, size_t> VersionPair;

    static bool NoNeedMerge(const std::vector<VersionPair>& versionInfos, const index_base::Version& lastVersion,
                            versionid_t baseVersionId);

    void MoveParallelInstanceSegments(const index_base::Version& version, const index_base::Version& lastVersion,
                                      const file_system::DirectoryPtr& instDir);

    void MoveParallelInstanceSchema(const index_base::Version& version, const file_system::DirectoryPtr& instDir);

    std::vector<VersionPair> GetValidVersions(const std::vector<index_base::Version>& versions,
                                              const index_base::Version& oldVersion);
    void CreateMergeSegmentForParallelInstance(const std::vector<VersionPair>& versionInfos,
                                               segmentid_t lastParentSegId, index_base::Version& newVersion);

    index_base::Version MakeNewVersion(const index_base::Version& lastVersion,
                                       const std::vector<VersionPair>& versionInfos, bool& hasNewSegment);

    void CollectSegmentDeletionFileInfo(const index_base::Version& version, index_base::DeletePatchInfos& segments,
                                        bool isSubPartition);

    void MergeDeletionMapFile(const std::vector<VersionPair>& versionInfos, const file_system::DirectoryPtr& directory,
                              segmentid_t lastParentSegId);
    void DoMergeDeletionMapFile(const std::vector<VersionPair>& versionInfos,
                                const file_system::DirectoryPtr& directory, segmentid_t lastParentSegId,
                                bool isSubPartition);
    void MergeSegmentDeletionMapFile(segmentid_t segmentId, const file_system::DirectoryPtr& directory,
                                     const std::vector<index_base::PatchFileInfo>& files);
    void MergeCounterFile(const index_base::Version& newVersion, const std::vector<VersionPair>& versionInfos,
                          const file_system::DirectoryPtr& directory, segmentid_t lastParentSegId);
    void DoMergeCounterFile(const util::CounterMapPtr& counterMap, const std::vector<VersionPair>& versionInfos);
    void UpdateStateCounter(const util::CounterMapPtr& counterMap, const index_base::PartitionDataPtr& partData,
                            bool isSubPartition, bool needDeletionMap);
    util::StateCounterPtr GetStateCounter(const util::CounterMapPtr& counterMap, const std::string& counterName,
                                          bool isSubPartition);
    util::CounterMapPtr LoadCounterMap(const std::string& segmentPath);
    index_base::SegmentInfo CreateMergedSegmentInfo(const std::vector<VersionPair>& versionInfos);

    index_base::PartitionDataPtr CreateInstancePartitionData(const index_base::Version& version, bool isSubPartition);

    std::string GetParentPath(const std::vector<std::string>& srcPaths);
    bool IsEmptyVersions(const std::vector<index_base::Version>& versions, const index_base::Version& lastVersion);

    // todo: remove
    versionid_t GetBaseVersionFromLagecyParallelBuild(const std::vector<index_base::Version>& versions);

private:
    file_system::DirectoryPtr mRoot;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    file_system::FileSystemOptions mFsOptions;
    // backup dirPaths temply, will be triggered in EndBuildTask::handleTarget
    std::vector<std::string> mToDelParallelDirPaths;
    std::string mEpochId;

private:
    friend class ParallelPartitionDataMergerTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ParallelPartitionDataMerger);
}} // namespace indexlib::merger
