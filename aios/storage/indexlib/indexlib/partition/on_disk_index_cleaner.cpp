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
#include "indexlib/partition/on_disk_index_cleaner.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/PhysicalDirectory.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/reader_container.h"

using namespace std;
using namespace fslib;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OnDiskIndexCleaner);

OnDiskIndexCleaner::OnDiskIndexCleaner(uint32_t keepVersionCount, const ReaderContainerPtr& readerContainer,
                                       const DirectoryPtr& directory)
    : mKeepVersionCount(keepVersionCount)
    , mReaderContainer(readerContainer)
    , mDirectory(directory)
{
}

OnDiskIndexCleaner::~OnDiskIndexCleaner() {}

void OnDiskIndexCleaner::Execute()
{
    IE_LOG(DEBUG, "rootDir:%s, keepVersionCount:%u", mDirectory->DebugString().c_str(), mKeepVersionCount);

    if (mKeepVersionCount == INVALID_KEEP_VERSION_COUNT) {
        return;
    }
    if (mKeepVersionCount == 0) {
        IE_LOG(WARN, "Keep version count should not be less than 0. "
                     "use 1 by default.");
        mKeepVersionCount = 1;
    }

    // for online, outer framwork may dp versions which indexlib not know
    DoExecute(mDirectory);
    FileSystemMetricsReporter* reporter =
        mDirectory->GetFileSystem() != nullptr ? mDirectory->GetFileSystem()->GetFileSystemMetricsReporter() : nullptr;
    DirectoryPtr physicalDir = Directory::GetPhysicalDirectory(mDirectory->GetOutputPath());
    DirectoryPtr directory = Directory::ConstructByFenceContext(physicalDir, mDirectory->GetFenceContext(), reporter);
    DoExecute(directory);
}

void OnDiskIndexCleaner::DoExecute(const file_system::DirectoryPtr& rootDirectoy)
{
    fslib::FileList fileList;
    VersionLoader::ListVersion(rootDirectoy, fileList);
    if (fileList.size() <= mKeepVersionCount) {
        return;
    }

    set<segmentid_t> needKeepSegments;
    set<schemavid_t> needKeepSchemaId;
    size_t firstKeepVersionIdx = 0;
    ConstructNeedKeepVersionIdxAndSegments(rootDirectoy, fileList, firstKeepVersionIdx, needKeepSegments,
                                           needKeepSchemaId);

    segmentid_t maxSegId = CalculateMaxSegmentIdInAllVersions(rootDirectoy, fileList);

    CleanVersionFiles(rootDirectoy, firstKeepVersionIdx, fileList);
    CleanSegmentFiles(rootDirectoy, maxSegId, needKeepSegments);
    CleanPatchIndexSegmentFiles(rootDirectoy, maxSegId, needKeepSegments);
    CleanUselessSchemaFiles(rootDirectoy, needKeepSchemaId);
}

void OnDiskIndexCleaner::ConstructNeedKeepVersionIdxAndSegments(const file_system::DirectoryPtr& dir,
                                                                const fslib::FileList& fileList,
                                                                size_t& firstKeepVersionIdx,
                                                                set<segmentid_t>& needKeepSegments,
                                                                set<schemavid_t>& needKeepSchemaId)
{
    firstKeepVersionIdx = 0;
    while (firstKeepVersionIdx < fileList.size() - mKeepVersionCount) {
        Version version;
        version.Load(dir, fileList[firstKeepVersionIdx]);
        if (mReaderContainer && mReaderContainer->HasReader(version.GetVersionId())) {
            break;
        }
        firstKeepVersionIdx++;
    }
    for (size_t i = firstKeepVersionIdx; i < fileList.size(); ++i) {
        Version version;
        version.Load(dir, fileList[i]);
        for (size_t j = 0; j < version.GetSegmentCount(); ++j) {
            needKeepSegments.insert(version[j]);
        }
        needKeepSchemaId.insert(version.GetSchemaVersionId());
    }
}

segmentid_t OnDiskIndexCleaner::CalculateMaxSegmentIdInAllVersions(const file_system::DirectoryPtr& dir,
                                                                   const fslib::FileList& fileList)
{
    segmentid_t maxSegIdInAllVersions = INVALID_SEGMENTID;
    // when version rollbacks(e.g. v0:[0,1,2], v1:[0,1], v1 is a roll
    // backed version), we must scan all version
    for (size_t i = 0; i < fileList.size(); ++i) {
        Version version;
        version.Load(dir, fileList[i]);
        segmentid_t lastSegId = version.GetLastSegment();
        if (lastSegId != INVALID_SEGMENTID && lastSegId > maxSegIdInAllVersions) {
            maxSegIdInAllVersions = lastSegId;
        }
    }
    return maxSegIdInAllVersions;
}

void OnDiskIndexCleaner::CleanVersionFiles(const file_system::DirectoryPtr& dir, size_t firstKeepVersionIdx,
                                           const fslib::FileList& fileList)
{
    for (size_t i = 0; i < firstKeepVersionIdx; ++i) {
        file_system::RemoveOption removeOption = file_system::RemoveOption::MayNonExist();
        removeOption.relaxedConsistency = true;
        versionid_t versionId = VersionLoader::GetVersionId(fileList[i]);
        string deployMetaFile = DeployIndexWrapper::GetDeployMetaFileName(versionId);
        dir->RemoveFile(deployMetaFile, removeOption);

        string patchMetaFile = PartitionPatchMeta::GetPatchMetaFileName(versionId);
        dir->RemoveFile(patchMetaFile, removeOption);

        string entryTableFile = EntryTable::GetEntryTableFileName(versionId);
        dir->RemoveFile(entryTableFile, removeOption);
        file_system::RemoveOption removeOptionRelaxed;
        removeOptionRelaxed.relaxedConsistency = true;
        dir->RemoveFile(fileList[i], removeOptionRelaxed);
        IE_LOG(INFO, "Version: [%s/%s] removed", dir->DebugString().c_str(), fileList[i].c_str());
    }
}

void OnDiskIndexCleaner::CleanSegmentFiles(const file_system::DirectoryPtr& dir, segmentid_t maxSegIdInAllVersions,
                                           const set<segmentid_t>& needKeepSegments)
{
    fslib::FileList segList;
    VersionLoader::ListSegments(dir, segList);
    for (fslib::FileList::const_iterator it = segList.begin(); it != segList.end(); ++it) {
        string segmentDirName = *it;
        segmentid_t segId = Version::GetSegmentIdByDirName(segmentDirName);
        if (segId <= maxSegIdInAllVersions && needKeepSegments.find(segId) == needKeepSegments.end()) {
            dir->RemoveDirectory(segmentDirName);
            IE_LOG(INFO, "Segment: [%s/%s] removed", dir->DebugString().c_str(), segmentDirName.c_str());
        }
    }
}

void OnDiskIndexCleaner::CleanPatchIndexSegmentFiles(const file_system::DirectoryPtr& dir, segmentid_t maxSegInVersion,
                                                     const set<segmentid_t>& needKeepSegment)
{
    FileList patchIndexDirList;
    PartitionPatchIndexAccessor::ListPatchRootDirs(dir, patchIndexDirList);
    for (size_t i = 0; i < patchIndexDirList.size(); i++) {
        DirectoryPtr patchDir = dir->GetDirectory(patchIndexDirList[i], false);
        if (!patchDir) {
            continue;
        }

        CleanSegmentFiles(patchDir, maxSegInVersion, needKeepSegment);
        FileList tmpList;
        dir->ListDir(patchIndexDirList[i], tmpList, false);
        if (tmpList.empty()) {
            IE_LOG(INFO, "Removing empty patch index path [%s]", patchIndexDirList[i].c_str());
            dir->RemoveDirectory(patchIndexDirList[i]);
        }
    }
}

void OnDiskIndexCleaner::CleanUselessSchemaFiles(const file_system::DirectoryPtr& dir,
                                                 const set<schemavid_t>& needKeepSchemaId)
{
    if (needKeepSchemaId.empty()) {
        IE_LOG(WARN, "no valid schema id to keep!");
        return;
    }

    schemavid_t maxSchemaId = *(needKeepSchemaId.rbegin());
    FileList tmpList;
    dir->ListDir("", tmpList, false);
    for (const auto& file : tmpList) {
        schemavid_t schemaId = DEFAULT_SCHEMAID;
        if (!Version::ExtractSchemaIdBySchemaFile(file, schemaId)) {
            continue;
        }
        if (schemaId < maxSchemaId && needKeepSchemaId.find(schemaId) == needKeepSchemaId.end()) {
            IE_LOG(INFO, "Removing schema file: [%s]", file.c_str());
            file_system::RemoveOption removeOption = file_system::RemoveOption::MayNonExist();
            dir->RemoveFile(file, removeOption);
        }
    }
}
}} // namespace indexlib::partition
