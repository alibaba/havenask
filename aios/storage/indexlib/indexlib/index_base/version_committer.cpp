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
#include "indexlib/index_base/version_committer.h"

#include <beeper/beeper.h>

using namespace std;
namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, VersionCommitter);

void VersionCommitter::CleanVersions(const file_system::DirectoryPtr& rootDir, uint32_t keepVersionCount,
                                     const std::set<versionid_t>& reservedVersionSet)
{
    IE_LOG(DEBUG, "rootDir[%s], keepVersionCount[%u]", rootDir->DebugString().c_str(), keepVersionCount);

    if (keepVersionCount == INVALID_KEEP_VERSION_COUNT) {
        return;
    } else if (keepVersionCount == 0) {
        IE_LOG(WARN, "Keep version count should not be less than 0. "
                     "use 1 by default.");
        keepVersionCount = 1;
    }

    fslib::FileList fileList;
    VersionLoader::ListVersion(rootDir, fileList);
    IndexSummary summary = IndexSummary::Load(rootDir, fileList);
    if (fileList.size() <= keepVersionCount) {
        summary.Commit(rootDir);
        return;
    }
    CleanVersions(summary, rootDir, fileList, keepVersionCount, reservedVersionSet);
    summary.Commit(rootDir);
}

void VersionCommitter::CleanVersions(IndexSummary& indexSummary, const file_system::DirectoryPtr& rootDir,
                                     const fslib::FileList& fileList, uint32_t keepVersionCount,
                                     const std::set<versionid_t>& reservedVersionSet)
{
    std::set<segmentid_t> needKeepSegment;
    std::set<schemavid_t> needKeepSchemaId;
    segmentid_t maxSegInVersion = ConstructNeedKeepSegment(rootDir, fileList, reservedVersionSet, keepVersionCount,
                                                           needKeepSegment, needKeepSchemaId);

    CleanVersionFiles(rootDir, fileList, reservedVersionSet, keepVersionCount);

    std::vector<segmentid_t> removeSegIds = CleanSegmentFiles(rootDir, maxSegInVersion, needKeepSegment);
    for (auto segId : removeSegIds) {
        indexSummary.RemoveSegment(segId);
    }
    CleanPatchIndexSegmentFiles(rootDir, maxSegInVersion, needKeepSegment);
    CleanUselessSchemaFiles(rootDir, needKeepSchemaId);
}

segmentid_t VersionCommitter::ConstructNeedKeepSegment(const file_system::DirectoryPtr& rootDir,
                                                       const fslib::FileList& fileList,
                                                       const std::set<versionid_t>& reservedVersionSet,
                                                       uint32_t keepVersionCount,
                                                       std::set<segmentid_t>& needKeepSegment,
                                                       std::set<schemavid_t>& needKeepSchemaId)
{
    // Determine which segments need to keep
    for (size_t i = 0; i < keepVersionCount; ++i) {
        Version version;
        versionid_t versionId = VersionLoader::GetVersionId(fileList[fileList.size() - i - 1]);
        VersionLoader::GetVersion(rootDir, version, versionId);
        for (size_t j = 0; j < version.GetSegmentCount(); ++j) {
            needKeepSegment.insert(version[j]);
        }
        needKeepSchemaId.insert(version.GetSchemaVersionId());
    }

    for (const auto& versionId : reservedVersionSet) {
        Version version;
        VersionLoader::GetVersion(rootDir, version, versionId);
        for (size_t j = 0; j < version.GetSegmentCount(); ++j) {
            needKeepSegment.insert(version[j]);
        }
        needKeepSchemaId.insert(version.GetSchemaVersionId());
    }

    if (needKeepSegment.size() == 0) {
        IE_LOG(WARN,
               "No segments in kept versions, "
               "keep count: [%u]",
               keepVersionCount);
    }

    // get maxSegInVersion
    segmentid_t maxSegInVersion = INVALID_SEGMENTID;
    if (!needKeepSegment.empty()) {
        maxSegInVersion = *(needKeepSegment.rbegin());
    } else {
        size_t versionCountToClean = fileList.size() - keepVersionCount;
        for (int32_t i = (int32_t)(versionCountToClean - 1); i >= 0; i--) {
            versionid_t versionId = VersionLoader::GetVersionId(fileList[i]);
            Version version;
            VersionLoader::GetVersion(rootDir, version, versionId);
            segmentid_t lastSegId = version.GetLastSegment();
            if (lastSegId != INVALID_SEGMENTID) {
                maxSegInVersion = lastSegId;
                break;
            }
        }
    }
    return maxSegInVersion;
}

void VersionCommitter::CleanVersionFiles(const file_system::DirectoryPtr& rootDir, const fslib::FileList& fileList,
                                         const std::set<versionid_t>& reservedVersionSet, uint32_t keepVersionCount)
{
    size_t versionCountToClean = fileList.size() - keepVersionCount;
    for (size_t i = 0; i < versionCountToClean; ++i) {
        versionid_t versionId = VersionLoader::GetVersionId(fileList[i]);
        if (reservedVersionSet.find(versionId) != reservedVersionSet.end()) {
            continue;
        }
        IE_LOG(INFO, "Removing [%s] file.", fileList[i].c_str());
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME, "Remove version [%d]", versionId);

        file_system::RemoveOption removeOption = file_system::RemoveOption::MayNonExist();
        rootDir->RemoveFile(fileList[i]);
        rootDir->RemoveFile(DeployIndexWrapper::GetDeployMetaFileName(versionId), removeOption);
        rootDir->RemoveFile(file_system::EntryTable::GetEntryTableFileName(versionId), removeOption);
        rootDir->RemoveFile(index_base::PartitionPatchMeta::GetPatchMetaFileName(versionId), removeOption);
        rootDir->RemoveFile(
            util::PathUtil::JoinPath(INDEX_SUMMARY_INFO_DIR_NAME, index_base::IndexSummary::GetFileName(versionId)),
            removeOption);
    }
}

void VersionCommitter::CleanPatchIndexSegmentFiles(const file_system::DirectoryPtr& rootDir,
                                                   segmentid_t maxSegInVersion,
                                                   const std::set<segmentid_t>& needKeepSegment)
{
    fslib::FileList patchIndexDirList;
    index_base::PartitionPatchIndexAccessor::ListPatchRootDirs(rootDir, patchIndexDirList);
    for (size_t i = 0; i < patchIndexDirList.size(); i++) {
        const file_system::DirectoryPtr& patchPath = rootDir->GetDirectory(patchIndexDirList[i], true);
        vector<segmentid_t> removeSegments = CleanSegmentFiles(patchPath, maxSegInVersion, needKeepSegment);

        fslib::FileList tmpList;
        patchPath->ListDir("", tmpList, false);
        if (tmpList.empty() && removeSegments.size() != 0) {
            IE_LOG(INFO, "Removing empty patch index path [%s]", patchPath->DebugString().c_str());
            rootDir->RemoveDirectory(patchIndexDirList[i]);
        }
    }
}

std::vector<segmentid_t> VersionCommitter::CleanSegmentFiles(const file_system::DirectoryPtr& rootDir,
                                                             segmentid_t maxSegInVersion,
                                                             const std::set<segmentid_t>& needKeepSegment)
{
    std::vector<segmentid_t> removeSegIds;
    fslib::FileList segList;
    VersionLoader::ListSegments(rootDir, segList);

    // Clean segment files
    for (fslib::FileList::const_iterator it = segList.begin(); it != segList.end(); ++it) {
        std::string segFileName = *it;
        segmentid_t segId = Version::GetSegmentIdByDirName(segFileName);
        if (segId < maxSegInVersion && needKeepSegment.find(segId) == needKeepSegment.end()) {
            removeSegIds.push_back(segId);
            try {
                rootDir->RemoveDirectory(segFileName);
            } catch (const util::NonExistException& e) {
                IE_LOG(WARN, "Segment: [%s] already removed.", segFileName.c_str());
                continue;
            }
            IE_LOG(INFO, "Segment: [%s] removed", segFileName.c_str());
            BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME, "Segment: [%s] removed",
                                              segFileName.c_str());
        }
    }
    return removeSegIds;
}

bool VersionCommitter::CleanVersionAndBefore(const file_system::DirectoryPtr& rootDir, versionid_t versionId,
                                             const std::set<versionid_t>& reservedVersionSet)
{
    IE_LOG(INFO, "clean version[%d] and before it in partitionDir[%s]", versionId, rootDir->DebugString().c_str());

    fslib::FileList fileList;
    VersionLoader::ListVersion(rootDir, fileList);
    IndexSummary summary = IndexSummary::Load(rootDir, fileList);
    uint32_t keepVersionCount = 0;
    for (fslib::FileList::const_iterator it = fileList.begin(); it != fileList.end(); ++it) {
        versionid_t curVersionId = VersionLoader::GetVersionId(*it);
        if (versionId < curVersionId) {
            ++keepVersionCount;
        }
    }
    if (keepVersionCount == 0) {
        summary.Commit(rootDir);
        IE_LOG(ERROR, "can not clean all version, version[%d] in partitionDir[%s]", versionId,
               rootDir->DebugString().c_str());
        return false;
    }
    CleanVersions(summary, rootDir, fileList, keepVersionCount, reservedVersionSet);
    summary.Commit(rootDir);
    return true;
}

void VersionCommitter::CleanUselessSchemaFiles(const file_system::DirectoryPtr& rootDir,
                                               const std::set<schemavid_t>& needKeepSchemaId)
{
    if (needKeepSchemaId.empty()) {
        IE_LOG(WARN, "no valid schema id to keep!");
        return;
    }

    schemavid_t maxSchemaId = *(needKeepSchemaId.rbegin());
    fslib::FileList schemaList;
    SchemaAdapter::ListSchemaFile(rootDir, schemaList);
    for (const auto& schemaFile : schemaList) {
        schemavid_t schemaId = DEFAULT_SCHEMAID;
        Version::ExtractSchemaIdBySchemaFile(schemaFile, schemaId);
        if (schemaId < maxSchemaId && needKeepSchemaId.find(schemaId) == needKeepSchemaId.end()) {
            if (!rootDir->IsExist(schemaFile)) {
                IE_LOG(WARN, "schema file: [%s] already removed.", schemaFile.c_str());
                continue;
            }
            rootDir->RemoveFile(schemaFile);
            IE_LOG(INFO, "schema file: [%s] removed", schemaFile.c_str());
            BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME, "schema file [%s] removed",
                                              schemaFile.c_str());
        }
    }
}

void VersionCommitter::DumpPartitionPatchMeta(const file_system::DirectoryPtr& rootDir, const Version& version)
{
    if (version.GetSchemaVersionId() == DEFAULT_SCHEMAID) {
        return;
    }
    if (!rootDir->IsExist(version.GetSchemaFileName())) {
        return;
    }

    config::IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(rootDir, version.GetSchemaFileName());
    index_base::PartitionPatchMetaPtr patchMeta =
        index_base::PartitionPatchMetaCreator::Create(rootDir, schema, version);
    if (patchMeta) {
        patchMeta->Store(rootDir, version.GetVersionId());
    }
}
}} // namespace indexlib::index_base
