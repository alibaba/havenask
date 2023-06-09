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
#include "indexlib/partition/local_index_cleaner.h"

#include "autil/StringUtil.h"
#include "fslib/util/PathUtil.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/index_meta/index_path_util.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
using namespace indexlib::index_base;
using namespace indexlib::file_system;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, LocalIndexCleaner);

LocalIndexCleaner::LocalIndexCleaner(const std::string& localIndexRoot, const std::string& loadVersionIndexRoot,
                                     uint32_t keepVersionCount, const ReaderContainerPtr& readerContainer)
    : mLocalIndexRoot(localIndexRoot)
    , mLoadVersionIndexRoot(loadVersionIndexRoot)
    , mKeepVersionCount(keepVersionCount)
    , mReaderContainer(readerContainer)
{
}

bool LocalIndexCleaner::Clean(const vector<versionid_t>& keepVersionIds)
{
    try {
        DoClean(keepVersionIds, true);
    } catch (const util::ExceptionBase& e) {
        IE_LOG(WARN, "error occurred when clean local index [%s], [%s]", mLocalIndexRoot.c_str(),
               e.GetMessage().c_str());
        return false;
    } catch (...) {
        IE_LOG(WARN, "error occurred when clean local index [%s]", mLocalIndexRoot.c_str());
        return false;
    }
    return true;
}

bool LocalIndexCleaner::CleanUnreferencedIndexFiles(const index_base::Version& targetVersion,
                                                    const set<string>& toKeepFiles)
{
    bool ret = false;
    if (targetVersion.GetVersionId() == INVALID_VERSION || targetVersion.GetSegmentCount() == 0) {
        IE_LOG(INFO,
               "invalid version id or target version has no segment, doesn't need clean local disk, versionId[%d]",
               static_cast<int>(targetVersion.GetVersionId()));
        return true;
    }
    try {
        std::set<std::string> whiteList;
        if (mReaderContainer) {
            if (!mReaderContainer->GetNeedKeepDeployFiles(&whiteList)) {
                IE_LOG(ERROR, "reader container get need keep deploy files failed, target versionId[%d]",
                       static_cast<int>(targetVersion.GetVersionId()));
                return false;
            }
            whiteList.insert(toKeepFiles.begin(), toKeepFiles.end());
            return CleanUnreferencedSegments(targetVersion, whiteList) &&
                   CleanUnreferencedLocalFiles(targetVersion, whiteList);
        } else {
            return CleanUnreferencedSegments(targetVersion, toKeepFiles) &&
                   CleanUnreferencedLocalFiles(targetVersion, toKeepFiles);
        }
    } catch (const util::ExceptionBase& e) {
        IE_LOG(WARN, "error occurred when clean local index [%s], [%s]", mLocalIndexRoot.c_str(),
               e.GetMessage().c_str());
        return false;
    } catch (...) {
        IE_LOG(WARN, "error occurred when clean local index [%s]", mLocalIndexRoot.c_str());
        return false;
    }
    return ret;
}

bool LocalIndexCleaner::CleanUnreferencedSegments(const index_base::Version& targetVersion,
                                                  const std::set<string>& toKeepFiles)
{
    // clean segments in best effort
    // return false if any error occurs
    size_t segCount = targetVersion.GetSegmentCount();
    bool hasError = false;

    for (size_t i = 0; i < segCount; i++) {
        auto segDirName = targetVersion.GetSegmentDirName(targetVersion[i]);
        auto segPath = FslibWrapper::JoinPath(mLocalIndexRoot, segDirName);

        if (!PrefixMatchAny(segDirName, toKeepFiles)) {
            auto ec = FslibWrapper::DeleteDir(segPath, DeleteOption::NoFence(true)).Code();
            if (ec != FSEC_OK) {
                hasError = true;
                IE_LOG(WARN, "[%s] removed failed, ec = [%d]", segPath.c_str(), static_cast<int>(ec));
                continue;
            }
            IE_LOG(INFO, "[%s] removed", segPath.c_str());
        }
    }
    return hasError == false;
}

bool LocalIndexCleaner::PrefixMatchAny(const std::string& prefix, const std::set<string>& candidates)
{
    for (const auto& item : candidates) {
        if (item.size() < prefix.size()) {
            continue;
        }
        if (item.find(prefix) == 0) {
            return true;
        }
    }
    return false;
}

bool LocalIndexCleaner::CleanUnreferencedLocalFiles(const index_base::Version& targetVersion,
                                                    const std::set<string>& toKeepFiles)
{
    std::set<string> localFiles;
    if (!ListFilesInSegmentDirs(targetVersion, &localFiles)) {
        IE_LOG(WARN, "list files in target version's segments failed, target versionId[%d]",
               static_cast<int>(targetVersion.GetVersionId()));
        return false;
    }
    std::set<string> toRemoveFiles;
    std::set_difference(localFiles.begin(), localFiles.end(), toKeepFiles.begin(), toKeepFiles.end(),
                        inserter(toRemoveFiles, toRemoveFiles.begin()));

    bool hasError = false;

    IE_LOG(INFO, "begin clean unreference files is Root[%s]", mLocalIndexRoot.c_str());
    for (const auto& file : toRemoveFiles) {
        auto filePath = FslibWrapper::JoinPath(mLocalIndexRoot, file);
        auto ec = FslibWrapper::DeleteFile(filePath, DeleteOption::NoFence(false)).Code();
        if (ec != FSEC_OK) {
            hasError = true;
            IE_LOG(WARN, "[%s] removed failed, ec = [%d]", file.c_str(), static_cast<int>(ec));
            continue;
        }
        IE_LOG(INFO, "[%s] removed", file.c_str());
    }
    IE_LOG(INFO, "end clean unreference files is Root[%s]", mLocalIndexRoot.c_str());
    return hasError == false;
}

bool LocalIndexCleaner::ListFilesInSegmentDirs(const index_base::Version& targetVersion, std::set<string>* files)
{
    size_t segCount = targetVersion.GetSegmentCount();
    fslib::FileList segmentFileList;

    for (size_t i = 0; i < segCount; i++) {
        segmentFileList.clear();
        auto segDirName = targetVersion.GetSegmentDirName(targetVersion[i]);
        auto segDirPath = FslibWrapper::JoinPath(mLocalIndexRoot, segDirName);
        auto ec = FslibWrapper::ListDirRecursive(segDirPath, segmentFileList).Code();
        if (ec == file_system::ErrorCode::FSEC_NOENT) {
            continue;
        }
        if (ec != indexlib::file_system::ErrorCode::FSEC_OK) {
            IE_LOG(ERROR, "list dir[%s] failed with ec = [%d]", segDirPath.c_str(), static_cast<int>(ec));
            return false;
        }
        for (const auto& path : segmentFileList) {
            if (!path.empty() && path.back() != '/') {
                files->insert(FslibWrapper::JoinPath(segDirName, path));
            }
        }
    }
    return true;
}

void LocalIndexCleaner::DoClean(const vector<versionid_t>& keepVersionIds, bool cleanAfterMaxKeepVersionFiles)
{
    IE_LOG(INFO, "start clean local index. keep versions [%s], cleanAfterMaxKeepVersionFiles [%d]",
           StringUtil::toString(keepVersionIds, ",").c_str(), cleanAfterMaxKeepVersionFiles);
    ScopedLock lock(mLock);
    set<Version> keepVersions;
    if (mReaderContainer) {
        vector<Version> usingVersions;
        mReaderContainer->GetIncVersions(usingVersions);
        keepVersions.insert(usingVersions.begin(), usingVersions.end());
        mVersionCache.insert(usingVersions.begin(), usingVersions.end());
    }
    for (versionid_t versionId : keepVersionIds) {
        keepVersions.insert(GetVersion(versionId));
    }

    FileList fileList;
    auto ec = FslibWrapper::ListDir(mLocalIndexRoot, fileList).Code();
    THROW_IF_FS_ERROR(ec, "list dir [%s] failed", mLocalIndexRoot.c_str());
    if (mKeepVersionCount != INVALID_KEEP_VERSION_COUNT && keepVersions.size() < mKeepVersionCount) {
        versionid_t tmpVersionId;
        versionid_t currentVersionId =
            mReaderContainer ? mReaderContainer->GetLatestReaderVersion()
                             : (keepVersions.empty() ? INVALID_VERSION : keepVersions.begin()->GetVersionId());
        set<versionid_t> localVersionIds;
        for (const string& name : fileList) {
            if (IndexPathUtil::GetVersionId(name, tmpVersionId) &&
                (currentVersionId == INVALID_VERSION || tmpVersionId < currentVersionId)) {
                localVersionIds.insert(tmpVersionId);
            }
        }
        for (auto it = localVersionIds.rbegin();
             it != localVersionIds.rend() && keepVersions.size() < mKeepVersionCount; ++it) {
            keepVersions.insert(GetVersion(*it));
        }
    }
    CleanFiles(fileList, keepVersions, cleanAfterMaxKeepVersionFiles);
    mVersionCache.swap(keepVersions);
    IE_LOG(INFO, "finish clean local index");
}

const Version& LocalIndexCleaner::GetVersion(versionid_t versionId)
{
    auto it = mVersionCache.find(Version(versionId));
    if (it != mVersionCache.end()) {
        return *it;
    }
    Version version;
    try {
        // try load from local first because it's cheap
        VersionLoader::GetVersionS(mLocalIndexRoot, version, versionId);
    } catch (const util::NonExistException& e) {
        if (!mLoadVersionIndexRoot.empty()) {
            VersionLoader::GetVersionS(mLoadVersionIndexRoot, version, versionId);
        } else {
            throw;
        }
    } catch (...) {
        throw;
    }
    return *(mVersionCache.insert(version).first);
}

void LocalIndexCleaner::CleanFiles(const FileList& fileList, const set<Version>& keepVersions,
                                   bool cleanAfterMaxKeepVersionFiles)
{
    set<segmentid_t> keepSegmentIds;
    set<schemavid_t> keepSchemaIds;
    for (const Version& version : keepVersions) {
        keepSegmentIds.insert(version.GetSegmentVector().begin(), version.GetSegmentVector().end());
        keepSchemaIds.insert(version.GetSchemaVersionId());
    }
    versionid_t maxCanCleanVersionId = (cleanAfterMaxKeepVersionFiles || keepVersions.empty())
                                           ? std::numeric_limits<versionid_t>::max()
                                           : keepVersions.rbegin()->GetVersionId();
    segmentid_t maxCanCleanSegmentId = (cleanAfterMaxKeepVersionFiles || keepSegmentIds.empty())
                                           ? std::numeric_limits<segmentid_t>::max()
                                           : *keepSegmentIds.rbegin();
    schemavid_t maxCanCleanSchemaid = (cleanAfterMaxKeepVersionFiles || keepSchemaIds.empty())
                                          ? std::numeric_limits<schemavid_t>::max()
                                          : *keepSchemaIds.rbegin();

    segmentid_t segmentId;
    versionid_t versionId;
    schemavid_t schemaId;

    // only for online now, thus no fence
    for (const string& name : fileList) {
        string path = util::PathUtil::JoinPath(mLocalIndexRoot, name);
        if (IndexPathUtil::GetSegmentId(name, segmentId)) {
            if (segmentId <= maxCanCleanSegmentId && keepSegmentIds.count(segmentId) == 0) {
                auto ec = FslibWrapper::DeleteDir(path, DeleteOption::NoFence(false)).Code();
                THROW_IF_FS_ERROR(ec, "delete [%s] failed", path.c_str());
                IE_LOG(INFO, "[%s] removed", path.c_str());
            }
        } else if (IndexPathUtil::GetVersionId(name, versionId) || IndexPathUtil::GetDeployMetaId(name, versionId) ||
                   IndexPathUtil::GetPatchMetaId(name, versionId)) {
            if (versionId <= maxCanCleanVersionId && keepVersions.count(Version(versionId)) == 0) {
                auto ec = FslibWrapper::DeleteDir(path, DeleteOption::NoFence(false)).Code();
                THROW_IF_FS_ERROR(ec, "delete [%s] failed", path.c_str());
                IE_LOG(INFO, "[%s] removed", path.c_str());
            }
        } else if (IndexPathUtil::GetSchemaId(name, schemaId)) {
            if (schemaId <= maxCanCleanSchemaid && keepSchemaIds.count(schemaId) == 0) {
                auto ec = FslibWrapper::DeleteDir(path, DeleteOption::NoFence(false)).Code();
                THROW_IF_FS_ERROR(ec, "delete [%s] failed", path.c_str());
                IE_LOG(INFO, "[%s] removed", path.c_str());
            }
        } else if (IndexPathUtil::GetPatchIndexId(name, schemaId)) {
            CleanPatchIndexSegmentFiles(name, keepSegmentIds, maxCanCleanSegmentId, schemaId);
        }
    }
}

void LocalIndexCleaner::CleanPatchIndexSegmentFiles(const string& patchDirName, const set<segmentid_t>& keepSegmentIds,
                                                    segmentid_t maxCanCleanSegmentId, schemavid_t patchSchemaId)
{
    if (unlikely(patchSchemaId == DEFAULT_SCHEMAID)) // Why? for legency code, may useless
    {
        assert(false);
        return;
    }
    FileList segmentList;
    string patchDirPath = util::PathUtil::JoinPath(mLocalIndexRoot, patchDirName);
    auto ec = FslibWrapper::ListDir(patchDirPath, segmentList).Code();
    THROW_IF_FS_ERROR(ec, "list dir [%s] failed", patchDirPath.c_str());

    bool canRemovePatchDir = true;
    for (const string& segmentName : segmentList) {
        segmentid_t segmentId;
        bool tmpIsSuccess = IndexPathUtil::GetSegmentId(segmentName, segmentId);
        assert(tmpIsSuccess);
        (void)tmpIsSuccess;
        if (segmentId <= maxCanCleanSegmentId && keepSegmentIds.count(segmentId) == 0) {
            string segmentDirPath = util::PathUtil::JoinPath(patchDirPath, segmentName);
            auto ec = FslibWrapper::DeleteDir(segmentDirPath, DeleteOption::NoFence(false)).Code();
            THROW_IF_FS_ERROR(ec, "delete [%s] failed", segmentDirPath.c_str());
            IE_LOG(INFO, "Patch index segment [%s] removed", segmentDirPath.c_str());
        } else {
            canRemovePatchDir = false;
        }
    }
    if (canRemovePatchDir) {
        auto ec = FslibWrapper::DeleteDir(patchDirPath, DeleteOption::NoFence(false)).Code();
        THROW_IF_FS_ERROR(ec, "delete [%s] failed", patchDirPath.c_str());
        IE_LOG(INFO, "Empty patch index path [%s] removed", patchDirName.c_str());
    }
}
}} // namespace indexlib::partition
