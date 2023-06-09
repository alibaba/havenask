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
#include "indexlib/framework/cleaner/OnDiskIndexCleaner.h"

#include <map>
#include <string>

#include "indexlib/base/Constant.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/FileInfo.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/Fence.h"
#include "indexlib/framework/VersionDeployDescription.h"
#include "indexlib/framework/VersionLoader.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, OnDiskIndexCleaner);
#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

static std::set<versionid_t> VersionVecToVersionSet(const std::vector<versionid_t>& versions)
{
    std::set<versionid_t> versionIds;
    for (const auto& versionId : versions) {
        if (versionId >= 0) {
            versionIds.insert(versionId);
        }
    }
    return versionIds;
}

static std::set<versionid_t> VersionVecToVersionSet(const std::vector<Version>& versions)
{
    std::set<versionid_t> versionIds;
    for (const auto& version : versions) {
        auto versionId = version.GetVersionId();
        if (versionId >= 0) {
            versionIds.insert(versionId);
        }
    }
    return versionIds;
}
Status OnDiskIndexCleaner::Clean(const std::vector<versionid_t>& reservedVersions)
{
    autil::ScopedTime2 timer;
    TABLET_LOG(DEBUG, "begin clean local index");
    auto [localStatus, isLocalExist] = indexlib::file_system::FslibWrapper::IsExist(_localIndexRoot).StatusWith();
    RETURN_IF_STATUS_ERROR(localStatus, "check local root exist failed");
    if (!isLocalExist) {
        return Status::OK();
    }
    auto rootDir = indexlib::file_system::Directory::GetPhysicalDirectory(_localIndexRoot)->GetIDirectory();
    assert(rootDir);
    std::set<std::string> keepFiles;
    std::set<segmentid_t> keepSegments;
    std::set<versionid_t> keepVersions;
    bool isAllReservedVersionExist = false;
    // 1. 通过keep_version_count，reader_container算出要保留的文件
    RETURN_IF_STATUS_ERROR(CollectReservedFilesAndDirs(rootDir, VersionVecToVersionSet(reservedVersions), &keepFiles,
                                                       &keepSegments, &keepVersions, &isAllReservedVersionExist),
                           "collect files and directory failed");
    if (!isAllReservedVersionExist) {
        TABLET_LOG(WARN, "not all reserved version in local, not clean");
        //如果本地的version全，不执行本次清理，否则可能一些正在分发的文件会被误删
        return Status::OK();
    }

    // 2. 得到partition目录下所有文件
    std::vector<std::string> allFileList;
    RETURN_IF_STATUS_ERROR(rootDir->ListDir("", /*ListOption*/ {true}, allFileList).Status(), "list file infos failed");
    /*
      文件主要分为以下几类
      1.segment下的文件:
      segment_xxx_xxx
      /index_patch/segment_xxx
      /fence_xxx/segment_xxx

      2.带versionid的特殊文件
      version.xxx entry_table.xxx
      version.xxxx.done

      3.一定需要保留的文件
      fence_rt
      truncate
      adaptive_dict
      schema.json
      index_format_version
    */

    // 3. 算出要清理的文件
    auto needCleanFileList = CaculateNeedCleanFileList(allFileList, keepFiles, keepSegments, keepVersions);
    // 4. 清理文件和目录
    RETURN_IF_STATUS_ERROR(CleanFiles(allFileList, rootDir, needCleanFileList), "clean file list failed");
    TABLET_LOG(INFO, "end clean index files, reserved versions are : [%s], used [%.3f]s",
               autil::legacy::ToJsonString(reservedVersions, true).c_str(), timer.done_sec());
    return Status::OK();
}

Status OnDiskIndexCleaner::CleanFiles(const std::vector<std::string>& allFileList,
                                      const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir,
                                      const std::vector<std::string>& needCleanFileList)
{
    //将文件列表分为文件和目录
    std::vector<std::string> files, dirs;
    for (const auto& filePath : needCleanFileList) {
        auto result = rootDir->IsDir(filePath);
        RETURN_IF_STATUS_ERROR(result.Status(), "IsDir failed, filePath[%s]", filePath.c_str());
        if (result.Value()) {
            dirs.push_back(filePath);
        } else {
            files.push_back(filePath);
        }
    }
    std::set<std::string> allFileSet(allFileList.begin(), allFileList.end());
    //先清理文件
    for (const auto& filePath : files) {
        allFileSet.erase(filePath);
        RETURN_IF_STATUS_ERROR(
            rootDir->RemoveFile(filePath, indexlib::file_system::RemoveOption::MayNonExist()).Status(),
            "remove file failed, filePath[%s]", filePath.c_str());
    }
    std::sort(dirs.begin(), dirs.end());
    std::reverse(dirs.begin(), dirs.end());
    //再清理空目录
    for (const auto& dir : dirs) {
        auto iter = allFileSet.upper_bound(dir);
        if (iter != allFileSet.end() && autil::StringUtil::startsWith(*iter, dir)) {
            TABLET_LOG(DEBUG, "do not remove dir [%s] because child [%s]", dir.c_str(), iter->c_str());
        } else {
            RETURN_IF_STATUS_ERROR(
                rootDir->RemoveDirectory(dir, indexlib::file_system::RemoveOption::MayNonExist()).Status(),
                "remove directory failed, filePath[%s]", dir.c_str());
            allFileSet.erase(dir);
        }
    }
    return Status::OK();
}

std::set<std::string> MergeKeepFiles(const std::set<std::string>& keepFiles, const std::set<versionid_t>& keepVersions)
{
    std::set<std::string> ret = keepFiles;
    ret.insert(INDEX_FORMAT_VERSION_FILE_NAME);
    ret.insert(INDEX_PARTITION_META_FILE_NAME);
    ret.insert(SCHEMA_FILE_NAME);
    return ret;
}

segmentid_t GetSegmentIdFromFilePath(const std::string& filePath)
{
    std::vector<std::string> splitDirs;
    autil::StringUtil::split(splitDirs, filePath, "/");

    if (splitDirs.size() >= 1 && PathUtil::IsValidSegmentDirName(splitDirs[0])) {
        return PathUtil::GetSegmentIdByDirName(splitDirs[0]);
    }
    if (splitDirs.size() >= 2 && PathUtil::IsValidSegmentDirName(splitDirs[1])) {
        return PathUtil::GetSegmentIdByDirName(splitDirs[1]);
    }
    return INVALID_SEGMENTID;
}

bool IsKeepedByVersion(const std::string& path, const std::set<versionid_t>& keepVersions)
{
    std::string fileName = PathUtil::GetFileNameFromPath(path);
    versionid_t versionId = INVALID_VERSIONID;
    versionid_t tmpVersionId;
    if (VersionDeployDescription::ExtractVersionId(fileName, tmpVersionId)) {
        versionId = tmpVersionId;
    } else if (VersionLoader::IsValidVersionFileName(fileName)) {
        versionId = VersionLoader::GetVersionId(fileName);
    } else if (indexlib::file_system::EntryTable::ExtractVersionId(fileName, tmpVersionId)) {
        versionId = tmpVersionId;
    }

    if (versionId == INVALID_VERSIONID) {
        return false;
    }
    return keepVersions.find(versionId) != keepVersions.end();
}

std::vector<std::string> OnDiskIndexCleaner::CaculateNeedCleanFileList(const std::vector<std::string>& allFileList,
                                                                       const std::set<std::string>& keepFilesBase,
                                                                       const std::set<segmentid_t>& keepSegments,
                                                                       const std::set<versionid_t>& keepVersions)
{
    auto keepFiles = MergeKeepFiles(keepFilesBase, keepVersions);
    std::set<std::string> reservedDirs(
        {TRUNCATE_META_DIR_NAME, ADAPTIVE_DICT_DIR_NAME, Fence::GetFenceName(RT_INDEX_PARTITION_DIR_NAME)});
    std::vector<std::string> needCleanFileList;
    for (const auto& filePath : allFileList) {
        auto path = filePath;
        if (keepFiles.find(filePath) != keepFiles.end()) {
            continue;
        }
        if (IsKeepedByVersion(filePath, keepVersions)) {
            continue;
        }
        segmentid_t segmentId = GetSegmentIdFromFilePath(filePath);
        if (keepSegments.find(segmentId) != keepSegments.end()) {
            continue;
        }
        std::string dirName;
        auto pos = filePath.find('/');
        if (pos != std::string::npos) {
            dirName = filePath.substr(0, pos);
        }
        if (reservedDirs.find(dirName) != reservedDirs.end()) {
            continue;
        }
        schemaid_t schemaId = INVALID_SCHEMAID;
        if (PathUtil::ExtractSchemaIdBySchemaFile(filePath, schemaId)) {
            continue;
        }
        needCleanFileList.push_back(filePath);
    }
    return needCleanFileList;
}

Status OnDiskIndexCleaner::CollectAllRelatedVersion(const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir,
                                                    const std::set<versionid_t>& reservedVersionIds,
                                                    std::vector<Version>* versions, bool* isAllReservedVersionExist)
{
    RETURN_IF_STATUS_ERROR(CollectVersionFromLocal(rootDir, versions), "collect versions from local failed");
    auto localVersionIdSet = VersionVecToVersionSet(*versions);
    *isAllReservedVersionExist =
        std::all_of(reservedVersionIds.begin(), reservedVersionIds.end(),
                    [&localVersionIdSet](auto versionId) { return localVersionIdSet.count(versionId) > 0; });
    return Status::OK();
}

Status
OnDiskIndexCleaner::CollectReservedFilesAndDirs(const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir,
                                                const std::set<versionid_t>& reservedVersionIds,
                                                std::set<std::string>* keepFiles, std::set<segmentid_t>* keepSegments,
                                                std::set<versionid_t>* keepVersions, bool* isAllReservedVersionExist)
{
    // 1. 收集本地保留的version
    std::vector<Version> releatedVersions;
    RETURN_IF_STATUS_ERROR(
        CollectAllRelatedVersion(rootDir, reservedVersionIds, &releatedVersions, isAllReservedVersionExist),
        "collect releated version failed");
    if (!*isAllReservedVersionExist) {
        return Status::OK();
    }

    // 2.根据keep version count剔除一些无意义的version
    std::set<versionid_t> reservedVersionIdSet;
    size_t currentKeepCount = 0;
    for (auto iter = releatedVersions.rbegin(); iter != releatedVersions.rend(); ++iter) {
        auto versionId = iter->GetVersionId();
        if (currentKeepCount < _keepVersionCount) {
            reservedVersionIdSet.insert(versionId);
            currentKeepCount++;
        }
        if (reservedVersionIds.count(versionId) > 0) {
            reservedVersionIdSet.insert(versionId);
        }
    }

    std::vector<Version> readerContainerVersions;
    _tabletReaderContainer->GetIncVersions(readerContainerVersions);

    // 3.剔除reader container里有的version
    std::set<versionid_t> versionKeepedByClenaer;
    for (const auto& versionId : reservedVersionIdSet) {
        auto prediction = [&](const Version& readerVersion) { return readerVersion.GetVersionId() == versionId; };
        if (std::find_if(readerContainerVersions.begin(), readerContainerVersions.end(), prediction) ==
            readerContainerVersions.end()) {
            versionKeepedByClenaer.insert(versionId);
        }
    }

    // 4. 从reader contain中取要保留的文件和目录
    _tabletReaderContainer->GetNeedKeepFilesAndSegmentIds(keepFiles, keepSegments);

    // 5. 收集完整保留的segment目录
    for (const auto& version : releatedVersions) {
        if (versionKeepedByClenaer.count(version.GetVersionId()) > 0) {
            for (const auto& segment : version) {
                keepSegments->insert(segment.segmentId);
            }
        }
    }

    for (const auto& version : readerContainerVersions) {
        // by container
        if (version.GetVersionId() >= 0) {
            keepVersions->insert(version.GetVersionId());
        }
    }
    for (const auto& versionId : versionKeepedByClenaer) {
        // by cleaner
        keepVersions->insert(versionId);
    }
    return Status::OK();
}

Status OnDiskIndexCleaner::CollectVersionFromLocal(const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir,
                                                   std::vector<Version>* allVersions)
{
    fslib::FileList fileList;
    std::shared_ptr<indexlib::file_system::Directory> dir(new indexlib::file_system::Directory(rootDir));
    auto status = framework::VersionLoader::ListVersion(dir, &fileList);
    RETURN_IF_STATUS_ERROR(status, "[%s] list version failed", _tabletName.c_str());
    for (auto& fileName : fileList) {
        versionid_t versionId = framework::VersionLoader::GetVersionId(fileName);
        if ((versionId & Version::PRIVATE_VERSION_ID_MASK) != 0) {
            continue;
        }
        Version version;
        auto status = framework::VersionLoader::GetVersion(dir, versionId, &version);
        RETURN_IF_STATUS_ERROR(status, "[%s] get version for dir[%s] failed", _tabletName.c_str(),
                               rootDir->GetOutputPath().c_str());
        allVersions->push_back(version);
    }
    return Status::OK();
}

//#undef TABLET_LOG
} // namespace indexlibv2::framework
