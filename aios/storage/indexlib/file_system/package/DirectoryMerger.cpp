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
#include "indexlib/file_system/package/DirectoryMerger.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/package/InnerFileMeta.h"
#include "indexlib/file_system/package/PackageFileMeta.h"
#include "indexlib/file_system/package/VersionedPackageFileMeta.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, DirectoryMerger);

ErrorCode DirectoryMerger::MoveFiles(const fslib::EntryList& files, const string& targetDir)
{
    RETURN_IF_FS_ERROR(FslibWrapper::MkDirIfNotExist(targetDir).Code(), "");
    // opt for packagefile
    for (size_t i = 0; i < files.size(); ++i) {
        string folder, fileName;
        SplitFileName(files[i].entryName, folder, fileName);
        if (files[i].isDir) {
            string dest = PathUtil::JoinPath(targetDir, fileName);
            RETURN_IF_FS_ERROR(FslibWrapper::MkDirIfNotExist(dest).Code(), "");
            fslib::EntryList subFiles;
            RETURN_IF_FS_ERROR(FslibWrapper::ListDir(files[i].entryName, subFiles).Code(), "");
            for (size_t j = 0; j < subFiles.size(); ++j) {
                string subSrc = PathUtil::JoinPath(files[i].entryName, subFiles[j].entryName);
                string subDest = PathUtil::JoinPath(dest, subFiles[j].entryName);
                ErrorCode ec = FslibWrapper::Rename(subSrc, subDest /*no fence*/).Code();
                if (ec == FSEC_EXIST) {
                    if (!subFiles[j].isDir) {
                        AUTIL_LOG(ERROR, "rename file [%s] to exist path [%s]", subSrc.c_str(), subDest.c_str());
                        return FSEC_EXIST;
                    }
                    AUTIL_LOG(WARN, "rename directory [%s] to exist path [%s]", subSrc.c_str(), subDest.c_str());
                    FSResult<fslib::EntryList> subEntryListRet = ListFiles(files[i].entryName, subFiles[j].entryName);
                    if (subEntryListRet.ec == FSEC_NOENT || subEntryListRet.result.empty()) {
                        // pass
                    } else if (subEntryListRet.ec != FSEC_OK) {
                        return ec;
                    } else {
                        RETURN_IF_FS_ERROR(MoveFiles(subEntryListRet.result, subDest), "");
                    }
                } else if (ec == FSEC_NOENT) {
                    assert(false);
                } else {
                    RETURN_IF_FS_ERROR(ec, "");
                }
                AUTIL_LOG(INFO, "rename path [%s] to [%s] OK", subSrc.c_str(), subDest.c_str());
            }
        } else {
            const string& srcFile = files[i].entryName;
            const string& dstFile = PathUtil::JoinPath(targetDir, fileName);
            RETURN_IF_FS_ERROR(FslibWrapper::Rename(srcFile, dstFile /*noFence*/).Code(), "");
            AUTIL_LOG(INFO, "rename file [%s] to path [%s]", srcFile.c_str(), dstFile.c_str());
        }
    }
    return FSEC_OK;
}

void DirectoryMerger::SplitFileName(const string& input, string& folder, string& fileName)
{
    size_t found;
    found = input.find_last_of("/\\");
    if (found == string::npos) {
        fileName = input;
        return;
    }
    folder = input.substr(0, found);
    fileName = input.substr(found + 1);
}

FSResult<fslib::EntryList> DirectoryMerger::ListFiles(const string& dir, const string& subDir)
{
    fslib::EntryList entryList;
    string dirPath = FslibWrapper::JoinPath(dir, subDir);
    ErrorCode ec = FslibWrapper::ListDir(dirPath, entryList).Code();
    if (ec == FSEC_NOENT) {
        return {FSEC_OK, entryList};
    } else if (ec != FSEC_OK) {
        return {ec, entryList};
    }
    for (size_t i = 0; i < entryList.size(); ++i) {
        entryList[i].entryName = FslibWrapper::JoinPath(dirPath, entryList[i].entryName);
    }
    return {FSEC_OK, entryList};
}

FSResult<bool> DirectoryMerger::MergePackageFiles(const string& dir, FenceContext* fenceContext)
{
    FileList metaFileNames;
    FSResult<bool> ret = CollectPackageMetaFiles(dir, metaFileNames, fenceContext);
    if (ret.ec != FSEC_OK) {
        return {ret.ec, false};
    }
    if (!ret.result) {
        // no need merge, may be recover
        return {CleanMetaFiles(dir, metaFileNames, fenceContext), true};
    }
    if (metaFileNames.empty()) {
        // non package segment
        return {FSEC_OK, false};
    }

    sort(metaFileNames.begin(), metaFileNames.end());
    ErrorCode ec = MergePackageMeta(dir, metaFileNames, fenceContext);
    if (ec != FSEC_OK) {
        return {ec, true};
    }
    return {CleanMetaFiles(dir, metaFileNames, fenceContext), true};
}

ErrorCode DirectoryMerger::CleanMetaFiles(const string& dir, const FileList& metaFileNames, FenceContext* fenceContext)
{
    for (const string& metaFileName : metaFileNames) {
        RETURN_IF_FS_ERROR(
            FslibWrapper::DeleteFile(PathUtil::JoinPath(dir, metaFileName), DeleteOption::Fence(fenceContext, true))
                .Code(),
            "");
    }
    return FSEC_OK;
}

FSResult<bool> DirectoryMerger::CollectPackageMetaFiles(const string& dir, FileList& metaFileNames,
                                                        FenceContext* fenceContext)
{
    FileList allFileNames;
    auto ec = FslibWrapper::ListDir(dir, allFileNames).Code();
    if (ec == FSEC_NOENT) {
        return {FSEC_OK, false};
    } else if (unlikely(ec != FSEC_OK)) {
        return {ec, false};
    }
    string packageMetaPrefix = string(PACKAGE_FILE_PREFIX) + PACKAGE_FILE_META_SUFFIX;
    map<string, pair<int32_t, string>> descriptionToMeta;
    bool ret = true;
    for (const string& fileName : allFileNames) {
        if (!StringUtil::startsWith(fileName, packageMetaPrefix)) {
            continue;
        }
        // eg: package_file.__meta__
        if (fileName.size() == packageMetaPrefix.size()) {
            assert(fileName == string(PACKAGE_FILE_PREFIX) + PACKAGE_FILE_META_SUFFIX);
            AUTIL_LOG(INFO, "final package file meta [%s] already exist", fileName.c_str());
            ret = false;
            continue;
        }
        // eg: package_file.__meta__.i0t0.0
        versionid_t versionId = VersionedPackageFileMeta::GetVersionId(fileName);
        if (versionId < 0) {
            // eg: package_file.__meta__.i0t0.0.__tmp__
            ErrorCode ec =
                FslibWrapper::DeleteFile(PathUtil::JoinPath(dir, fileName), DeleteOption::Fence(fenceContext, false))
                    .Code();
            if (unlikely(ec != FSEC_OK)) {
                return {ec, false};
            }
            continue;
        }
        string description = VersionedPackageFileMeta::GetDescription(fileName);
        auto iter = descriptionToMeta.find(description);
        if (iter == descriptionToMeta.end()) {
            descriptionToMeta[description] = make_pair(versionId, fileName);
        } else if (versionId > iter->second.first) {
            ErrorCode ec = FslibWrapper::DeleteFile(PathUtil::JoinPath(dir, iter->second.second),
                                                    DeleteOption::Fence(fenceContext, false))
                               .Code();
            if (unlikely(ec != FSEC_OK)) {
                return {ec, false};
            }
            descriptionToMeta[description] = make_pair(versionId, fileName);
        } else {
            assert(versionId < iter->second.first);
            ErrorCode ec =
                FslibWrapper::DeleteFile(PathUtil::JoinPath(dir, fileName), DeleteOption::Fence(fenceContext, false))
                    .Code();
            if (unlikely(ec != FSEC_OK)) {
                return {ec, false};
            }
        }
    }

    for (auto it = descriptionToMeta.begin(); it != descriptionToMeta.end(); ++it) {
        metaFileNames.push_back(it->second.second);
    }
    return {FSEC_OK, ret};
}

ErrorCode DirectoryMerger::MergePackageMeta(const string& dir, const FileList& metaFileNames,
                                            FenceContext* fenceContext)
{
    PackageFileMeta mergedMeta;
    set<InnerFileMeta> dedupInnerFileMetas;
    uint32_t baseFileId = 0;
    for (const string& metaFileName : metaFileNames) {
        VersionedPackageFileMeta meta;
        RETURN_IF_FS_ERROR(meta.Load(PathUtil::JoinPath(dir, metaFileName)).ec, "");
        for (auto it = meta.Begin(); it != meta.End(); ++it) {
            auto tempIt = dedupInnerFileMetas.find(it->GetFilePath());
            if (tempIt == dedupInnerFileMetas.end()) {
                InnerFileMeta newMeta(*it);
                newMeta.SetDataFileIdx(newMeta.IsDir() ? 0 : newMeta.GetDataFileIdx() + baseFileId);
                dedupInnerFileMetas.insert(newMeta);
            } else if (!it->IsDir() || !tempIt->IsDir()) {
                AUTIL_LOG(ERROR, "repeated package data file [%s], [%d,%d]", it->GetFilePath().c_str(), it->IsDir(),
                          tempIt->IsDir());
                return FSEC_ERROR;
            }
        }
        if (meta.GetFileAlignSize() != mergedMeta.GetFileAlignSize()) {
            AUTIL_LOG(ERROR, "inconsistent file align size %lu != %lu", meta.GetFileAlignSize(),
                      mergedMeta.GetFileAlignSize());
            return FSEC_ERROR;
        }
        vector<string> newDataFileNames;
        const vector<string>& dataFileNames = meta.GetPhysicalFileNames();
        const vector<string>& dataFileTags = meta.GetPhysicalFileTags();
        RETURN_IF_FS_ERROR(MovePackageDataFile(baseFileId, dir, dataFileNames, dataFileTags, newDataFileNames), "");
        mergedMeta.AddPhysicalFiles(newDataFileNames, meta.GetPhysicalFileTags());
        baseFileId += dataFileNames.size();
    }
    for (InnerFileMeta innerFile : dedupInnerFileMetas) {
        mergedMeta.AddInnerFile(innerFile);
    }

    return StorePackageFileMeta(dir, mergedMeta, fenceContext);
}

ErrorCode DirectoryMerger::MovePackageDataFile(uint32_t baseFileId, const string& dir,
                                               const vector<string>& srcFileNames, const vector<string>& srcFileTags,
                                               vector<string>& newFileNames)
{
    assert(srcFileTags.size() == srcFileNames.size());
    for (size_t i = 0; i < srcFileNames.size(); ++i) {
        string newFileName =
            PackageFileMeta::GetPackageFileDataPath(PACKAGE_FILE_PREFIX, srcFileTags[i], baseFileId + i);
        newFileNames.push_back(newFileName);
        string srcFilePath = PathUtil::JoinPath(dir, srcFileNames[i]);
        string dstFilePath = PathUtil::JoinPath(dir, newFileName);
        ErrorCode ec = FslibWrapper::Rename(srcFilePath, dstFilePath /*no fence*/).Code();
        if (ec == FSEC_NOENT) {
            bool isExist = true;
            RETURN_IF_FS_ERROR(FslibWrapper::IsExist(dstFilePath, isExist), "");
            if (!isExist) {
                AUTIL_LOG(ERROR, "move file [%s] to [%s]", srcFilePath.c_str(), dstFilePath.c_str());
                return FSEC_NOENT;
            }
        } else if (ec == FSEC_EXIST) {
            bool isExist = true;
            RETURN_IF_FS_ERROR(FslibWrapper::IsExist(srcFilePath, isExist), "");
            if (!isExist) {
                AUTIL_LOG(ERROR, "move file [%s] to [%s]", srcFilePath.c_str(), dstFilePath.c_str());
                return FSEC_NOENT;
            }
        } else if (ec != FSEC_OK) {
            AUTIL_LOG(ERROR, "rename package data file [%s] to [%s] failed", srcFilePath.c_str(), dstFilePath.c_str());
            return ec;
        }
        AUTIL_LOG(INFO, "rename package data file [%s] to [%s]", srcFilePath.c_str(), dstFilePath.c_str());
    }
    return FSEC_OK;
}

ErrorCode DirectoryMerger::StorePackageFileMeta(const string& dir, PackageFileMeta& mergedMeta,
                                                FenceContext* fenceContext)
{
    if (mergedMeta.GetPhysicalFileNames().empty() && mergedMeta.GetInnerFileCount() > 0) {
        // touch a empty file
        string emptyDataFileName = PackageFileMeta::GetPackageFileDataPath(PACKAGE_FILE_PREFIX, "", 0);
        string emptyDataFilePath = PathUtil::JoinPath(dir, emptyDataFileName);
        RETURN_IF_FS_ERROR(FslibWrapper::AtomicStore(emptyDataFilePath, "", true, fenceContext).Code(), "");
        mergedMeta.AddPhysicalFile(emptyDataFileName, "");
    }

    ErrorCode ec = mergedMeta.Store(dir, fenceContext);
    if (ec == FSEC_EXIST) {
        return FSEC_OK;
    }
    return ec;
}
}} // namespace indexlib::file_system
