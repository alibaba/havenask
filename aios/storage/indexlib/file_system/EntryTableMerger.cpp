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
#include "indexlib/file_system/EntryTableMerger.h"

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Lock.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/EntryMeta.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/package/PackageFileMeta.h"
#include "indexlib/file_system/package/VersionedPackageFileMeta.h"
#include "indexlib/util/PathUtil.h"

using autil::ScopedLock;
using std::string;

using indexlib::util::PathUtil;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, EntryTableMerger);

EntryTableMerger::EntryTableMerger(std::shared_ptr<EntryTable> entryTable) : _entryTable(entryTable) {}

EntryTableMerger::~EntryTableMerger() {}

// In each new dir that contains package files(after packges have been moved from original dir to new dir),
// rename the package files properly to follow merged pattern.
// e.g. new_dir/package_file.__data__.i0t1.0 -> new_dir/package_file.__data__1
// For failure recovery, this function assumes that the entry table is correct and contains new physical dir
// path of package files(package files can be physically renamed on disk [partially]). If this function fails
// during execution and memory(entry table) corrupts, it can safely rerun.
// TODO(panghai.hj): Make sure DirectoryMerger::MergePackageFiles is reentrant.
ErrorCode EntryTableMerger::RenamePackageMetaAndDataFiles()
{
    ScopedLock lock(_entryTable->_lock);
    std::set<string> processedDir;
    std::map<string, string> renamedPackageFilePhysicalNameMap;
    for (const auto& pair : _entryTable->_entryMetaMap) {
        const EntryMeta& meta = pair.second;
        if (!meta.IsInPackage()) {
            continue;
        }
        if (meta.IsDir()) {
            if (_entryTable->_packageFileLengths.count(meta.GetRawFullPhysicalPath()) == 0) {
                _entryTable->_packageFileLengths[meta.GetRawFullPhysicalPath()] = 0;
            }
            continue;
        }
        string physicalPath = meta.GetFullPhysicalPath();
        string physicalDir = PathUtil::GetParentDirPath(physicalPath);
        if (processedDir.find(physicalDir) == processedDir.end()) {
            processedDir.emplace(physicalDir);
            PackageFileMeta mergedMeta;
            // bool packageMergeHappened;
            // RETURN_IF_FS_ERROR(DirectoryMerger::MergePackageFiles(
            //                     physicalDir, &renamedPackageFilePhysicalNameMap, &mergedMeta,
            //                     &packageMergeHappened), "");
            // if(!packageMergeHappened)
            // {
            //     AUTIL_LOG(INFO, "No package file to merge under dir %s", physicalDir.c_str());
            // }
            string mergedMetaPath =
                PathUtil::JoinPath(physicalDir, PackageFileMeta::GetPackageFileMetaPath(PACKAGE_FILE_PREFIX));
            string metaJsonStr;
            RETURN_IF_FS_ERROR(mergedMeta.ToString(&metaJsonStr), "");
            _entryTable->_packageFileLengths[mergedMetaPath] = metaJsonStr.size();
        }
    }
    for (const auto& it : renamedPackageFilePhysicalNameMap) {
        assert(_entryTable->_packageFileLengths.count(it.first) == 1);
        _entryTable->_packageFileLengths[it.second] = _entryTable->_packageFileLengths.at(it.first);
        _entryTable->_packageFileLengths.erase(it.first);
    }
    for (const auto& pair : _entryTable->_entryMetaMap) {
        const EntryMeta& meta = pair.second;
        if (meta.IsDir() || !meta.IsInPackage()) {
            continue;
        }
        string physicalPath = meta.GetFullPhysicalPath();
        if (renamedPackageFilePhysicalNameMap.find(physicalPath) != renamedPackageFilePhysicalNameMap.end()) {
            _entryTable->UpdatePackageDataFile(
                /*logicalPath=*/meta.GetLogicalPath(),
                /*physicalPath=*/renamedPackageFilePhysicalNameMap.at(physicalPath),
                /*offset=*/meta.GetOffset());
        }
        // instace_0/seg_1/attr/name/data
        // instace_0/seg_1/attr/name/offset
        // instace_0/seg_1/attr/age/data
        // instace_0/seg_1/attr/age/offset
    }
    for (auto it = _entryTable->_packageFileLengths.begin(); it != _entryTable->_packageFileLengths.end();) {
        if (VersionedPackageFileMeta::IsValidFileName(PathUtil::GetFileName(it->first))) {
            _entryTable->_packageFileLengths.erase(it++);
        } else {
            ++it;
        }
    }
    return FSEC_OK;
}

// Return true if path should be created in the merged destination root.
bool ShouldCreateDir(const std::set<string>& packageFileDstDirPhysicalPaths, const string& path)
{
    // if path itself contains package files, the path is a segment level dir and should physically exist.
    if (packageFileDstDirPhysicalPaths.find(path) != packageFileDstDirPhysicalPaths.end()) {
        return true;
    }
    auto it = std::find_if(packageFileDstDirPhysicalPaths.begin(), packageFileDstDirPhysicalPaths.end(),
                           [path](const string& rootPath) -> bool { return PathUtil::IsInRootPath(path, rootPath); });
    // if path is inside a package, we shouldn't create the dir.
    if (it != packageFileDstDirPhysicalPaths.end()) {
        return false;
    }
    // path is a regular path, not in a package
    return true;
}

// Handle move dir case during merge.
// Don't need to create dir for dir that only appears in packges.
// Update entry-meta for dir to new physical root and path.
ErrorCode EntryTableMerger::MaybeHandleDir(const std::set<string>& packageFileDstDirPhysicalPaths)
{
    for (auto it = _entryTable->_entryMetaMap.begin(); it != _entryTable->_entryMetaMap.end(); ++it) {
        EntryMeta& meta = it->second;
        if (!meta.IsDir() || !meta.IsOwner()) {
            continue;
        }
        if (meta.IsInPackage()) {
            std::string packageFileDir = PathUtil::GetParentDirPath(meta.GetPhysicalPath());
            std::string newPhysicalFileName = PackageFileMeta::GetPackageFileMetaPath(PACKAGE_FILE_PREFIX, "");
            meta.SetPhysicalPath(PathUtil::JoinPath(packageFileDir, newPhysicalFileName));
        }
        const string* destRoot = _entryTable->FindPhysicalRootForWrite(meta.GetLogicalPath());
        meta.SetPhysicalRoot(destRoot);
        string dest = PathUtil::JoinPath(*destRoot, meta.GetLogicalPath());
        if (!ShouldCreateDir(packageFileDstDirPhysicalPaths, dest)) {
            continue;
        }
        auto ec = FslibWrapper::MkDir(dest, /*recursive=*/true, /*ignoreExist=*/true).Code();
        if (unlikely(ec != FSEC_OK)) {
            AUTIL_LOG(ERROR, "Mkdir failed, path [%s], ec [%d]", dest.c_str(), ec);
            return ec;
        }
    }
    return FSEC_OK;
}

// Handle move file case during merge, for both regular and package files.
// Package files are moved in this function and paths recorded in return value movedPackageDataFiles.
// Package files need to be merged later in this class.
ErrorCode EntryTableMerger::MaybeHandleFile(bool inRecoverPhase, std::map<string, string>* movedPackageDataFiles)
{
    for (auto it = _entryTable->_entryMetaMap.begin(); it != _entryTable->_entryMetaMap.end(); ++it) {
        EntryMeta& meta = it->second;

        if (meta.IsDir() || !meta.IsOwner() || meta.IsMemFile() ||
            meta.GetPhysicalRoot() == _entryTable->GetOutputRoot()) {
            continue;
        }

        const string* destRoot = _entryTable->FindPhysicalRootForWrite(meta.GetLogicalPath());
        string src = meta.GetFullPhysicalPath();
        string dest;
        dest = PathUtil::JoinPath(*destRoot, meta.GetPhysicalPath());

        if (inRecoverPhase) {
            bool isDir = false;
            if (_entryTable->IsExist(dest, &isDir) && isDir) {
                AUTIL_LOG(INFO, "InConsistence in recover phase, dest file [%s] already exist, will skip merge recover",
                          dest.c_str());
                // TODO: Handle rename case in recover.
                assert(false);
            }
        } else {
            // normal merge, call from LogicalFileSystem::Merge
            if (src != dest) {
                AUTIL_LOG(DEBUG, "mv file from [%s] to [%s]", src.c_str(), dest.c_str());
                if (movedPackageDataFiles->find(src) != movedPackageDataFiles->end()) {
                    assert(movedPackageDataFiles->at(src) == dest);
                } else {
                    auto ec = _entryTable->RenamePhysicalPath(src, dest, /*isFile=*/true, FenceContext::NoFence());
                    if (unlikely(ec != FSEC_OK)) {
                        AUTIL_LOG(ERROR, "Rename file [%s] => [%s] failed, ec [%d]", src.c_str(), dest.c_str(), ec);
                        return ec;
                    }
                    if (meta.IsInPackage()) {
                        movedPackageDataFiles->insert(std::pair<string, string>(src, dest));
                    }
                }
            }
        }
        meta.SetPhysicalRoot(destRoot);
        // We (very probably) should update physical path during the entire merge phyase for
        // package files only. Regular files and dirs physical path need to move physically, but
        // not inside entry-table. In entry table, only their physical root needs to be updated.

        // if (!meta.IsInPackage())
        // {
        //     meta.SetPhysicalPath(meta.GetLogicalPath());
        // }
    }
    return FSEC_OK;
}

ErrorCode EntryTableMerger::HandleMovePackageMetaFiles(const std::map<string, string>& movedPackageDataFiles,
                                                       std::set<string>* destDirSet)
{
    std::map<string, std::vector<string>> srcDirToDescriptions;
    std::map<string, string> srcDirToDestDir;
    // Collect source and destination dir information, description of package files from source dir
    for (const auto& pair : movedPackageDataFiles) {
        string srcDir = PathUtil::GetParentDirPath(pair.first);
        string dstDir = PathUtil::GetParentDirPath(pair.second);
        string packageDataFileName = PathUtil::GetFileName(pair.first);
        string description = VersionedPackageFileMeta::GetDescription(packageDataFileName);
        if (srcDirToDescriptions.find(srcDir) == srcDirToDescriptions.end()) {
            std::vector<string> descs {description};
            srcDirToDescriptions[srcDir] = descs;
        } else {
            srcDirToDescriptions[srcDir].push_back(description);
        }
        srcDirToDestDir[srcDir] = dstDir;
        destDirSet->insert(dstDir);
    }

    // Actually move package files from source to desnation dir and rename
    // from e.g. package_file.__meta__.i0t0.0 to package_file.__meta__0
    string packageMetaPrefix = string(PACKAGE_FILE_PREFIX) + PACKAGE_FILE_META_SUFFIX;
    for (const auto& pair : srcDirToDestDir) {
        FileList files;
        const string& srcDir = pair.first;
        auto ec = FslibWrapper::ListDir(srcDir, files).Code();
        if (unlikely(FSEC_OK != ec)) {
            AUTIL_LOG(ERROR, "ListDir error: List [%s], ec en[%d]", srcDir.c_str(), ec);
            return ec;
        }
        for (const string& file : files) {
            if (!autil::StringUtil::startsWith(file, packageMetaPrefix)) {
                continue;
            }
            string description = VersionedPackageFileMeta::GetDescription(file);
            if (std::find(srcDirToDescriptions[srcDir].begin(), srcDirToDescriptions[srcDir].end(), description) !=
                srcDirToDescriptions[srcDir].end()) {
                string src = PathUtil::JoinPath(srcDir, file);
                string dest = PathUtil::JoinPath(pair.second, file);
                auto ec = _entryTable->RenamePhysicalPath(src, dest, true, FenceContext::NoFence());
                if (unlikely(ec != FSEC_OK)) {
                    AUTIL_LOG(ERROR, "Rename file [%s] => [%s] failed, ec [%d]", src.c_str(), dest.c_str(), ec);
                    return ec;
                }
            }
        }
    }
    return FSEC_OK;
}

ErrorCode EntryTableMerger::Merge(bool inRecoverPhase)
{
    ScopedLock lock(_entryTable->_lock);
    /** TODO:
     * 1. Optimize: end merge only need to move instance_x/segment_x/ to segment_x/, instead of move files one by one
     **/

    // Keep track of package data files that are moved. For each package data file, it only needs to move once.
    // Map: old packge data file physical path->new package data file physical path.
    // For regular files, move from old root to new root.
    std::map<string, string> movedPackageDataFiles;
    RETURN_IF_FS_ERROR(MaybeHandleFile(inRecoverPhase, &movedPackageDataFiles), "");
    // For package files, we need to move both package data and meta files. The code above only
    // moves package data files. In each dir that contains package data files, there are some
    // package meta files that need to be merged and moved. Package meta files are moved for
    // backard compatible reasons. HandleMovePackageMetaFiles returns src dir that contains
    // package files to dest dir. For all the other dirs that are not in
    // packageFileSrcDirToDstDirMap, they are either normal dirs, or dirs that are sub dirs to
    // packages. We don't need to create physical dir for those that are under a package.
    std::set<string> packageFileDstDirPhysicalPaths;
    RETURN_IF_FS_ERROR(HandleMovePackageMetaFiles(movedPackageDataFiles, &packageFileDstDirPhysicalPaths), "");
    // Move normal and package dir from old to new root.
    RETURN_IF_FS_ERROR(MaybeHandleDir(packageFileDstDirPhysicalPaths), "");
    RETURN_IF_FS_ERROR(RenamePackageMetaAndDataFiles(), "");

    return FSEC_OK;
}
}} // namespace indexlib::file_system
