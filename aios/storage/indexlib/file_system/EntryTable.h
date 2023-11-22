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
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/file_system/EntryMeta.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/fslib/FenceContext.h"

namespace indexlib { namespace file_system {

// no throw
class EntryTable : private autil::NoCopyable
{
public:
    EntryTable(const std::string& name, const std::string& outputRoot, bool isRootEntryMetaLazy);
    ~EntryTable();

public:
    static std::string GetEntryTableFileName(int version);
    static bool ExtractVersionId(const std::string& fileName, int& version);

public:
    void Init();

    const std::string* GetPhysicalRootPointer(const std::string& physicalRoot);

public:
    FSResult<EntryMeta> CreateFile(const std::string& path);
    ErrorCode MakeDirectory(const std::string& path, bool recursive, std::vector<EntryMeta>* metas);

    bool UpdateFileSize(const std::string& path, int64_t length);
    bool IsExist(const std::string& path, bool* isDir = nullptr) const;
    bool IsDir(const std::string& path) const;
    std::vector<EntryMeta> ListDir(const std::string& dirPath, bool recursive = false) const;

    FSResult<EntryMeta> AddEntryMeta(const EntryMeta& meta); // insert or overwrite
    void Delete(const EntryMeta& meta);
    void Delete(const std::string& path);

    FSResult<EntryMeta> GetEntryMeta(const std::string& logicalPath) const;
    FSResult<EntryMeta> GetEntryMetaMayLazy(const std::string& logicalPath);
    FSResult<EntryMeta> GetAncestorLazyEntryMeta(const std::string& logicalPath) const;
    const std::string& GetOutputRoot() const { return _outputRoot; }
    const std::string& GetPatchRoot() const { return _patchRoot; }

    ErrorCode Rename(const std::string& srcLogicalPath, const std::string& destLogicalPath, FenceContext* fenceContext,
                     bool inRecoverPhase = false);

    void Clear();
    // optimize STL container members which are rarely modified, eg. roots, packages.
    // But we have to do this in online scene, and it cost too much if we do this every time do delete()
    ErrorCode OptimizeMemoryStructures();
    bool Commit(int versionId, const std::vector<std::string>& wishFileList,
                const std::vector<std::string>& filterDirList, const std::vector<std::string>& wishDirList,
                FenceContext* fenceContext);

    // PreloadDependence is a special entryTable that can only be commit once
    // for a logical_file_system and is the original entryTable before all the other entryTable.
    // The PreloadDependence is more like a pre-mounted link assign files from other root,
    // It can be use to achive the multil-level dependence logical_file_system.
    bool CommitPreloadDependence(FenceContext* fenceContext);

    void UpdateAfterCommit(const std::vector<std::string>& wishFileList, const std::vector<std::string>& wishDirList);

    ErrorCode Freeze(const std::string& logicalFileName);
    ErrorCode Freeze(const std::vector<std::string>& logicalFileNames);
    std::string DebugString() const;
    bool SetEntryMetaMutable(const std::string& logicalPath, bool isMutable);
    bool SetEntryMetaIsMemFile(const std::string& logicalPath, bool isMemFile);
    bool SetEntryMetaLazy(const std::string& logicalPath, bool isLazy);

public:
    // package, physicalPath should be full path
    void UpdatePackageDataFile(const std::string& logicalPath, const std::string& physicalPath, int64_t offset);
    void UpdatePackageMetaFile(const std::string& physicalPath, int64_t fileLength);
    void SetPackageFileLength(const std::string& physicalPath, int64_t fileLength);
    void UpdatePackageFileLengthsCache(const EntryMeta& entryMeta);
    FSResult<int64_t> GetPackageFileLength(const std::string& physicalPath);
    size_t CalculateFileSize() const;

public:
    bool TEST_GetPhysicalPath(const std::string& logicalPath, std::string& physicalPath) const;
    bool TEST_GetPhysicalInfo(const std::string& logicalPath, std::string& physicalRoot, std::string& relativePath,
                              bool& inPackage, bool& isDir) const;

private:
    FSResult<EntryMeta> AddWithoutCheck(const EntryMeta& meta);
    ErrorCode ToString(const std::vector<std::string>& wishFileList, const std::vector<std::string>& wishDirList,
                       const std::vector<std::string>& filterDirList, std::string* result);

    const std::string* FindPhysicalRootForWrite(const std::string& path);
    std::string GetPatchRoot(const std::string& name) const;

    ErrorCode RenamePhysicalPath(const std::string& srcPath, const std::string& destPath, bool isFile,
                                 FenceContext* fenceContext);
    bool ValidatePackageFileLengths();

private:
    mutable autil::RecursiveThreadMutex _lock; // TODO: make sure all relative funcs in lock
    // to avoid duplicately save physicalRoot
    std::set<std::string> _physicalRoots;
    // {normalized logical file path -> EntryMeta}, include all files and dirs, should not start or end with '/'
    std::map<std::string, EntryMeta> _entryMetaMap;
    // {physical full path of package.__data__ or package.__meta__ -> file length}
    // we keep this only to make compatible with old fs when roll back
    std::map<std::string, int64_t> _packageFileLengths;      // may be used for to json
    std::map<std::string, int64_t> _packageFileLengthsCache; // store redirected physicalFullPath
    std::string _name;
    std::string _outputRoot;
    std::string _patchRoot; // TODO: not safe
    EntryMeta _rootEntryMeta;

private:
    friend class EntryTableJsonizable;
    friend class EntryTableMerger;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<EntryTable> EntryTablePtr;
}} // namespace indexlib::file_system
