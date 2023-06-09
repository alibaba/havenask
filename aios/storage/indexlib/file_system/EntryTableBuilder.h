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
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/MountDirOption.h"

namespace indexlib { namespace file_system {

struct FileSystemOptions;
class EntryMeta;
class EntryTable;
class LifecycleTable;
class EntryTableJsonizable;

// no throw
class EntryTableBuilder
{
public:
    EntryTableBuilder();
    ~EntryTableBuilder();

public:
    std::shared_ptr<EntryTable> CreateEntryTable(const std::string& name, const std::string& outputRoot,
                                                 const std::shared_ptr<FileSystemOptions>& fsOptions);
    // just for deploy, valid when last mount version from entry table file
    const EntryMeta* GetLastEntryTableEntryMeta() const;

    // [physicalRoot]/... ->  [logicalPath]/...
    // logicalPath is logicalPath
    ErrorCode MountVersion(const std::string& physicalRoot, versionid_t versionId, const std::string& logicalPath,
                           MountDirOption mountOption);
    // [physicalRoot + physicalPath]/... -->  [logicalPath]/...
    // smart processed segment file list, including truncate_meta, adaptive_bitmap_meta...
    ErrorCode MountSegment(const std::string& physicalRoot, const std::string& physicalPath,
                           const std::string& logicalPath, FSMountType mountType);
    // [physicalRoot + physicalPath]/... -->  [logicalPath]/...
    ErrorCode MountDirRecursive(const std::string& physicalRoot, const std::string& physicalPath,
                                const std::string& logicalPath, MountDirOption mountOption);
    // MountDirLazy only mount one entry which is the dir itself
    ErrorCode MountDirLazy(const std::string& physicalRoot, const std::string& physicalPath,
                           const std::string& logicalPath, MountDirOption mountOption);
    ErrorCode MountPackageDir(const std::string& physicalRoot, const std::string& physicalPath,
                              const std::string& logicalDirPath, FSMountType mountType);
    // [physicalRoot + physicalPath] -->  [logicalPath]
    // smart processed package meta file
    ErrorCode MountFile(const std::string& physicalRoot, const std::string& physicalPath,
                        const std::string& logicalPath, int64_t length, FSMountType mountType);

    ErrorCode SetDirLifecycle(const std::string& rawPath, const std::string& lifecycle);

public:
    static FSResult<versionid_t> GetLastVersion(const std::string& rootPath);

public:
    ErrorCode TEST_FromEntryTableString(const std::string& jsonStr, const std::string& defaultPhysicalRoot);

private:
    // implementation of the public interface
    ErrorCode DoMountVersion(const std::string* pPhysicalRoot, versionid_t versionId, const std::string& logicalPath,
                             bool isOwner, ConflictResolution conflictResolution);
    ErrorCode DoMountSegment(const std::string* pPhysicalRoot, const std::string& segmentDir,
                             const std::string& logicalPath, bool isOwner);
    ErrorCode DoMountDirRecursive(const std::string* pPhysicalRoot, const std::string& physicalPath,
                                  const std::string& logicalPath, bool isOwner, ConflictResolution conflictResolution);
    ErrorCode DoMountFile(const std::string* pPhysicalRoot, const std::string& physicalPath,
                          const std::string& logicalPath, int64_t length, bool isOwner,
                          ConflictResolution conflictResolution);
    // version file & segment_info file dumps after entry_table & index_file_list
    ErrorCode VerifyMayNotExistFile(const std::string& logicalPath);

private:
    ErrorCode MountFromEntryTable(const std::string* pPhysicalRoot, versionid_t versionId,
                                  const std::string& logicalPath, bool isOwner, ConflictResolution conflictResolution);
    ErrorCode MountFromDeployMeta(const std::string* pPhysicalRoot, versionid_t versionId,
                                  const std::string& logicalPath, bool isOwner);
    ErrorCode MountFromVersion(const std::string* pPhysicalRoot, versionid_t versionId, const std::string& logicalPath,
                               bool isOwner);
    ErrorCode MountFromPreloadEntryTable(const std::string* pPhysicalRoot, const std::string& logicalPath,
                                         bool isOwner);

    ErrorCode MountPackageFile(const std::string* pPhysicalRoot, const std::string& physicalPath,
                               const std::string& logicalPath, bool isOwner);
    ErrorCode MountIndexFileList(const std::string* pPhysicalRoot, const std::string& physicalPath,
                                 const std::string& logicalPath, bool isOwner);

    void MountNormalFile(const std::string* pPhysicalRoot, const std::string& physicalPath,
                         const std::string& logicalPath, int64_t length, bool isOwner,
                         ConflictResolution conflictResolution);
    void MountNormalDir(const std::string* pPhysicalRoot, const std::string& physicalPath,
                        const std::string& logicalPath, bool isOwner, ConflictResolution conflictResolution);
    void MountDir(const std::string* pPhysicalRoot, const std::string& physicalPath, const std::string& logicalPath,
                  bool isOwner, ConflictResolution conflictResolution);

    void RewriteEntryMeta(EntryMeta& entryMeta) const;

    ErrorCode FromEntryTableString(const std::string& jsonStr, const std::string& logicalPath,
                                   const std::string& defaultPhysicalRoot, bool isOwner,
                                   ConflictResolution conflictResolution);
    ErrorCode FillEntryTable(EntryTableJsonizable& json, const std::string& logicalPath,
                             const std::string& defaultPhysicalRoot, bool isOwner,
                             ConflictResolution conflictResolution);
    ErrorCode FillFromFiles(EntryTableJsonizable& json, std::vector<EntryMeta>* entryMetas,
                            const std::string& logicalPath, bool isOwner, bool isMutable,
                            const std::string& defaultPhysicalRoot);
    ErrorCode FillFromPackageFiles(EntryTableJsonizable& json, std::vector<EntryMeta>* entryMetas,
                                   const std::string& logicalPath, bool isOwner, bool isMutable,
                                   const std::string& defaultPhysicalRoot);

    static bool CompareFile(const std::string& left, const std::string& right);

private:
    std::shared_ptr<FileSystemOptions> _options;
    std::shared_ptr<EntryTable> _entryTable;
    std::shared_ptr<LifecycleTable> _lifecycleTable;
    std::shared_ptr<EntryMeta> _lastEntryTableEntryMeta;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<EntryTableBuilder> EntryTableBuilderPtr;
}} // namespace indexlib::file_system
