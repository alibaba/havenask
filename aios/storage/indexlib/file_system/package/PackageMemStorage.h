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

#include <list>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/MemStorage.h"
#include "indexlib/file_system/package/PackageFileMeta.h"

namespace indexlib { namespace file_system {
struct WriterOption;
}} // namespace indexlib::file_system

namespace indexlib { namespace file_system {

class PackageMemStorage : public MemStorage
{
public:
    PackageMemStorage(bool isReadonly, const util::BlockMemoryQuotaControllerPtr& memController,
                      std::shared_ptr<EntryTable> entryTable) noexcept;
    ~PackageMemStorage() noexcept;

public:
    ErrorCode MakeDirectory(const std::string& logicalDirPath, const std::string& physicalDirPath, bool recursive,
                            bool packageHint) noexcept override;
    ErrorCode RemoveDirectory(const std::string& logicalDirPath, const std::string& physicalDirPath,
                              FenceContext* fenceContext) noexcept override;
    ErrorCode StoreFile(const std::shared_ptr<FileNode>& fileNode, const WriterOption& writerOption) noexcept override;

    ErrorCode FlushPackage(const std::string& logicalDirPath) noexcept override;

    void CleanCache() noexcept override { _fileNodeCache->Clean(); }
    FSStorageType GetStorageType() const noexcept override { return FSST_PACKAGE_MEM; }
    std::string DebugString() const noexcept override;

private:
    ErrorCode DoFlushPackage(const std::string& logicalDirPath) noexcept;

    ErrorCode CollectInnerFileAndDumpParam(const std::string& logicalDirPath, std::vector<std::string>& logicalDirs,
                                           std::vector<std::string>& dirVec,
                                           std::vector<std::shared_ptr<FileNode>>& fileNodeVec,
                                           std::vector<WriterOption>& dumpParamVec) noexcept;
    bool GetPackageDirPath(const std::string& logicalDirPath, std::string& packageDirPath) noexcept;
    FSResult<PackageFileMetaPtr>
    GeneratePackageFileMeta(const std::string& logicalDirPath, const std::string& physicalDirPath,
                            const std::vector<std::string>& innerDirs,
                            const std::vector<std::shared_ptr<FileNode>>& innerFiles) noexcept;

private:
    using InnerInMemoryFile = std::pair<std::shared_ptr<FileNode>, WriterOption>;
    using InnerInMemFileMap = std::unordered_map<std::string, std::list<InnerInMemoryFile>>;

private:
    mutable autil::ThreadMutex _lock;
    std::map<std::string, std::pair<std::string, bool>> _innerDirs; // logicalDirPath -> <physicalDirPath,isPackage>
    InnerInMemFileMap _innerInMemFileMap;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<PackageMemStorage> PackageMemStoragePtr;
}} // namespace indexlib::file_system
