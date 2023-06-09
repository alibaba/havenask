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

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/file_system/package/InnerFileMeta.h"

namespace indexlib { namespace file_system {
class FileNode;

// nothrow, but Jsonize
class PackageFileMeta : public autil::legacy::Jsonizable
{
public:
    PackageFileMeta();
    ~PackageFileMeta();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    ErrorCode InitFromString(const std::string& metaContent);

    // only for build
    ErrorCode InitFromFileNode(const std::string& logicalDirPath, const std::string& packageFilePath,
                               const std::vector<std::shared_ptr<FileNode>>& fileNodes, size_t fileAlignSize);

    void AddInnerFile(InnerFileMeta innerFileMeta);

    ErrorCode Store(const std::string& dir, FenceContext* fenceContext);
    ErrorCode Store(const std::shared_ptr<IDirectory>& dir);

    ErrorCode Load(const std::string& physicalPath);

    ErrorCode ToString(std::string* result) const;

    InnerFileMeta::Iterator Begin() const { return _fileMetaVec.begin(); }

    InnerFileMeta::Iterator End() const { return _fileMetaVec.end(); }

    size_t GetFileAlignSize() const { return _fileAlignSize; }

    void SetFileAlignSize(size_t fileAlignSize) { _fileAlignSize = fileAlignSize; }

    size_t GetInnerFileCount() const { return _fileMetaVec.size(); }

    // TODO, add const std::string& tag
    void AddPhysicalFile(const std::string& name, const std::string& tag);
    void AddPhysicalFiles(const std::vector<std::string>& names, const std::vector<std::string> tags);

    const std::vector<std::string>& GetPhysicalFileNames() const { return _physicalFileNames; }
    const std::vector<std::string>& GetPhysicalFileTags() const { return _physicalFileTags; }
    const std::vector<size_t>& GetPhysicalFileLengths() const { return _physicalFileLengths; }

    const std::string& GetPhysicalFileName(uint32_t dataFileIdx) const
    {
        assert(dataFileIdx < _physicalFileNames.size());
        return _physicalFileNames[dataFileIdx];
    }
    const std::string& GetPhysicalFileTag(uint32_t dataFileIdx) const
    {
        assert(dataFileIdx < _physicalFileTags.size());
        return _physicalFileTags[dataFileIdx];
    }
    size_t GetPhysicalFileLength(uint32_t dataFileIdx) const
    {
        if (unlikely(_physicalFileLengths.empty())) {
            const_cast<PackageFileMeta*>(this)->ComputePhysicalFileLengths();
        }
        assert(dataFileIdx < _physicalFileLengths.size());
        return _physicalFileLengths[dataFileIdx];
    }

    ErrorCode GetMetaStrLength(size_t* length) const;

public:
    static std::string GetPackageFileDataPath(const std::string& packageFilePath, const std::string& description,
                                              uint32_t dataFileIdx);

    static std::string GetPackageFileMetaPath(const std::string& packageFilePath, const std::string& description = "",
                                              int32_t metaVersionId = -1);

    // package data or meta file with same [dir & packageFilePrefix] is a group
    // eg: dfs://ea120/package_file.__data__.MERGE.3 => dfs://ea120/package_file
    //     dfs://ea120/package_file.__meta__.MERGE.3 => dfs://ea120/package_file
    static ErrorCode GetPackageFileGroup(const std::string& packageFilePath, std::string* packageFileGroup);
    static int32_t GetPackageDataFileIdx(const std::string& packageFileDataPath);
    static int32_t GetVersionId(const std::string& fileName);
    // <IS_PACKAGE, IS_PACKAGE_META>
    // <true, true> for packageMetaFile, <true, false> for packageDataFile, other for non
    static std::pair<bool, bool> IsPackageFileName(const std::string& fileName);
    static int32_t GetDataFileIdx(const std::string& fileName);

protected:
    void SortInnerFileMetas();
    void ComputePhysicalFileLengths();

private:
    ErrorCode GetRelativeFilePath(const std::string& parentPath, const std::string& absPath, std::string* relativePath);
    ErrorCode Validate() const;

private:
    std::vector<std::string> _physicalFileNames;
    std::vector<std::string> _physicalFileTags;
    std::vector<size_t> _physicalFileLengths;
    InnerFileMeta::InnerFileMetaVec _fileMetaVec;
    size_t _fileAlignSize;
    int64_t _metaStrLen;

private:
    friend class PackageFileMetaTest;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<PackageFileMeta> PackageFileMetaPtr;
}} // namespace indexlib::file_system
