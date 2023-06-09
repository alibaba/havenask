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
#include "indexlib/file_system/package/PackageFileMeta.h"

#include <algorithm>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>
#include <unistd.h>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, PackageFileMeta);

PackageFileMeta::PackageFileMeta() : _fileAlignSize(getpagesize()), _metaStrLen(-1) {}

PackageFileMeta::~PackageFileMeta() {}

ErrorCode PackageFileMeta::InitFromString(const std::string& metaContent)
{
    autil::legacy::FromJsonString(*this, metaContent);
    // compatity problem:
    // when old binary read a new package_meta file, 'meta.ToString().size()' will get a wrong length which resulting
    // reopen fail.
    _metaStrLen = metaContent.size();
    RETURN_IF_FS_ERROR(Validate(), "");
    return FSEC_OK;
}

ErrorCode PackageFileMeta::InitFromFileNode(const std::string& logicalDirPath, const string& packageFilePath,
                                            const vector<std::shared_ptr<FileNode>>& fileNodes, size_t fileAlignSize)
{
    _fileAlignSize = fileAlignSize;
    size_t curFileOffset = 0;

    string dirPath = PathUtil::GetParentDirPath(packageFilePath);
    for (size_t i = 0; i < fileNodes.size(); i++) {
        string fileRelativePath;
        RETURN_IF_FS_ERROR(GetRelativeFilePath(dirPath, fileNodes[i]->GetPhysicalPath(), &fileRelativePath), "");
        if (fileNodes[i]->GetType() == FSFT_DIRECTORY) {
            InnerFileMeta innerFileMeta(fileRelativePath, true);
            innerFileMeta.Init(0, 0, 0);
            _fileMetaVec.push_back(innerFileMeta);
        } else {
            assert(fileNodes[i]->GetType() == FSFT_MEM);
            InnerFileMeta innerFileMeta(fileRelativePath, false);
            innerFileMeta.Init(curFileOffset, fileNodes[i]->GetLength(), 0);
            curFileOffset += (fileNodes[i]->GetLength() + _fileAlignSize - 1) / _fileAlignSize * _fileAlignSize;
            _fileMetaVec.push_back(innerFileMeta);
        }
    }
    string packageFileDataPath = GetPackageFileDataPath(packageFilePath, "", 0);
    AddPhysicalFile(util::PathUtil::GetFileName(packageFileDataPath), "");
    ComputePhysicalFileLengths();
    return FSEC_OK;
}

void PackageFileMeta::AddInnerFile(InnerFileMeta innerFileMeta) { _fileMetaVec.push_back(innerFileMeta); }

ErrorCode PackageFileMeta::Load(const string& physicalPath)
{
    // TODO: Make sure physical path is right.
    // string physicalPath = GetPackageFileMetaPath(PathUtil::JoinPath(dir, PACKAGE_FILE_PREFIX));
    auto ret = JsonUtil::Load(physicalPath, this);
    _metaStrLen = ret.result;
    return ret.ec;
}

ErrorCode PackageFileMeta::ToString(std::string* result) const
{
    RETURN_IF_FS_ERROR(Validate(), "");
    *result = autil::legacy::ToJsonString(*this);
    return FSEC_OK;
}

ErrorCode PackageFileMeta::Store(const std::shared_ptr<IDirectory>& dir)
{
    SortInnerFileMetas();
    ComputePhysicalFileLengths();
    string metaPath = GetPackageFileMetaPath(PACKAGE_FILE_PREFIX);
    string jsonStr;
    RETURN_IF_FS_ERROR(ToString(&jsonStr), "convert package file meta to string failed.");
    RETURN_IF_FS_ERROR(dir->Store(metaPath, jsonStr, WriterOption::AtomicDump()),
                       "store package file meta to dir[%s] failed.", dir->GetPhysicalPath("").c_str());
    return FSEC_OK;
}

ErrorCode PackageFileMeta::Store(const string& dir, FenceContext* fenceContext)
{
    SortInnerFileMetas();
    ComputePhysicalFileLengths();
    string metaPath = GetPackageFileMetaPath(PathUtil::JoinPath(dir, PACKAGE_FILE_PREFIX));
    string jsonStr;
    RETURN_IF_FS_ERROR(ToString(&jsonStr), "");
    ErrorCode ec = FslibWrapper::AtomicStore(metaPath, jsonStr, false, fenceContext).Code();
    if (unlikely(ec == FSEC_EXIST)) {
        AUTIL_LOG(ERROR, "atomic store failed: file [%s] already exist", metaPath.c_str());
    } else if (unlikely(ec != FSEC_OK)) {
        AUTIL_LOG(ERROR, "atomic store failed path[%s]", metaPath.c_str());
    }
    return ec;
}

ErrorCode PackageFileMeta::GetRelativeFilePath(const string& parentPath, const string& absPath, string* relativePath)
{
    if (parentPath == absPath) {
        *relativePath = "";
        return FSEC_OK;
    }

    string normalizeDirPath = PathUtil::NormalizeDir(parentPath);
    if (absPath.find(normalizeDirPath) != 0) {
        AUTIL_LOG(ERROR, "Invalid inner file path [%s]", absPath.c_str());
        return FSEC_ERROR;
    }
    *relativePath = absPath.substr(normalizeDirPath.size());
    return FSEC_OK;
}

void PackageFileMeta::SortInnerFileMetas()
{
    auto compare = [](const InnerFileMeta& left, const InnerFileMeta& right) {
        // <idx, offset, dir, length, path>
        // dir always has offset=0, length=0
        assert(!left.IsDir() || (left.GetLength() == 0 && left.GetOffset() == 0));
        assert(!right.IsDir() || (right.GetLength() == 0 && right.GetOffset() == 0));

        if (left.GetDataFileIdx() != right.GetDataFileIdx()) {
            return left.GetDataFileIdx() < right.GetDataFileIdx();
        }
        if (left.GetOffset() != right.GetOffset()) {
            return left.GetOffset() < right.GetOffset();
        }
        if (left.IsDir() != right.IsDir()) {
            return left.IsDir();
        }
        if (left.GetLength() != right.GetLength()) {
            return left.GetLength() < right.GetLength();
        }
        return left.GetFilePath() < right.GetFilePath();
    };
    sort(_fileMetaVec.begin(), _fileMetaVec.end(), compare);
}

int32_t PackageFileMeta::GetDataFileIdx(const std::string& fileName)
{
    size_t start = fileName.find(PACKAGE_FILE_DATA_SUFFIX) + std::string(PACKAGE_FILE_DATA_SUFFIX).size();
    if (start == std::string::npos || start >= fileName.size()) {
        return -1;
    }
    int32_t dataFileIdx = -1;
    // fileName is TAGGED format, example: package_file.__data__.MERGE.123
    if (fileName.at(start) == '.') {
        start = fileName.find_first_of('.', start + 1);
        if (!autil::StringUtil::strToInt32(fileName.substr(start + 1).c_str(), dataFileIdx)) {
            return -1;
        }
        return dataFileIdx;
    }
    // fileName is NOT TAGGED format, example: package_file.__data__123
    if (!autil::StringUtil::strToInt32(fileName.substr(start).c_str(), dataFileIdx)) {
        return -1;
    }
    return dataFileIdx;
}

void PackageFileMeta::ComputePhysicalFileLengths()
{
    _physicalFileLengths.resize(_physicalFileNames.size(), 0);
    for (const auto& fileMeta : _fileMetaVec) {
        if (fileMeta.IsDir()) {
            continue;
        }
        assert(fileMeta.GetLength() >= 0);
        size_t length = fileMeta.GetOffset() + fileMeta.GetLength();
        uint32_t dataFileIdx = fileMeta.GetDataFileIdx();
        if (length > _physicalFileLengths[dataFileIdx]) {
            _physicalFileLengths[dataFileIdx] = length;
        }
        // We must make sure _physicalFileNames is ordered by dataFileIdx.
        assert(GetDataFileIdx(_physicalFileNames.at(dataFileIdx)) == fileMeta.GetDataFileIdx());
    }
}

ErrorCode PackageFileMeta::Validate() const
{
    if (unlikely(_physicalFileNames.size() != _physicalFileLengths.size() ||
                 _physicalFileNames.size() != _physicalFileTags.size())) {
        AUTIL_LOG(ERROR, "physical_file_names[%lu], physical_file_lengths[%lu], physical_file_tags[%lu]",
                  _physicalFileNames.size(), _physicalFileLengths.size(), _physicalFileTags.size());
        return FSEC_ERROR;
    }

    return FSEC_OK;
}

void PackageFileMeta::Jsonize(JsonWrapper& json)
{
    json.Jsonize("inner_files", _fileMetaVec, _fileMetaVec);
    json.Jsonize("file_align_size", _fileAlignSize, _fileAlignSize);
    json.Jsonize("physical_file_names", _physicalFileNames, _physicalFileNames);
    json.Jsonize("physical_file_lengths", _physicalFileLengths, _physicalFileLengths);
    json.Jsonize("physical_file_tags", _physicalFileTags, _physicalFileTags);
    if (json.GetMode() == FROM_JSON) {
        // for compatible
        if (_physicalFileNames.empty()) {
            uint32_t maxDataFileIdx = 0;
            bool hasFile = false;
            for (const auto& fileMeta : _fileMetaVec) {
                if (!fileMeta.IsDir()) {
                    hasFile = true;
                }
                if (fileMeta.GetDataFileIdx() > maxDataFileIdx) {
                    maxDataFileIdx = fileMeta.GetDataFileIdx();
                }
            }
            if (hasFile) {
                for (uint32_t i = 0; i < maxDataFileIdx + 1; ++i) {
                    string packageFileDataPath = GetPackageFileDataPath(PACKAGE_FILE_PREFIX, "", i);
                    AddPhysicalFile(util::PathUtil::GetFileName(packageFileDataPath), "");
                }
                ComputePhysicalFileLengths();
            }
        }
    }
}

pair<bool, bool> PackageFileMeta::IsPackageFileName(const string& fileName)
{
    static const string packageFilePrefix(PACKAGE_FILE_PREFIX);
    static const string packageFileMetaSuffix(PACKAGE_FILE_META_SUFFIX);
    static const string packageFileDataSuffix(PACKAGE_FILE_DATA_SUFFIX);

    if (fileName.compare(0, packageFilePrefix.size(), packageFilePrefix) == 0) {
        if (fileName.compare(packageFilePrefix.size(), packageFileMetaSuffix.size(), packageFileMetaSuffix) == 0) {
            return {true, true};
        } else if (fileName.compare(packageFilePrefix.size(), packageFileDataSuffix.size(), packageFileDataSuffix) ==
                   0) {
            return {true, false};
        }
    }
    return {false, false};
}

string PackageFileMeta::GetPackageFileDataPath(const string& packageFilePath, const string& description,
                                               uint32_t dataFileIdx)
{
    if (description.empty()) {
        // eg: package_file.__data__0, FOR Build
        return packageFilePath + PACKAGE_FILE_DATA_SUFFIX + StringUtil::toString(dataFileIdx);
    }
    // eg: package_file.__data__.DESCRIPTION.0, FOR Merge
    return packageFilePath + PACKAGE_FILE_DATA_SUFFIX + "." + description + "." + StringUtil::toString(dataFileIdx);
}

string PackageFileMeta::GetPackageFileMetaPath(const string& packageFilePath, const string& description,
                                               int32_t metaVersionId)
{
    if (description.empty()) {
        // this is final meta name
        // eg: package_file.__meta__, FOR Build & EndMerge
        return packageFilePath + PACKAGE_FILE_META_SUFFIX;
    }

    // eg: package_file.__meta__.DESCRIPTION.VERSION, FOR DoMerge
    return packageFilePath + PACKAGE_FILE_META_SUFFIX + "." + description + "." + StringUtil::toString(metaVersionId);
}

int32_t PackageFileMeta::GetVersionId(const string& fileName)
{
    string versionIdStr = fileName.substr(fileName.rfind(".") + 1);
    versionid_t versionId = -1;
    if (unlikely(!StringUtil::strToInt32(versionIdStr.c_str(), versionId))) {
        AUTIL_LOG(WARN, "Invalid package file meta name [%s]", fileName.c_str());
        return -1;
    }
    return versionId;
}

ErrorCode PackageFileMeta::GetPackageFileGroup(const std::string& packageFilePath, std::string* packageFileGroup)
{
    string dirPath = PathUtil::GetParentDirPath(packageFilePath);
    string fileName = PathUtil::GetFileName(packageFilePath);
    vector<string> frags = StringUtil::split(fileName, ".", false);
    if (frags.size() != 2 && frags.size() != 4) {
        AUTIL_LOG(ERROR, "invalid package file path [%s]", packageFilePath.c_str());
        return FSEC_ERROR;
    }
    if (frags[1] == PACKAGE_FILE_META_SUFFIX && frags[1] != PACKAGE_FILE_DATA_SUFFIX) {
        AUTIL_LOG(ERROR, "invalid package file path [%s]", packageFilePath.c_str());
        return FSEC_ERROR;
    }
    string prefix = frags[0];
    *packageFileGroup = PathUtil::JoinPath(dirPath, prefix);
    return FSEC_OK;
}

int32_t PackageFileMeta::GetPackageDataFileIdx(const std::string& packageFileDataPath)
{
    string suffix = PACKAGE_FILE_DATA_SUFFIX;
    size_t pos = packageFileDataPath.rfind(suffix);
    if (pos == string::npos) {
        return -1;
    }
    pos += suffix.size();
    int32_t dataFileIdx = -1;
    if (StringUtil::fromString(packageFileDataPath.substr(pos), dataFileIdx)) {
        return dataFileIdx;
    }
    return -1;
}

void PackageFileMeta::AddPhysicalFile(const string& name, const string& tag)
{
    _physicalFileNames.push_back(name);
    _physicalFileTags.push_back(tag);
}

void PackageFileMeta::AddPhysicalFiles(const vector<string>& names, const vector<string> tags)
{
    _physicalFileNames.insert(_physicalFileNames.end(), names.begin(), names.end());
    _physicalFileTags.insert(_physicalFileTags.end(), tags.begin(), tags.end());
}

ErrorCode PackageFileMeta::GetMetaStrLength(size_t* length) const
{
    string jsonStr;
    RETURN_IF_FS_ERROR(ToString(&jsonStr), "");
    *length = _metaStrLen >= 0 ? _metaStrLen : jsonStr.size();
    return FSEC_OK;
}
}} // namespace indexlib::file_system
