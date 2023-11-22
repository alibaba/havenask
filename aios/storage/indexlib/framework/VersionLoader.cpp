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
#include "indexlib/framework/VersionLoader.h"

#include <algorithm>
#include <assert.h>
#include <stddef.h>
#include <vector>

#include "autil/StringUtil.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ListOption.h"
#include "indexlib/file_system/ReaderOption.h"

namespace indexlibv2::framework {

AUTIL_LOG_SETUP(indexlib.framework, VersionLoader);

Status VersionLoader::LoadVersion(const indexlib::file_system::DirectoryPtr& dir, const std::string& versionFileName,
                                  Version* version)
{
    AUTIL_LOG(DEBUG, "begin load version, name [%s], dir[%s]", versionFileName.c_str(), dir->DebugString().c_str());
    std::string content("");
    auto st = dir->GetIDirectory()
                  ->Load(versionFileName,
                         indexlib::file_system::ReaderOption::PutIntoCache(indexlib::file_system::FSOT_MEM), content)
                  .Status();
    if (st.IsNotFound()) {
        auto status = Status::InternalError("version not exist, logical dir[%s] version[%s]",
                                            dir->DebugString().c_str(), versionFileName.c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    } else if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "load version failed, logical dir[%s] version[%s] error[%s]", dir->DebugString().c_str(),
                  versionFileName.c_str(), st.ToString().c_str());
        return st;
    }

    st = version->FromString(content);
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "de-serialize version failed, logical dir[%s] version file[%s] error[%s]",
                  dir->DebugString().c_str(), versionFileName.c_str(), st.ToString().c_str());
        return st;
    }
    AUTIL_LOG(DEBUG, "end load version, name [%s]", versionFileName.c_str());
    return st;
}

int32_t VersionLoader::ExtractSuffixNumber(const std::string& name, const std::string& prefix)
{
    int32_t number = INVALID_VERSIONID;
    // format like version.${number}, prefix = version, 1 represent char '.'
    bool success = autil::StringUtil::strToInt32(name.data() + prefix.size() + 1, number);
    if (!success) {
        AUTIL_LOG(ERROR, "extract suffix number failed, name:[%s]", name.c_str());
        return number;
    }
    return number;
}

bool VersionLoader::MatchPattern(const std::string& str, const std::string& prefix, char sep)
{
    if (str.find(prefix) != 0) {
        return false;
    }
    const size_t suffixPos = prefix.length() + sizeof(sep);
    if (str.length() < suffixPos || str[prefix.size()] != sep) {
        return false;
    }
    int32_t lftVal;
    return autil::StringUtil::strToInt32(str.data() + suffixPos, lftVal);
}

bool VersionLoader::IsValidVersionFileName(const std::string& fileName)
{
    return MatchPattern(fileName, VERSION_FILE_NAME_PREFIX, '.');
}

bool VersionLoader::IsNotVersionFile(const std::string& fileName)
{
    return !VersionLoader::IsValidVersionFileName(fileName);
}

struct VersionFileComp {
    bool operator()(const std::string& lft, const std::string& rht)
    {
        int32_t lftVal = VersionLoader::ExtractSuffixNumber(lft, VERSION_FILE_NAME_PREFIX);
        int32_t rhtVal = VersionLoader::ExtractSuffixNumber(rht, VERSION_FILE_NAME_PREFIX);
        assert(lftVal != INVALID_VERSIONID);
        assert(rhtVal != INVALID_VERSIONID);
        return lftVal < rhtVal;
    }
};

struct SegmentDirComp {
    bool operator()(const std::string& lft, const std::string& rht)
    {
        segmentid_t lftVal = PathUtil::GetSegmentIdByDirName(lft);
        segmentid_t rhtVal = PathUtil::GetSegmentIdByDirName(rht);
        assert(lftVal != INVALID_SEGMENTID);
        assert(rhtVal != INVALID_SEGMENTID);
        return lftVal < rhtVal;
    }
};

bool VersionLoader::IsNotSegmentFile(const std::string& fileName) { return !PathUtil::IsValidSegmentDirName(fileName); }

Status VersionLoader::ListVersion(const indexlib::file_system::DirectoryPtr& dir, fslib::FileList* fileList)
{
    Status st = dir->GetIDirectory()
                    ->ListDir(/*path*/ "", indexlib::file_system::ListOption::Recursive(false), *fileList)
                    .Status();
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "list dir failed, logical dir[%s] error[%s]", dir->GetOutputPath().c_str(),
                  st.ToString().c_str());
        return st;
    }

    fslib::FileList::iterator it = std::remove_if(fileList->begin(), fileList->end(), IsNotVersionFile);
    fileList->erase(it, fileList->end());
    std::sort(fileList->begin(), fileList->end(), VersionFileComp());
    return Status::OK();
}

Status VersionLoader::ListSegment(const indexlib::file_system::DirectoryPtr& dir, fslib::FileList* fileList)
{
    Status st = dir->GetIDirectory()
                    ->ListDir(/*path*/ "", indexlib::file_system::ListOption::Recursive(false), *fileList)
                    .Status();
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "list dir failed, logical dir[%s] error[%s]", dir->GetOutputPath().c_str(),
                  st.ToString().c_str());
        return st;
    }

    fslib::FileList::iterator it = std::remove_if(fileList->begin(), fileList->end(), IsNotSegmentFile);
    fileList->erase(it, fileList->end());
    std::sort(fileList->begin(), fileList->end(), SegmentDirComp());
    return Status::OK();
}

Status VersionLoader::GetVersion(const indexlib::file_system::DirectoryPtr& dir, versionid_t versionId,
                                 Version* version)
{
    if (versionId == INVALID_VERSIONID) {
        fslib::FileList fileList;
        Status st = ListVersion(dir, &fileList);
        if (!st.IsOK()) {
            AUTIL_LOG(ERROR, "list version failed, dir[%s], error[%s]", dir->GetOutputPath().c_str(),
                      st.ToString().c_str());
            return st;
        }
        if (fileList.size() != 0) {
            std::string latestVersionName = fileList[fileList.size() - 1];
            st = LoadVersion(dir, latestVersionName, version);
            if (!st.IsOK()) {
                AUTIL_LOG(ERROR, "load version failed, logical dir[%s] error[%s]", dir->GetOutputPath().c_str(),
                          st.ToString().c_str());
                return st;
            }
            return Status::OK();
        }
        // TODO(xiuchen) return ok is better ?
        // return Status::InternalError("version not exist, dir", dir->GetOutputPath());
        return Status::OK();
    }
    std::string versionFileName = PathUtil::GetVersionFileName(versionId);
    Status st = LoadVersion(dir, versionFileName, version);
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "load version failed, logical dir[%s] version file[%s] error[%s]",
                  dir->GetOutputPath().c_str(), versionFileName.c_str(), st.ToString().c_str());
        return st;
    }
    return Status::OK();
}

versionid_t VersionLoader::GetVersionId(const std::string& versionFileName)
{
    return (versionid_t)ExtractSuffixNumber(versionFileName, VERSION_FILE_NAME_PREFIX);
}

std::pair<Status, bool> VersionLoader::HasVersion(const std::shared_ptr<indexlib::file_system::IDirectory>& dir,
                                                  versionid_t versionId)
{
    std::string versionFileName = PathUtil::GetVersionFileName(versionId);
    return dir->IsExist(versionFileName).StatusWith();
}

std::pair<Status, std::unique_ptr<Version>>
VersionLoader::LoadVersion(const std::shared_ptr<indexlib::file_system::IDirectory>& dir,
                           const std::string& versionFileName) noexcept
{
    AUTIL_LOG(INFO, "begin load version, name [%s], dir[%s]", versionFileName.c_str(), dir->DebugString().c_str());
    std::string content;
    auto readOption = indexlib::file_system::ReaderOption::PutIntoCache(indexlib::file_system::FSOT_MEM);
    auto st = dir->Load(versionFileName, readOption, content).Status();
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "load version failed, logical dir[%s] version[%s] error[%s]", dir->DebugString().c_str(),
                  versionFileName.c_str(), st.ToString().c_str());
        return {st, nullptr};
    }
    auto version = std::make_unique<Version>();
    st = version->FromString(content);
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "deserialize version failed, logical dir[%s] version file[%s] error[%s]",
                  dir->DebugString().c_str(), versionFileName.c_str(), st.ToString().c_str());
        return {st, nullptr};
    }
    AUTIL_LOG(DEBUG, "end load version, name [%s]", versionFileName.c_str());
    return {st, std::move(version)};
}

std::pair<Status, std::unique_ptr<Version>>
VersionLoader::GetVersion(const std::shared_ptr<indexlib::file_system::IDirectory>& dir, versionid_t versionId) noexcept
{
    if (versionId == INVALID_VERSIONID) {
        return {Status::InvalidArgs("not support INVALID_VERSIONID"), nullptr};
    }
    std::string versionFileName = PathUtil::GetVersionFileName(versionId);
    auto [st, version] = LoadVersion(dir, versionFileName);
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "load version failed, logical dir[%s] version file[%s] error[%s]",
                  dir->GetOutputPath().c_str(), versionFileName.c_str(), st.ToString().c_str());
    }
    return {st, std::move(version)};
}

} // namespace indexlibv2::framework
