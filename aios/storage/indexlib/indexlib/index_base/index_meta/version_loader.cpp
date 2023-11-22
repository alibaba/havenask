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
#include "indexlib/index_base/index_meta/version_loader.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_define.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace fslib;
using namespace autil;

using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, VersionLoader);

bool VersionLoader::MatchPattern(const string& fileName, const string& prefix, char sep)
{
    if (fileName.find(prefix) != 0) {
        return false;
    }
    size_t prefixLen = prefix.length() + 1;
    if (fileName.length() < prefixLen || fileName[prefixLen - 1] != sep) {
        return false;
    }
    string lftVerSufix = fileName.substr(prefixLen);
    int32_t lftVal;
    return StringUtil::strToInt32(lftVerSufix.c_str(), lftVal);
}

bool VersionLoader::IsValidVersionFileName(const string& fileName)
{
    return MatchPattern(fileName, VERSION_FILE_NAME_PREFIX, '.');
}

bool VersionLoader::IsValidSegmentFileName(const string& fileName)
{
    return (Version::IsValidSegmentDirName(fileName, false) || Version::IsValidSegmentDirName(fileName, true));
}

static bool IsNotVersionFile(const string& fileName) { return !VersionLoader::IsValidVersionFileName(fileName); }

static bool IsNotSegmentFile(const string& fileName) { return !VersionLoader::IsValidSegmentFileName(fileName); }

struct VersionFileComp {
    bool operator()(const string& lft, const string& rht)
    {
        int32_t lftVal = VersionLoader::extractSuffixNumber(lft, VERSION_FILE_NAME_PREFIX);
        int32_t rhtVal = VersionLoader::extractSuffixNumber(rht, VERSION_FILE_NAME_PREFIX);
        return lftVal < rhtVal;
    }
};

struct SegmentDirComp {
    bool operator()(const string& lft, const string& rht)
    {
        int32_t lftVal = Version::GetSegmentIdByDirName(lft);
        int32_t rhtVal = Version::GetSegmentIdByDirName(rht);
        return lftVal < rhtVal;
    }
};

VersionLoader::VersionLoader() {}

VersionLoader::~VersionLoader() {}

void VersionLoader::ListVersionS(const string& dirPath, FileList& fileList)
{
    auto ec = FslibWrapper::ListDir(PathUtil::NormalizeDir(dirPath), fileList).Code();
    THROW_IF_FS_ERROR(ec, "list version in [%s] failed", dirPath.c_str());

    FileList::iterator it = remove_if(fileList.begin(), fileList.end(), IsNotVersionFile);
    fileList.erase(it, fileList.end());
    sort(fileList.begin(), fileList.end(), VersionFileComp());
}

void VersionLoader::ListVersion(const DirectoryPtr& directory, fslib::FileList& fileList)
{
    directory->ListDir("", fileList, false);
    FileList::iterator it = remove_if(fileList.begin(), fileList.end(), IsNotVersionFile);

    fileList.erase(it, fileList.end());

    sort(fileList.begin(), fileList.end(), VersionFileComp());
}

void VersionLoader::GetVersionS(const string& dirPath, Version& version, versionid_t versionId)
{
    string path = "";
    if (versionId == INVALID_VERSIONID) {
        fslib::FileList fileList;
        ListVersionS(dirPath, fileList);
        if (fileList.size() == 0) {
            return;
        }
        path = PathUtil::JoinPath(dirPath, fileList[fileList.size() - 1]);
    } else {
        path = PathUtil::JoinPath(dirPath, Version::GetVersionFileName(versionId));
    }

    string content;
    auto ec = FslibWrapper::AtomicLoad(path, content).Code();
    THROW_IF_FS_ERROR(ec, "load version file [%s] failed", path.c_str());
    version.FromString(content);
}

void VersionLoader::GetVersion(const DirectoryPtr& directory, Version& version, versionid_t versionId)
{
    if (versionId == INVALID_VERSIONID) {
        fslib::FileList fileList;
        ListVersion(directory, fileList);
        if (fileList.size() != 0) {
            string versionPath = fileList[fileList.size() - 1];
            version.Load(directory, versionPath);
        }
        return;
    }
    string versionFileName = Version::GetVersionFileName(versionId);
    if (!version.Load(directory, versionFileName)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "expected version file[%s/%s] lost", directory->DebugString().c_str(),
                             versionFileName.c_str());
    }
}

void VersionLoader::ListSegments(const file_system::DirectoryPtr& directory, fslib::FileList& fileList)
{
    directory->ListDir("", fileList, false);
    FileList::iterator it = remove_if(fileList.begin(), fileList.end(), IsNotSegmentFile);
    fileList.erase(it, fileList.end());

    sort(fileList.begin(), fileList.end(), SegmentDirComp());
}

void VersionLoader::ListSegmentsS(const string& rootDir, fslib::FileList& fileList)
{
    auto ec = FslibWrapper::ListDir(PathUtil::NormalizeDir(rootDir), fileList).Code();
    THROW_IF_FS_ERROR(ec, "list version in [%s] failed", rootDir.c_str());

    FileList::iterator it = remove_if(fileList.begin(), fileList.end(), IsNotSegmentFile);
    fileList.erase(it, fileList.end());

    sort(fileList.begin(), fileList.end(), SegmentDirComp());
}

versionid_t VersionLoader::GetVersionId(const string& versionFileName)
{
    return (versionid_t)extractSuffixNumber(versionFileName, VERSION_FILE_NAME_PREFIX);
}

int32_t VersionLoader::extractSuffixNumber(const string& name, const string& prefix)
{
    string tmpName = name;
    int32_t prefixLen = prefix.length() + 1;
    string suffix = tmpName.substr(prefixLen);
    int32_t retVal = -1;
    bool ret = StringUtil::strToInt32(suffix.c_str(), retVal);
    if (!ret) {
        INDEXLIB_FATAL_ERROR(InvalidVersion, "Invalid pattern name: [%s]", tmpName.c_str());
    }
    return retVal;
}
}} // namespace indexlib::index_base
