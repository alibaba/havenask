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
#include "indexlib/file_system/package/VersionedPackageFileMeta.h"

#include <assert.h>
#include <cstddef>
#include <set>
#include <stdint.h>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/package/PackageFileMeta.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, VersionedPackageFileMeta);

VersionedPackageFileMeta::VersionedPackageFileMeta(versionid_t versionId) : PackageFileMeta(), _versionId(versionId) {}

VersionedPackageFileMeta::~VersionedPackageFileMeta() {}

void VersionedPackageFileMeta::Jsonize(Jsonizable::JsonWrapper& json) { PackageFileMeta::Jsonize(json); }

FSResult<size_t> VersionedPackageFileMeta::Store(const string& root, const string& description,
                                                 FenceContext* fenceContext)
{
    SortInnerFileMetas();
    ComputePhysicalFileLengths();
    const string& fileName = VersionedPackageFileMeta::GetPackageMetaFileName(description, _versionId);
    string metaFilePath = PathUtil::JoinPath(root, fileName);
    AUTIL_LOG(INFO, "store meta file[%s]", metaFilePath.c_str());
    string metaContent;
    ErrorCode ec = ToString(&metaContent);
    if (unlikely(ec != FSEC_OK)) {
        AUTIL_LOG(ERROR, "jsonize string failed, ec[%d] path[%s]", ec, metaFilePath.c_str());
        return {ec, (size_t)-1};
    }
    ec = FslibWrapper::AtomicStore(metaFilePath, metaContent.c_str(), metaContent.size(), true, fenceContext).Code();
    if (unlikely(ec != FSEC_OK)) {
        AUTIL_LOG(ERROR, "atomic store failed, ec[%d] path[%s]", ec, metaFilePath.c_str());
        return {ec, (size_t)-1};
    }
    return {FSEC_OK, metaContent.size()};
}

FSResult<int64_t> VersionedPackageFileMeta::Load(const string& metaFilePath)
{
    AUTIL_LOG(INFO, "load meta file[%s]", metaFilePath.c_str());
    FSResult<int64_t> ret = JsonUtil::Load(metaFilePath, this);
    if (ret.ec != FSEC_OK) {
        AUTIL_LOG(INFO, "load meta file[%s]", metaFilePath.c_str());
        return {ret.ec, -1};
    }
    _versionId = VersionedPackageFileMeta::GetVersionId(PathUtil::GetFileName(metaFilePath));
    return {FSEC_OK, ret.result};
}

void VersionedPackageFileMeta::Recognize(const string& description, int32_t recoverMetaId,
                                         const vector<string>& fileNames, set<string>& dataFileSet,
                                         set<string>& uselessMetaFileSet, string& recoverMetaPath) noexcept
{
    assert(dataFileSet.empty() && uselessMetaFileSet.empty());
    string dataPrefix = string(PACKAGE_FILE_PREFIX) + PACKAGE_FILE_DATA_SUFFIX + "." + description + ".";
    string metaPrefix = string(PACKAGE_FILE_PREFIX) + PACKAGE_FILE_META_SUFFIX + "." + description + ".";

    int32_t maxMetaId = -1;
    string maxMetaPath = "";
    recoverMetaPath = "";
    for (const string& fileName : fileNames) {
        if (StringUtil::startsWith(fileName, dataPrefix)) {
            dataFileSet.insert(fileName);
        } else if (StringUtil::startsWith(fileName, metaPrefix)) {
            uselessMetaFileSet.insert(fileName);
            int32_t metaId = GetVersionId(fileName);
            if (metaId < 0) {
                continue;
            }
            if (recoverMetaId == metaId) {
                recoverMetaPath = fileName;
            }
            if (metaId > maxMetaId) {
                maxMetaId = metaId;
                maxMetaPath = fileName;
            }
        } else {
            AUTIL_LOG(DEBUG, "file [%s] can not match dataPrefix [%s] or metaPrefix [%s]", fileName.c_str(),
                      dataPrefix.c_str(), metaPrefix.c_str());
        }
    }
    if (recoverMetaId < 0) {
        recoverMetaPath = maxMetaPath;
    }
    uselessMetaFileSet.erase(recoverMetaPath);
}

int32_t VersionedPackageFileMeta::GetVersionId(const string& fileName)
{
    string versionIdStr = fileName.substr(fileName.rfind(".") + 1);
    versionid_t versionId = -1;
    if (unlikely(!StringUtil::strToInt32(versionIdStr.c_str(), versionId))) {
        AUTIL_LOG(WARN, "Invalid package file meta name [%s]", fileName.c_str());
        return -1;
    }
    return versionId;
}

bool VersionedPackageFileMeta::IsValidFileName(const std::string& fileName)
{
    static uint32_t tmp = 0;
    std::vector<std::string> vec = StringUtil::split(fileName, ".");
    return vec.size() == 4 && vec[0] == PACKAGE_FILE_PREFIX && vec[1] == PACKAGE_FILE_META_SUFFIX_WITHOUT_DOT &&
           StringUtil::fromString(vec[3], tmp);
}

string VersionedPackageFileMeta::GetDescription(const string& fileName)
{
    size_t endPos = fileName.rfind(".");
    size_t startPos = fileName.rfind(".", endPos - 1) + 1;
    return fileName.substr(startPos, endPos - startPos);
}

string VersionedPackageFileMeta::GetPackageDataFileName(const string& description, uint32_t dataFileIdx)
{
    return PackageFileMeta::GetPackageFileDataPath(PACKAGE_FILE_PREFIX, description, dataFileIdx);
}

string VersionedPackageFileMeta::GetPackageMetaFileName(const string& description, int32_t metaVersionId)
{
    return PackageFileMeta::GetPackageFileMetaPath(PACKAGE_FILE_PREFIX, description, metaVersionId);
}
}} // namespace indexlib::file_system
