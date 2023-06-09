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
#include "indexlib/index_base/index_meta/index_format_version.h"

#include "autil/StringUtil.h"
#include "indexlib/config/index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

using namespace indexlib::file_system;

using FSEC = indexlib::file_system::ErrorCode;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, IndexFormatVersion);

IndexFormatVersion::IndexFormatVersion(const string& version)
    : mIndexFormatVersion(version)
    , mInvertedIndexFormatVersion(config::IndexConfig::BINARY_FORMAT_VERSION)
{
}

IndexFormatVersion::~IndexFormatVersion() {}

bool IndexFormatVersion::Load(const DirectoryPtr& directory, bool mayNonExist)
{
    string content;
    if (!directory->LoadMayNonExist(INDEX_FORMAT_VERSION_FILE_NAME, content, mayNonExist)) {
        return false;
    }
    *this = LoadFromString(content);
    return true;
}

bool IndexFormatVersion::Load(const string& path, bool mayNonExist)
{
    string content;
    auto ec = FslibWrapper::Load(path, content).Code();
    if (ec == FSEC_OK) {
        *this = LoadFromString(content);
        return true;
    }
    if (ec == FSEC_NOENT && mayNonExist) {
        return false;
    }
    THROW_IF_FS_ERROR(ec, "load IndexFormatVersion failed, path [%s], mayNonExist [%d]", path.c_str(), mayNonExist);
    return false;
}

IndexFormatVersion IndexFormatVersion::LoadFromString(const string& content)
{
    IndexFormatVersion formatVersion;
    try {
        FromJsonString(formatVersion, content);
    } catch (const json::JsonParserError& e) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "Parse index format version file FAILED,"
                             "content: %s",
                             content.c_str());
    }
    return formatVersion;
}

void IndexFormatVersion::Store(const string& path, FenceContext* fenceContext)
{
    string jsonStr = ToJsonString(*this);
    IE_LOG(DEBUG, "Store index format file: [%s]", jsonStr.c_str());
    auto ec = FslibWrapper::AtomicStore(path, jsonStr, false, fenceContext).Code();
    THROW_IF_FS_ERROR(ec, "atomic store failed, path [%s]", path.c_str());
}

void IndexFormatVersion::Store(const DirectoryPtr& directory)
{
    string jsonStr = ToJsonString(*this);
    directory->Store(INDEX_FORMAT_VERSION_FILE_NAME, jsonStr, WriterOption::AtomicDump());
}

void IndexFormatVersion::StoreBinaryVersion(const file_system::DirectoryPtr& rootDirectory)
{
    IndexFormatVersion binaryVersion(INDEX_FORMAT_VERSION);
    binaryVersion.Store(rootDirectory);
}

bool IndexFormatVersion::IsLegacyFormat() const
{
    // should checkCompatible first!
    vector<int32_t> binaryVersionIds;
    vector<int32_t> currentVersionIds;

    SplitVersionIds(INDEX_FORMAT_VERSION, binaryVersionIds);
    SplitVersionIds(mIndexFormatVersion, currentVersionIds);
    return binaryVersionIds[1] > currentVersionIds[1];
}

void IndexFormatVersion::SplitVersionIds(const string& versionStr, vector<int32_t>& versionIds) const
{
    vector<string> strVec = StringUtil::split(versionStr, ".", true);
    if (strVec.size() != 3) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Index Format Version [%s] illegal.", versionStr.c_str());
    }
    versionIds.resize(3);

    for (size_t i = 0; i < strVec.size(); i++) {
        if (!StringUtil::strToInt32(strVec[i].c_str(), versionIds[i])) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "Index Format Version [%s] illegal.", versionStr.c_str());
        }
    }
}

void IndexFormatVersion::CheckCompatible(const string& binaryVersion)
{
    vector<int32_t> binaryVersionIds;
    vector<int32_t> currentVersionIds;

    SplitVersionIds(binaryVersion, binaryVersionIds);
    SplitVersionIds(mIndexFormatVersion, currentVersionIds);

    if (binaryVersionIds[0] == currentVersionIds[0] &&
        (binaryVersionIds[1] == currentVersionIds[1] || binaryVersionIds[1] - currentVersionIds[1] == 1)) {
        return;
    }

    INDEXLIB_FATAL_ERROR(IndexCollapsed,
                         "Index Format Version is incompatible with binary version. "
                         "Current version [%s], Binary version [%s]. "
                         "The first digits of both versions should be identical. "
                         "The second digit of index version should be equal to"
                         " or one less than that of current version",
                         binaryVersion.c_str(), mIndexFormatVersion.c_str());
}

void IndexFormatVersion::Jsonize(JsonWrapper& json)
{
    json.Jsonize("index_format_version", mIndexFormatVersion);
    format_versionid_t defaultVersionId = 0;
    json.Jsonize("inverted_index_binary_format_version", mInvertedIndexFormatVersion, defaultVersionId);
}

void IndexFormatVersion::Set(const string& version) { mIndexFormatVersion = version; }

bool IndexFormatVersion::operator==(const IndexFormatVersion& other) const
{
    return mIndexFormatVersion == other.mIndexFormatVersion;
}

bool IndexFormatVersion::operator!=(const IndexFormatVersion& other) const { return !(*this == other); }

bool IndexFormatVersion::operator<(const IndexFormatVersion& other) const
{
    vector<int32_t> myVersionIds;
    vector<int32_t> otherVersionIds;

    SplitVersionIds(mIndexFormatVersion, myVersionIds);
    SplitVersionIds(other.mIndexFormatVersion, otherVersionIds);

    assert(myVersionIds.size() == otherVersionIds.size());

    size_t i = 0;
    while (i < myVersionIds.size() && myVersionIds[i] == otherVersionIds[i]) {
        i++;
    }
    if (i < myVersionIds.size()) {
        return myVersionIds[i] < otherVersionIds[i];
    }
    return false;
}
}} // namespace indexlib::index_base
