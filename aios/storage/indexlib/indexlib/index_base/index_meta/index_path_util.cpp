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
#include "indexlib/index_base/index_meta/index_path_util.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

using namespace indexlib::index;
namespace indexlib { namespace index_base {

bool IndexPathUtil::GetSegmentId(const std::string& path, segmentid_t& segmentId) noexcept
{
    vector<string> frags = StringUtil::split(path, "_", false);
    if (frags.size() == 4) {
        uint32_t levelId = 0;
        return frags[0] == SEGMENT_FILE_NAME_PREFIX && frags[2] == "level" &&
               StringUtil::fromString<segmentid_t>(frags[1], segmentId) &&
               StringUtil::fromString<uint32_t>(frags[3], levelId);
    } else if (frags.size() == 2) {
        return frags[0] == SEGMENT_FILE_NAME_PREFIX && StringUtil::fromString<segmentid_t>(frags[1], segmentId);
    }
    return false;
}

bool IndexPathUtil::GetVersionId(const std::string& path, versionid_t& versionId) noexcept
{
    return StringUtil::startsWith(path, VERSION_FILE_NAME_DOT_PREFIX) &&
           StringUtil::fromString<versionid_t>(path.substr(VERSION_FILE_NAME_DOT_PREFIX.length()), versionId);
}

bool IndexPathUtil::GetDeployMetaId(const std::string& path, versionid_t& versionId) noexcept
{
    string prefix = string(DEPLOY_META_FILE_NAME_PREFIX) + ".";
    return StringUtil::startsWith(path, prefix) &&
           StringUtil::fromString<versionid_t>(path.substr(prefix.length()), versionId);
}

bool IndexPathUtil::GetPatchMetaId(const std::string& path, versionid_t& versionId) noexcept
{
    string prefix = string(PARTITION_PATCH_META_FILE_PREFIX) + ".";
    return StringUtil::startsWith(path, prefix) &&
           StringUtil::fromString<versionid_t>(path.substr(prefix.length()), versionId);
}

bool IndexPathUtil::GetSchemaId(const std::string& path, schemavid_t& schemaId) noexcept
{
    if (path == SCHEMA_FILE_NAME) {
        schemaId = DEFAULT_SCHEMAID;
        return true;
    }
    string prefix = SCHEMA_FILE_NAME + ".";
    return StringUtil::startsWith(path, prefix) &&
           StringUtil::fromString<schemavid_t>(path.substr(prefix.length()), schemaId);
}

bool IndexPathUtil::GetPatchIndexId(const std::string& path, schemavid_t& schemaId) noexcept
{
    return StringUtil::startsWith(path, PATCH_INDEX_DIR_PREFIX) &&
           StringUtil::fromString<schemavid_t>(path.substr(PATCH_INDEX_DIR_PREFIX.length()), schemaId);
}

bool IndexPathUtil::IsValidSegmentName(const std::string& path) noexcept
{
    segmentid_t segId;
    return IndexPathUtil::GetSegmentId(path, segId);
}
}} // namespace indexlib::index_base
