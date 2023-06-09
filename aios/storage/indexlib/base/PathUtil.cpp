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
#include "indexlib/base/PathUtil.h"

#include <assert.h>
#include <regex.h>

#include "autil/Regex.h"
#include "autil/StringUtil.h"
#include "indexlib/base/Constant.h"

namespace indexlibv2 {

std::string PathUtil::GetShardDirName(size_t shardIdx)
{
    return std::string("shard_") + autil::StringUtil::toString<uint32_t>(shardIdx);
}

std::string PathUtil::JoinPath(const std::string& path, const std::string& name)
{
    if (path.empty()) {
        return name;
    }
    if (name.empty()) {
        return path;
    }

    if (*(path.rbegin()) != '/') {
        return path + "/" + name;
    }

    return path + name;
}

std::string PathUtil::JoinPath(const std::string& basePath, const std::string& path, const std::string& name)
{
    std::string p = JoinPath(basePath, path);
    return JoinPath(p, name);
}

std::string PathUtil::GetEntryTableFileName(versionid_t versionId)
{
    return ENTRY_TABLE_FILE_NAME_PREFIX + std::string(".") + std::to_string(versionId);
}

std::string PathUtil::GetDeployMetaFileName(versionid_t versionId)
{
    return DEPLOY_META_FILE_NAME_PREFIX + std::string(".") + std::to_string(versionId);
}

std::string PathUtil::GetVersionFileName(versionid_t versionId)
{
    std::stringstream ss;
    ss << VERSION_FILE_NAME_PREFIX << "." << versionId;
    return ss.str();
}

bool PathUtil::GetVersionId(const std::string& path, versionid_t* versionId) noexcept
{
    std::string prefix = std::string(VERSION_FILE_NAME_PREFIX) + ".";
    return autil::StringUtil::startsWith(path, prefix) &&
           autil::StringUtil::fromString<versionid_t>(path.substr(prefix.length()), *versionId);
}

segmentid_t PathUtil::GetSegmentIdByDirName(const std::string& segDirName)
{
    std::string prefix = std::string(SEGMENT_FILE_NAME_PREFIX) + "_";
    if (segDirName.size() <= prefix.size() || segDirName.compare(0, prefix.size(), prefix) != 0) {
        return INVALID_SEGMENTID;
    }
    return autil::StringUtil::fromString<segmentid_t>(segDirName.data() + prefix.size());
}

uint32_t PathUtil::GetLevelIdxFromSegmentDirName(const std::string& segDirName)
{
    std::vector<std::string> strs;
    autil::StringUtil::fromString(segDirName, strs, "_");
    assert(strs.size() == 4);
    return autil::StringUtil::fromString<uint32_t>(strs[3]);
}

std::string PathUtil::NewSegmentDirName(segmentid_t segmentId, size_t levelIdx)
{
    return std::string(SEGMENT_FILE_NAME_PREFIX) + "_" + std::to_string(segmentId) + "_level_" +
           autil::StringUtil::toString(levelIdx);
}

bool PathUtil::IsValidSegmentDirName(const std::string& segDirName)
{
    std::string pattern = std::string("^") + SEGMENT_FILE_NAME_PREFIX + "_[0-9]+_level_[0-9]+$";
    return autil::Regex::match(segDirName, pattern, REG_EXTENDED | REG_NOSUB);
}

bool PathUtil::ExtractSchemaIdBySchemaFile(const std::string& schemaFileName, schemaid_t& schemaId)
{
    if (schemaFileName == SCHEMA_FILE_NAME) {
        schemaId = DEFAULT_SCHEMAID;
        return true;
    }

    std::string prefixStr = SCHEMA_FILE_NAME + std::string(".");
    if (schemaFileName.find(prefixStr) != 0) {
        return false;
    }
    std::string idStr = schemaFileName.substr(prefixStr.length());
    return autil::StringUtil::fromString(idStr, schemaId);
}

std::string PathUtil::GetDirName(const std::string& path)
{
    std::string dirPath = path;
    TrimLastDelim(dirPath);
    auto pos = dirPath.find_last_of('/');
    if (pos == std::string::npos) {
        return "";
    }
    return dirPath.substr(0, pos);
}

std::string PathUtil::GetParentDirPath(const std::string& path) noexcept
{
    std::string::const_reverse_iterator it;
    for (it = path.rbegin(); it != path.rend() && *it == '/'; it++)
        ;
    for (; it != path.rend() && *it != '/'; it++)
        ;
    for (; it != path.rend() && *it == '/'; it++)
        ;
    return path.substr(0, path.rend() - it);
}

std::string PathUtil::GetFirstLevelDirName(const std::string& logicalPath)
{
    std::string logicalPathCopy = logicalPath;
    TrimLastDelim(logicalPathCopy);
    if (logicalPathCopy.empty() or logicalPathCopy.front() == '/') {
        return "";
    }
    if (logicalPathCopy.find("://") != std::string::npos) {
        return "";
    }
    auto pos = logicalPathCopy.find_first_of('/');
    if (pos == std::string::npos) {
        return "";
    }
    return logicalPathCopy.substr(0, pos);
}

// Return file name from a full path
std::string PathUtil::GetFileNameFromPath(const std::string& path)
{
    if (path.empty()) {
        return "";
    }
    std::string dirPath = path;
    TrimLastDelim(dirPath);
    return dirPath.substr(dirPath.find_last_of('/') + 1);
}

void PathUtil::TrimLastDelim(std::string& dirPath)
{
    if (dirPath.empty()) {
        return;
    }
    if (*(dirPath.rbegin()) == '/') {
        dirPath.erase(dirPath.size() - 1, 1);
    }
}

std::pair<std::string, std::string> PathUtil::ExtractFenceName(std::string_view dirPath)
{
    assert(*dirPath.rbegin() != '/');
    auto pos = dirPath.find_last_of('/');
    auto parrentPath = dirPath.substr(0, pos);
    auto dirName = dirPath.substr(pos + 1);
    const std::string_view FENCE(FENCE_DIR_NAME_PREFIX);
    if (dirName.compare(0, FENCE.size(), FENCE) == 0) {
        // startsWith("__FENCE__")
        return {std::string(parrentPath), std::string(dirName)};
    }
    return {std::string(dirPath), std::string()};
}

std::string PathUtil::GetTaskTempWorkRoot(const std::string& indexRoot, const std::string& taskEpochId)
{
    assert(!indexRoot.empty());
    assert(!taskEpochId.empty());
    std::string dirName = std::string("__TASK__/task_") + taskEpochId;
    return JoinPath(indexRoot, dirName);
}

std::string PathUtil::GetPatchIndexDirName(schemaid_t schemaId)
{
    return std::string("patch_index_") + autil::StringUtil::toString(schemaId);
}

bool PathUtil::IsPatchIndexDirName(const std::string& dirName)
{
    auto pos = dirName.find("patch_index_");
    return pos == 0;
}

} // namespace indexlibv2
