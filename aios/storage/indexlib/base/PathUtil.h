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

#include <string>
#include <string_view>

#include "indexlib/base/Types.h"

namespace indexlibv2 {
class PathUtil
{
public:
    static std::string GetParentDirPath(const std::string& path) noexcept;
    static std::string JoinPath(const std::string& path, const std::string& name);
    static std::string JoinPath(const std::string& basePath, const std::string& path, const std::string& name);
    static std::string GetEntryTableFileName(versionid_t version);
    static std::string GetDeployMetaFileName(versionid_t version);
    static bool GetVersionId(const std::string& path, versionid_t* versionId) noexcept;
    static segmentid_t GetSegmentIdByDirName(const std::string& path);
    static std::string NewSegmentDirName(segmentid_t segmentId, size_t levelIdx);
    static uint32_t GetLevelIdxFromSegmentDirName(const std::string& segDirName);
    static std::string GetVersionFileName(versionid_t versionId);
    static bool IsValidSegmentDirName(const std::string& segDirName);
    static bool ExtractSchemaIdBySchemaFile(const std::string& schemaFileName, schemaid_t& schemaId);
    static std::string GetShardDirName(size_t shardIdx);
    static std::string GetDirName(const std::string& path);
    static std::string GetFirstLevelDirName(const std::string& logicalPath);
    static std::string GetFileNameFromPath(const std::string& path);
    static void TrimLastDelim(std::string& dirPath);
    // path="{rootPath}/__FENCE__xxx" --> {rootPath, "__FENCE__xxx"}
    // path="{rootPath}"              --> {rootPath, ""}
    static std::pair<std::string, std::string> ExtractFenceName(std::string_view path);
    static std::string GetTaskTempWorkRoot(const std::string& indexRoot, const std::string& taskEpochId);
    // patch_index_schemaid
    static std::string GetPatchIndexDirName(schemaid_t schemaId);
    static bool IsPatchIndexDirName(const std::string& dirName);
};

} // namespace indexlibv2
