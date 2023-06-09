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

#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/Version.h"

namespace indexlib::file_system {
class Directory;
class IDirectory;
} // namespace indexlib::file_system

namespace indexlibv2::framework {

class VersionLoader
{
public:
    // version按version id从小到大排序
    static Status ListVersion(const indexlib::file_system::DirectoryPtr& dir, fslib::FileList* fileList);
    static Status GetVersion(const indexlib::file_system::DirectoryPtr& dir, versionid_t versionId, Version* version);
    static Status ListSegment(const indexlib::file_system::DirectoryPtr& dir, fslib::FileList* fileList);
    static Status LoadVersion(const indexlib::file_system::DirectoryPtr& dir, const std::string& versionFileName,
                              Version* version);

public:
    static std::pair<Status, bool> HasVersion(const std::shared_ptr<indexlib::file_system::IDirectory>& dir,
                                              versionid_t versionId);
    static std::pair<Status, std::unique_ptr<Version>>
    LoadVersion(const std::shared_ptr<indexlib::file_system::IDirectory>& dir,
                const std::string& versionFileName) noexcept;
    static std::pair<Status, std::unique_ptr<Version>>
    GetVersion(const std::shared_ptr<indexlib::file_system::IDirectory>& dir, versionid_t versionId) noexcept;

public:
    static versionid_t GetVersionId(const std::string& versionFileName);
    static bool IsValidVersionFileName(const std::string& fileName);

private:
    // version.${number}
    static int32_t ExtractSuffixNumber(const std::string& name, const std::string& prefix);
    static bool MatchPattern(const std::string& str, const std::string& prefix, char sep);
    static bool IsNotVersionFile(const std::string& fileName);
    static bool IsNotSegmentFile(const std::string& fileName);

    friend struct VersionFileComp;
    friend struct SegmentDirComp;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
