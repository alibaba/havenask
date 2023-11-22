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

#include <memory>

#include "fslib/fslib.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, Version);

namespace indexlib { namespace index_base {

class VersionLoader
{
public:
    VersionLoader();
    ~VersionLoader();

public:
    // Function with Suffix Of S will be abandoned in the future, can't be used in new code.
    static void ListVersionS(const std::string& dirPath, fslib::FileList& fileList);
    static void GetVersionS(const std::string& dirPath, Version& version, versionid_t versionId);
    static void GetVersion(const file_system::DirectoryPtr& directory, Version& version, versionid_t versionId);
    static void ListVersion(const file_system::DirectoryPtr& directory, fslib::FileList& fileList);

    static void ListSegmentsS(const std::string& rootDir, fslib::FileList& fileList);

    static void ListSegments(const file_system::DirectoryPtr& directory, fslib::FileList& fileList);

    static versionid_t GetVersionId(const std::string& versionFileName);

    static bool IsValidVersionFileName(const std::string& fileName);

    static bool IsValidSegmentFileName(const std::string& fileName);

private:
    static int32_t extractSuffixNumber(const std::string& name, const std::string& prefix);

    static bool MatchPattern(const std::string& fileName, const std::string& prefix, char sep);

    friend struct VersionFileComp;
    friend struct SegmentDirComp;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VersionLoader);
}} // namespace indexlib::index_base
