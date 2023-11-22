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
#include <utility>
#include <vector>

#include "autil/Log.h"

namespace indexlib { namespace util {

class PathUtil
{
private:
    static const std::string CONFIG_SEPARATOR;

public:
    static std::string GetParentDirPath(const std::string& path) noexcept;
    static std::string GetParentDirName(const std::string& path) noexcept;
    static std::string GetFileName(const std::string& path) noexcept;
    static std::string GetDirName(const std::string& path) noexcept;
    static std::string JoinPath(const std::string& path, const std::string& name) noexcept;

    template <typename T>
    static std::string JoinPath(const std::string& path, const T& name) noexcept
    {
        return JoinPath(path, std::string(name));
    }

    template <typename... Args>
    static std::string JoinPath(const std::string& path, Args... args) noexcept
    {
        return JoinPath(path, JoinPath(std::forward<Args>(args)...));
    }
    static std::string NormalizePath(const std::string& path) noexcept;
    static std::string NormalizeDir(const std::string& dir) noexcept;
    static void TrimLastDelim(std::string& dirPath) noexcept;

    static bool IsInRootPath(const std::string& normPath, const std::string& normRootPath) noexcept;
    static bool GetRelativePath(const std::string& parentPath, const std::string& path,
                                std::string& relativePath) noexcept;

    static std::string GetRelativePath(const std::string& absPath) noexcept;

    static bool IsInDfs(const std::string& path) noexcept;

    static bool SupportMmap(const std::string& path) noexcept;
    static bool SupportPanguInlineFile(const std::string& path) noexcept;

    static std::string AddFsConfig(const std::string& srcPath, const std::string& configStr) noexcept;
    static std::string SetFsConfig(const std::string& srcPath, const std::string& configStr) noexcept;

    static std::string GetBinaryPath() noexcept;

    static std::vector<std::string> GetPossibleParentPaths(const std::string& path, bool includeSelf);
    static std::string GetFilePathUnderParam(const std::string& path, const std::string& param);
    static std::string GetDirectoryPath(const std::string& path);

public:
    static const char* TEMP_SUFFIX;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlib::util
