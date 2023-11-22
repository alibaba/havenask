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
#include <unordered_map>
#include <vector>

#include "autil/legacy/json.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {
class ConfigParser
{
public:
    enum ParseResult { PR_OK, PR_FAIL, PR_SECTION_NOT_EXIST };

private:
    ConfigParser(const std::string& basePath, const std::unordered_map<std::string, std::string>& fileMap =
                                                  std::unordered_map<std::string, std::string>());
    ~ConfigParser();

private:
    ConfigParser(const ConfigParser&);
    ConfigParser& operator=(const ConfigParser&);

private:
    bool parse(const std::string& path, autil::legacy::json::JsonMap& jsonMap, bool isRelativePath = true) const
    {
        if (isRelativePath) {
            return parseRelativePath(path, jsonMap, /*isNotExist=*/nullptr);
        }
        return parseExternalFile(path, jsonMap);
    }

    bool parseRelativePath(const std::string& relativePath, autil::legacy::json::JsonMap& jsonMap,
                           bool* isNotExist) const;

    bool parseExternalFile(const std::string& file, autil::legacy::json::JsonMap& jsonMap) const;

private:
    bool merge(autil::legacy::json::JsonMap& jsonMap, bool isExternal) const;
    bool mergeJsonMap(const autil::legacy::json::JsonMap& src, autil::legacy::json::JsonMap& dst,
                      bool isExternal) const;
    bool mergeJsonArray(autil::legacy::json::JsonArray& dst, bool isExternal) const;
    bool mergeWithInherit(autil::legacy::json::JsonMap& jsonMap, bool isExternal) const;

private:
    std::string _basePath;
    std::unordered_map<std::string, std::string> _fileMap;
    mutable std::vector<std::string> _configFileNames; // for inherit loop detection
private:
    friend class ResourceReader;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::config
