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

#include "autil/Log.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "swift/common/Common.h"

namespace swift {
namespace config {

class ResourceReader {
private:
    enum ErrorCode {
        JSON_PATH_OK,
        JSON_PATH_NOT_EXIST,
        JSON_PATH_INVALID,
    };

public:
    ResourceReader(const std::string &configRoot);
    ~ResourceReader();

public:
    bool getConfigContent(const std::string &relativePath, std::string &content) const;

    template <typename T>
    bool getConfigWithJsonPath(const std::string &relativePath, const std::string &jsonPath, T &config) const;

    bool getFileContent(std::string &result, const std::string &fileName) const;
    std::string getConfigPath() const;

public:
    // tools
    static ErrorCode getJsonNode(const std::string &jsonString, const std::string &jsonPath, autil::legacy::Any &any);

private:
    std::string _configRoot;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(ResourceReader);
///////////////////////////////////////////////////////
template <typename T>
bool ResourceReader::getConfigWithJsonPath(const std::string &relativePath,
                                           const std::string &jsonPath,
                                           T &config) const {
    std::string jsonStr;
    if (!getConfigContent(relativePath, jsonStr)) {
        return false;
    }
    autil::legacy::Any any;
    try {
        ErrorCode ec = getJsonNode(jsonStr, jsonPath, any);
        if (ec == JSON_PATH_NOT_EXIST) {
            return true;
        } else if (ec == JSON_PATH_INVALID) {
            return false;
        }
        FromJson(config, any);
    } catch (const autil::legacy::ExceptionBase &e) {
        AUTIL_LOG(WARN,
                  "get config failed, relativePath[%s] jsonPath[%s], exception[%s]",
                  relativePath.c_str(),
                  jsonPath.c_str(),
                  e.what());
        return false;
    }
    return true;
}

SWIFT_TYPEDEF_PTR(ResourceReader);

} // namespace config
} // namespace swift
