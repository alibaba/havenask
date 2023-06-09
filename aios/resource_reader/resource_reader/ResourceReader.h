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
#ifndef ISEARCH_RESOURCE_READER_RESOURCEREADER_H
#define ISEARCH_RESOURCE_READER_RESOURCEREADER_H

#include "autil/Log.h"
#include "autil/CommonMacros.h"
#include "autil/legacy/json.h"

namespace resource_reader {

class ResourceReader
{
public:
    enum ErrorCode {
        JSON_PATH_OK,
        JSON_PATH_NOT_EXIST,
        JSON_PATH_INVALID,
    };
public:
    ResourceReader(const std::string &configRoot);
    virtual ~ResourceReader();
public:
    bool getConfigContent(const std::string &relativePath, std::string &content) const;
    // legacy api
    bool getFileContent(const std::string &fileName, std::string &result) const;

    template <typename T>
    bool getConfigWithJsonPath(const std::string &relativePath,
                               const std::string &jsonPath, T &config) const;
    std::string getConfigPath() const;
    std::string getPluginPath() const;
public:
    // tools
    static ErrorCode getJsonNode(const std::string &jsonString,
                                 const std::string &jsonPath,
                                 autil::legacy::Any &any);
    static ErrorCode getJsonNode(const autil::legacy::json::JsonMap &jsonMap,
                                 const std::string &jsonPath,
                                 autil::legacy::Any &any);
    static std::string validatePath(const std::string &path);
private:
    bool getJsonMap(const std::string &relativePath,
                    autil::legacy::json::JsonMap &jsonMap) const;

private:
    std::string _configRoot;
private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<ResourceReader> ResourceReaderPtr;

/////////////////////////////////////////////////////////////////////////
template <typename T>
inline bool ResourceReader::getConfigWithJsonPath(const std::string &relativePath,
        const std::string &jsonPath, T &config) const
{
    autil::legacy::json::JsonMap jsonMap;
    if (!getJsonMap(relativePath, jsonMap)) {
        return false;
    }

    autil::legacy::Any any;
    ErrorCode ec = getJsonNode(jsonMap, jsonPath, any);
    if (ec == JSON_PATH_NOT_EXIST) {
        return true;
    } else if (ec == JSON_PATH_INVALID) {
        return false;
    }

    try {
        FromJson(config, any);
    } catch (const autil::legacy::ExceptionBase &e) {
        AUTIL_LOG(ERROR, "get config failed, relativePath[%s] jsonPath[%s], exception[%s]",
                  relativePath.c_str(), jsonPath.c_str(), e.what());
        return false;
    }
    return true;
}

}

#endif //ISEARCH_RESOURCE_READER_RESOURCEREADER_H
