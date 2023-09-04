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
#include "swift/config/ResourceReader.h"

#include <cstddef>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "autil/Span.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/json.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace fslib;
using namespace fslib::fs;

namespace swift {
namespace config {
AUTIL_LOG_SETUP(swift, ResourceReader);

ResourceReader::ResourceReader(const string &configRoot) : _configRoot(configRoot) {}

ResourceReader::~ResourceReader() {}

bool ResourceReader::getConfigContent(const string &relativePath, string &content) const {
    string absPath = FileSystem::joinFilePath(_configRoot, relativePath);
    return fslib::EC_OK == FileSystem::readFile(absPath, content);
}

bool ResourceReader::getFileContent(string &result, const string &fileName) const {
    return getConfigContent(fileName, result);
}

string ResourceReader::getConfigPath() const {
    size_t pos = _configRoot.rfind("/");
    if (pos != string::npos && pos == _configRoot.size() - 1) {
        return _configRoot;
    } else {
        return _configRoot + '/';
    }
}

ResourceReader::ErrorCode
ResourceReader::getJsonNode(const string &jsonString, const string &jsonPath, autil::legacy::Any &any) {
    vector<string> jsonPathVec = StringTokenizer::tokenize(
        StringView(jsonPath), '.', StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    any = json::ParseJson(jsonString);
    for (size_t i = 0; i < jsonPathVec.size(); ++i) {
        size_t pos = jsonPathVec[i].find('[');
        string nodeName;
        if (pos == string::npos) {
            nodeName = jsonPathVec[i];
        } else {
            nodeName = jsonPathVec[i].substr(0, pos);
        }
        JsonMap jsonMap = AnyCast<JsonMap>(any);
        JsonMap::const_iterator it = jsonMap.find(nodeName);
        if (it == jsonMap.end()) {
            AUTIL_LOG(DEBUG,
                      "get config by json path failed."
                      "key[%s] not found in json map.",
                      nodeName.c_str());
            return JSON_PATH_NOT_EXIST;
        }
        any = it->second;
        while (pos != string::npos) {
            size_t end = jsonPathVec[i].find(']', pos);
            if (end == string::npos) {
                AUTIL_LOG(WARN,
                          "get config by json path failed."
                          "Invalid json path[%s], missing ']'.",
                          jsonPathVec[i].c_str());
                return JSON_PATH_INVALID;
            }

            size_t idx;
            string idxStr = jsonPathVec[i].substr(pos + 1, end - pos - 1);
            if (!StringUtil::fromString<size_t>(idxStr, idx)) {
                AUTIL_LOG(WARN,
                          "get config by json path failed."
                          "Invalid json vector idx[%s], .",
                          idxStr.c_str());
                return JSON_PATH_INVALID;
            }
            JsonArray jsonVec = AnyCast<JsonArray>(any);
            if (idx >= jsonVec.size()) {
                AUTIL_LOG(WARN,
                          "get config by json path failed."
                          "[%s] out of range, total size[%lu].",
                          jsonPathVec[i].c_str(),
                          jsonVec.size());
                return JSON_PATH_INVALID;
            }
            any = jsonVec[idx];
            pos = jsonPathVec[i].find('[', end + 1);
        }
    }
    return JSON_PATH_OK;
}

} // namespace config
} // namespace swift
