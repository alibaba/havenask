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
#include "resource_reader/ResourceReader.h"
#include "resource_reader/ConfigParser.h"
#include "fslib/fslib.h"
#include "autil/StringUtil.h"
#include "autil/StringTokenizer.h"
#include "autil/legacy/jsonizable.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace fslib;
using namespace fslib::fs;

namespace resource_reader {
AUTIL_LOG_SETUP(resource_reader, ResourceReader);

ResourceReader::ResourceReader(const string &configRoot)
    : _configRoot(validatePath(configRoot))
{
}

ResourceReader::~ResourceReader() {
}

bool ResourceReader::getConfigContent(
        const string &relativePath, string &content) const
{
    string absPath = FileSystem::joinFilePath(_configRoot, relativePath);
    return EC_OK == FileSystem::readFile(absPath, content);
}

bool ResourceReader::getFileContent(const string &fileName, string &result) const
{
    return getConfigContent(fileName, result);
}

string ResourceReader::getConfigPath() const {
    return _configRoot;
}

string ResourceReader::validatePath(const string &path) {
    if (path.empty()) {
        return path;
    }
    if ((*path.rbegin()) == '/') {
        return path;
    }
    return path + '/';
}

ResourceReader::ErrorCode ResourceReader::getJsonNode(const string &jsonString,
        const string &jsonPath, autil::legacy::Any &any)
{
    vector<string> jsonPathVec = StringTokenizer::tokenize(StringView(jsonPath), '.',
            StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
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
            AUTIL_LOG(DEBUG, "get config by json path failed."
                   "key[%s] not found in json map.", nodeName.c_str());
            return JSON_PATH_NOT_EXIST;
        }
        any = it->second;
        while (pos != string::npos) {
            size_t end = jsonPathVec[i].find(']', pos);
            if (end == string::npos) {
                AUTIL_LOG(ERROR, "get config by json path failed."
                        "Invalid json path[%s], missing ']'.", jsonPathVec[i].c_str());
                return JSON_PATH_INVALID;
            }

            size_t idx;
            string idxStr = jsonPathVec[i].substr(pos + 1, end - pos - 1);
            if (!StringUtil::fromString<size_t>(idxStr, idx)) {
                AUTIL_LOG(ERROR, "get config by json path failed.Invalid json vector idx[%s], .", idxStr.c_str());
                return JSON_PATH_INVALID;
            }
            JsonArray jsonVec = AnyCast<JsonArray>(any);
            if (idx >= jsonVec.size()) {
                AUTIL_LOG(ERROR, "get config by json path failed.[%s] out of range, total size[%ld].", 
                        jsonPathVec[i].c_str(), jsonVec.size());
                return JSON_PATH_INVALID;
            }
            any = jsonVec[idx];
            pos = jsonPathVec[i].find('[', end + 1);
        }
    }
    return JSON_PATH_OK;
}

ResourceReader::ErrorCode ResourceReader::getJsonNode(
        const JsonMap &jsonMap, const string &jsonPath, Any &any)
{
    return getJsonNode(ToJsonString(jsonMap), jsonPath, any);
}

bool ResourceReader::getJsonMap(const string &relativePath, JsonMap &jsonMap) const {
    ConfigParser parser(_configRoot);
    return parser.parse(relativePath, jsonMap);
}

string ResourceReader::getPluginPath() const {
    return FileSystem::joinFilePath(getConfigPath(), "plugins") + "/";
}

}
