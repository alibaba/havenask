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
#include "build_service/config/ConfigParser.h"

#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/config/ConfigDefine.h"
#include "fslib/fslib.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace build_service::util;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace build_service { namespace config {
BS_LOG_SETUP(config, ConfigParser);

ConfigParser::ConfigParser(const std::string& basePath, const unordered_map<string, string>& fileMap)
{
    _basePath = basePath;
    _fileMap = fileMap;
}

ConfigParser::~ConfigParser() {}

bool ConfigParser::parseExternalFile(const string& jsonFile, JsonMap& jsonMap) const
{
    string content;
    if (!fslib::util::FileUtil::readFile(jsonFile, content)) {
        string errorMsg = "read file " + jsonFile + " failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    // check loop in an inheritance path
    if (find(_configFileNames.begin(), _configFileNames.end(), jsonFile) != _configFileNames.end()) {
        string errorMsg = "inherit loop detected, file: " + jsonFile;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _configFileNames.push_back(jsonFile);

    try {
        FromJsonString(jsonMap, content);
    } catch (const ExceptionBase& e) {
        string errorMsg = "jsonize " + content + " failed, exception: " + string(e.what());
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (!merge(jsonMap, true)) {
        string errorMsg = "merge inherit of  " + content + " failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _configFileNames.pop_back();
    return true;
}

bool ConfigParser::parseRelativePath(const string& relativePath, JsonMap& jsonMap, bool* isNotExist) const
{
    string jsonFile = fslib::util::FileUtil::joinFilePath(_basePath, relativePath);
    string content;
    auto it = _fileMap.find(relativePath);
    if (it != _fileMap.end()) {
        content = it->second;
    } else if (!fslib::util::FileUtil::readFile(jsonFile, content, isNotExist)) {
        string errorMsg =
            "read file " + jsonFile + " failed, _basePath: " + _basePath + ", relativePath: " + relativePath;
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }

    // check loop in an inheritance path
    if (find(_configFileNames.begin(), _configFileNames.end(), jsonFile) != _configFileNames.end()) {
        string errorMsg = "inherit loop detected, file: " + jsonFile;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _configFileNames.push_back(jsonFile);

    try {
        FromJsonString(jsonMap, content);
    } catch (const ExceptionBase& e) {
        string errorMsg = "jsonize " + content + " failed, exception: " + string(e.what());
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (!merge(jsonMap, false)) {
        string errorMsg = "merge inherit of  " + content + " failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    // NOTE: it won't pop_back in "return false" code path,
    // since it already fails ...
    _configFileNames.pop_back();
    return true;
}

bool ConfigParser::merge(JsonMap& jsonMap, bool isExternal) const
{
    // merge with inner level
    // inner level takes effect first, then outter level fill the blanks
    // that inner level left.
    for (JsonMap::iterator it = jsonMap.begin(); it != jsonMap.end(); it++) {
        const string& key = it->first;
        Any& any = it->second;
        if (any.GetType() == typeid(JsonMap)) {
            JsonMap& childJsonMap = *AnyCast<JsonMap>(&any);
            if (!merge(childJsonMap, isExternal)) {
                BS_LOG(ERROR, "merge child `%s` fail", key.c_str());
                return false;
            }
        } else if (any.GetType() == typeid(JsonArray)) {
            JsonArray& childJsonArray = *AnyCast<JsonArray>(&any);
            uint32_t i = 0;
            for (JsonArray::iterator childIt = childJsonArray.begin(); childIt != childJsonArray.end(); childIt++) {
                if (childIt->GetType() == typeid(JsonMap)) {
                    if (!merge(*AnyCast<JsonMap>(&*childIt), isExternal)) {
                        BS_LOG(ERROR, "merge child `%s[%u]` fail", key.c_str(), i);
                        return false;
                    }
                }
                i += 1;
            }
        }
    }
    // merge with this level
    // *relative file merge with relative config file & external file
    // external file only merge with external file
    if (!isExternal && !mergeWithInherit(jsonMap, false)) {
        BS_LOG(ERROR, "merge with inherit fail");
        return false;
    }
    if (!mergeWithInherit(jsonMap, true)) {
        BS_LOG(ERROR, "merge with external file inherit fail");
        return false;
    }
    return true;
}

bool ConfigParser::mergeWithInherit(JsonMap& jsonMap, bool isExternal) const
{
    string key = isExternal ? INHERIT_FROM_EXTERNAL_KEY : INHERIT_FROM_KEY;
    JsonMap::iterator it = jsonMap.find(key);
    if (it == jsonMap.end()) {
        return true;
    }

    string inheritFromFile;
    try {
        FromJson(inheritFromFile, it->second);
    } catch (const ExceptionBase& e) {
        BS_LOG(ERROR, "jsonize %s failed, exception: %s", key.c_str(), e.what());
        return false;
    }

    JsonMap inheritJsonMap;
    if (!parse(inheritFromFile, inheritJsonMap, !isExternal)) {
        BS_LOG(ERROR, "parse %s failed", inheritFromFile.c_str());
        return false;
    }
    return mergeJsonMap(inheritJsonMap, jsonMap, isExternal);
}

bool ConfigParser::mergeJsonMap(const JsonMap& src, JsonMap& dst, bool isExternal) const
{
    JsonMap::const_iterator it = src.begin();
    for (; it != src.end(); it++) {
        const string& key = it->first;
        const Any& value = it->second;
        if (dst.find(key) == dst.end()) {
            dst[key] = value;
        } else if (dst[key].GetType() != value.GetType()) {
            string errorMsg = "merge error, type not match for " + key;
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        } else if (value.GetType() == typeid(JsonMap)) {
            JsonMap& dstChild = *AnyCast<JsonMap>(&dst[key]);
            const JsonMap& srcChild = AnyCast<JsonMap>(value);
            if (!mergeJsonMap(srcChild, dstChild, isExternal)) {
                string errorMsg = "merge child[" + key + "] failed";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }
        } else if (value.GetType() == typeid(JsonArray)) {
            if (!mergeJsonArray(*AnyCast<JsonArray>(&dst[key]), isExternal)) {
                string errorMsg = "merge child[" + key + "] failed";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }
        }
    }
    return true;
}

bool ConfigParser::mergeJsonArray(JsonArray& dst, bool isExternal) const
{
    for (JsonArray::iterator it = dst.begin(); it != dst.end(); it++) {
        if (it->GetType() == typeid(JsonMap)) {
            if (!merge(*AnyCast<JsonMap>(&*it), isExternal)) {
                string errorMsg = "merge child[" + ToString(*it) + "] failed";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }
        } else if (it->GetType() == typeid(JsonArray)) {
            if (!mergeJsonArray(*AnyCast<JsonArray>(&*it), isExternal)) {
                string errorMsg = "merge child[" + ToString(*it) + "] failed";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }
        }
    }
    return true;
}

}} // namespace build_service::config
