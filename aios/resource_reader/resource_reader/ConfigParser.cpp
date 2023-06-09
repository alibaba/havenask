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
#include "resource_reader/ConfigParser.h"
#include "fslib/fslib.h"
#include "autil/legacy/jsonizable.h"
using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace fslib;
using namespace fslib::fs;

namespace resource_reader {
AUTIL_LOG_SETUP(resource_reader, ConfigParser);

const std::string ConfigParser::INHERIT_FROM_KEY = "inherit_from";

ConfigParser::ConfigParser(const std::string &basePath)
    : _basePath(basePath)
{
}

ConfigParser::~ConfigParser() {
}

bool ConfigParser::parse(const string &relativePath, JsonMap &jsonMap) const {
    string jsonFile = FileSystem::joinFilePath(_basePath, relativePath);
    string content;
    if (EC_OK != FileSystem::readFile(jsonFile, content)) {
        AUTIL_LOG(ERROR, "read file %s failed", jsonFile.c_str());
        return false;
    }
    // check loop in an inheritance path
    if (find(_configFileNames.begin(), _configFileNames.end(), jsonFile)
        != _configFileNames.end())
    {
        AUTIL_LOG(ERROR, "inherit loop detected, file: %s", jsonFile.c_str());
        return false;
    }
    _configFileNames.push_back(jsonFile);

    try {
        FromJsonString(jsonMap, content);
    } catch (const ExceptionBase &e) {
        AUTIL_LOG(ERROR, "jsonize %s failed, exception: %s", content.c_str(), e.what());
        return false;
    }
    if (!merge(jsonMap)) {
        AUTIL_LOG(ERROR, "merge inherit of %s failed", content.c_str());
        return false;
    }

    // NOTE: it won't pop_back in "return false" code path,
    // since it already fails ...
    _configFileNames.pop_back();

    return true;
}

bool ConfigParser::merge(JsonMap &jsonMap) const {
    // merge with inner level
    // inner level takes effect first, then outter level fill the blanks
    // that inner level left.
    for (JsonMap::iterator it = jsonMap.begin();
         it != jsonMap.end(); it++)
    {
        const string &key = it->first;
        Any &any = it->second;
        if (any.GetType() == typeid(JsonMap)) {
            JsonMap &childJsonMap = *AnyCast<JsonMap>(&any);
            if (!merge(childJsonMap)) {
                AUTIL_LOG(ERROR, "merge child %s failed", key.c_str());
                return false;
            }
        }
    }
    // merge with this level
    if (!mergeWithInherit(jsonMap)) {
        AUTIL_LOG(ERROR, "merge with inherit fail");
        return false;
    }
    return true;
}

bool ConfigParser::mergeWithInherit(JsonMap &jsonMap) const {
    JsonMap::iterator it = jsonMap.find(INHERIT_FROM_KEY);
    if (it == jsonMap.end()) {
        return true;
    }
    string inheritFromFile;
    try {
        FromJson(inheritFromFile, it->second);
    } catch (const ExceptionBase &e) {
        AUTIL_LOG(ERROR, "jsonize inherit_from failed, exception: %s", e.what());
        return false;
    }
    JsonMap inheritJsonMap;
    if (!parse(inheritFromFile, inheritJsonMap)) {
        AUTIL_LOG(ERROR, "parse %s failed", inheritFromFile.c_str());
        return false;
    }
    return mergeJsonMap(inheritJsonMap, jsonMap);
}

bool ConfigParser::mergeJsonMap(const JsonMap &src, JsonMap &dst) const {
    JsonMap::const_iterator it = src.begin();
    for (; it != src.end(); it++) {
        const string &key = it->first;
        const Any &value = it->second;
        if (dst.find(key) == dst.end()) {
            dst[key] = value;
        } else if (dst[key].GetType() != value.GetType()) {
            AUTIL_LOG(ERROR, "merge error, type not match for [%s]", key.c_str());
            return false;
        } else if (value.GetType() == typeid(JsonMap)) {
            JsonMap &dstChild = *AnyCast<JsonMap>(&dst[key]);
            const JsonMap &srcChild = AnyCast<JsonMap>(value);
            if (!mergeJsonMap(srcChild, dstChild)) {
                AUTIL_LOG(ERROR, "merge child[%s] failed", key.c_str());
                return false;
            }
        } else if (value.GetType() == typeid(JsonArray)) {
            if (!mergeJsonArray(*AnyCast<JsonArray>(&dst[key]))) {
                AUTIL_LOG(ERROR, "merge child[%s] failed", key.c_str());
                return false;
            }
        }
    }
    return true;
}

bool ConfigParser::mergeJsonArray(JsonArray &dst) const {
    for (JsonArray::iterator it = dst.begin(); it != dst.end(); it++) {
        if (it->GetType() == typeid(JsonMap)) {
            if (!merge(*AnyCast<JsonMap>(&*it))) {
                AUTIL_LOG(ERROR, "merge child[%s] failed", ToString(*it).c_str());
                return false;
            }
        } else if (it->GetType() == typeid(JsonArray)) {
            if (!mergeJsonArray(*AnyCast<JsonArray>(&*it))) {
                AUTIL_LOG(ERROR, "merge child[%s] failed", ToString(*it).c_str());
                return false;
            }
        }
    }
    return true;
}

}
