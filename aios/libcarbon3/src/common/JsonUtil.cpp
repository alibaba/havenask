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
#include "common/JsonUtil.h"
#include "carbon/Log.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
BEGIN_CARBON_NAMESPACE(common);

CARBON_LOG_SETUP(common, JsonUtil);

JsonUtil::JsonUtil() {
}

JsonUtil::~JsonUtil() {
}

#define LOG_AND_SET_ERROR(ec, em) {                     \
        string errorMsg(em);                            \
        CARBON_LOG(ERROR, "%s.", errorMsg.c_str());     \
        errorInfo->errorCode = ec;                      \
        errorInfo->errorMsg = errorMsg;                 \
}
        
bool JsonUtil::create(JsonMap *json, const string &path,
                      const Any &value, ErrorInfo *errorInfo)
{
    *errorInfo = ErrorInfo();
    JsonMap retJsonMap = *json;
    vector<string> keys = StringUtil::split(path, ".");
    if (keys.size() == 0) {
        retJsonMap = AnyCast<JsonMap>(value);
        *json = retJsonMap;
        return true;
    }

    string keyPath = "";
    JsonMap *jm = &retJsonMap;
    for (size_t i = 0; i < keys.size() - 1; i++) {
        const string &k = keys[i];
        keyPath = (i == 0) ? k : (keyPath += "." + k);
        JsonMap::iterator it = jm->find(k);
        if (it == jm->end()) {
            (*jm)[k] = JsonMap();
            jm = AnyCast<JsonMap>(&(*jm)[k]);
            continue;
        }
        
        Any *v = &it->second;
        if (typeid(JsonMap) == v->GetType()) {
            jm = AnyCast<JsonMap>(v);
        } else {
            LOG_AND_SET_ERROR(EC_JSON_NODE_ALREADY_EXIST,
                    "create json node failed, " + keyPath + " is not JsonMap");
            return false;
        }
    }

    const string &k = keys[keys.size() - 1];
    if (jm->find(k) != jm->end()) {
        LOG_AND_SET_ERROR(EC_JSON_NODE_ALREADY_EXIST,
                          "create json node failed, " + path + " already exist");
        return false;
    }
    
    (*jm)[k] = value;
    *json = retJsonMap;
    return true;
}

bool JsonUtil::update(JsonMap *json, const string &path,
                      const Any &value, ErrorInfo *errorInfo)
{
    vector<string> keys = StringUtil::split(path, ".");
    if (keys.size() == 0) {
        *json = AnyCast<JsonMap>(value);
        return true;
    }

    string keyPath = "";
    JsonMap *jm = json;
    for (size_t i = 0; i < keys.size() - 1; i++) {
        const string &k = keys[i];
        keyPath = (i == 0) ? k : (keyPath += "." + k);
        JsonMap::iterator it = jm->find(k);
        if (it == jm->end()) {
            LOG_AND_SET_ERROR(EC_JSON_NODE_NOT_EXIST,
                    "update json node failed, path not exist:" + keyPath);
            return false;
        }
        
        Any *v = &it->second;
        if (typeid(JsonMap) == v->GetType()) {
            jm = AnyCast<JsonMap>(v);
        } else {
            LOG_AND_SET_ERROR(EC_JSON_NODE_NOT_EXIST,
                              "update json node failed, path not exist:" + path);
            return false;
        }
    }

    const string &k = keys[keys.size() - 1];
    if (jm->find(k) == jm->end()) {
        LOG_AND_SET_ERROR(EC_JSON_NODE_NOT_EXIST,
                          "update json node failed, path not exist:" + path);
        return false;
    }
    
    (*jm)[k] = value;

    return true;
}

bool JsonUtil::read(const JsonMap &json, const string &path,
                    Any *value, ErrorInfo *errorInfo)
{
    vector<string> keys = StringUtil::split(path, ".");
    if (keys.size() == 0) {
        *value = ToJson(json);
        return true;
    }

    string keyPath = "";
    const Any *v = NULL;
    for (size_t i = 0; i < keys.size(); i++) {
        const string &k = keys[i];
        keyPath = (i == 0) ? k : (keyPath + "." + k);

        const JsonMap *jsonMap = NULL;
        if (v == NULL) {
            jsonMap = &json;
        } else {
            if (typeid(JsonMap) == v->GetType()) {
                jsonMap = AnyCast<JsonMap>(v);
            }

            if (jsonMap == NULL) {
                LOG_AND_SET_ERROR(
                        EC_JSON_NODE_NOT_EXIST,
                        "read value from json failed, " + keyPath + " not exist");
                return false;
            }
        }
        
        JsonMap::const_iterator it = jsonMap->find(k);
        if (it == jsonMap->end()) {
            LOG_AND_SET_ERROR(EC_JSON_NODE_NOT_EXIST,
                    "read value from json failed, " + keyPath + " not exist");
            return false;
        }

        v = &(it->second);
    }

    *value = *v;
    return true;
}

bool JsonUtil::del(JsonMap *json, const string &path, ErrorInfo *errorInfo) {
    vector<string> keys = StringUtil::split(path, ".");
    if (keys.size() == 0) {
        (*json) = JsonMap();
        return true;
    }

    string keyPath;
    JsonMap *jm = json;
    for (size_t i = 0; i < keys.size() - 1; i++) {
        const string &k = keys[i];
        keyPath = (i == 0) ? k : (keyPath += "." + k);
        JsonMap::iterator it = jm->find(k);
        if (it == jm->end()) {
            return true;
        }

        Any *v = &(it->second);
        if (typeid(JsonMap) == v->GetType()) {
            jm = AnyCast<JsonMap>(v);
        } else {
            return true;
        }
    }

    jm->erase(keys[keys.size() - 1]);

    return true;
}

END_CARBON_NAMESPACE(common);

