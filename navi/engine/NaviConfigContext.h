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

#include "autil/legacy/jsonizable.h"
#include "navi/common.h"

namespace navi {

class NodeDef;

class NaviConfigContext {
 protected:
    NaviConfigContext(const std::string &configPath, autil::legacy::RapidValue *jsonAttrs,
                      autil::legacy::RapidValue *jsonConfig, const NodeDef *nodeDef);
    NaviConfigContext(const std::string &configPath, autil::legacy::RapidValue *jsonAttrs);

 public:
    virtual ~NaviConfigContext();
    template <typename T>
    void Jsonize(const std::string &key, T &value);
    void JsonizeAsString(const std::string &key, std::string &value);
    void JsonizeAsString(const std::string &key, std::string &value, const std::string &defautValue);
    template <typename T>
    void Jsonize(const std::string &key, T &value, const T &defaultValue);
    std::map<std::string, std::string> getBinaryAttrs() const;
    NaviConfigContext enter(const std::string &key);
    const std::string &getConfigPath() const;
    bool isArray() const;
    size_t size() const;
    NaviConfigContext enter(size_t index);
    bool hasKey(const std::string &key);

 private:
    friend class Node;
    friend class ResourceManager;
    friend std::ostream &operator<<(std::ostream &os, const NaviConfigContext &ctx);

 private:
    template <typename T>
    bool JsonizeImpl(const std::string &key, T &value);
    template <typename T>
    bool JsonizeRapidValue(autil::legacy::RapidValue *jsonAttrs, const std::string &key, T &value);
    template <typename T>
    bool JsonizeBinary(const std::string &key, T &value);

    template <typename T>
    bool JsonizeInteger(const std::string &key, T &value);

 private:
    bool hasBinaryKey(const std::string &key) const;
    bool hasIntegerKey(const std::string &key) const;

 protected:
    const std::string &_configPath;
    autil::legacy::RapidValue *_jsonAttrs;
    autil::legacy::RapidValue *_jsonConfig;
    const NodeDef *_nodeDef;
};

extern std::ostream &operator<<(std::ostream &os, const NaviConfigContext &ctx);

NAVI_TYPEDEF_PTR(NaviConfigContext);

template <typename T>
bool NaviConfigContext::JsonizeImpl(const std::string &key, T &value) {
    if (JsonizeRapidValue(_jsonAttrs, key, value)) {
        return true;
    }
    if (JsonizeBinary(key, value)) {
        return true;
    }
    if (JsonizeInteger(key, value)) {
        return true;
    }
    if (JsonizeRapidValue(_jsonConfig, key, value)) {
        return true;
    }
    return false;
}

template <typename T>
bool NaviConfigContext::JsonizeRapidValue(autil::legacy::RapidValue *jsonAttrs, const std::string &key, T &value) {
    if (jsonAttrs && jsonAttrs->HasMember(key.c_str())) {
        autil::legacy::Jsonizable::JsonWrapper json(jsonAttrs);
        assert(json.GetMode() == autil::legacy::Jsonizable::FROM_JSON);
        json.Jsonize(key, value);
        return true;
    }
    return false;
}

template <typename T>
bool NaviConfigContext::JsonizeBinary(const std::string &key, T &value) {
    if (!hasBinaryKey(key)) {
        return false;
    } else {
        AUTIL_LEGACY_THROW(autil::legacy::NotJsonizableException,
                           std::string("invalid Jsonize type for key [") + key + "] in binary attr, must be string");
    }
    return false;
}

template <>
bool NaviConfigContext::JsonizeBinary<std::string>(const std::string &key, std::string &value);

template <typename T>
bool NaviConfigContext::JsonizeInteger(const std::string &key, T &value) {
    if (!hasIntegerKey(key)) {
        return false;
    } else {
        AUTIL_LEGACY_THROW(autil::legacy::NotJsonizableException,
                           std::string("invalid Jsonize type for key [") + key + "] in integer attr, must be integer");
    }
    return false;
}

template <>
bool NaviConfigContext::JsonizeInteger<int64_t>(const std::string &key, int64_t &value);

template <typename T>
void NaviConfigContext::Jsonize(const std::string &key, T &value) {
    bool ok = JsonizeImpl(key, value);
    if (!ok) {
        AUTIL_LEGACY_THROW(autil::legacy::NotJsonizableException,
                           std::string("[") + key + "] not found when try to parse from Json");
    }
}

template <typename T>
void NaviConfigContext::Jsonize(const std::string &key, T &value, const T &defaultValue) {
    bool ok = JsonizeImpl(key, value);
    if (!ok) {
        value = defaultValue;
    }
}

}  // namespace navi
