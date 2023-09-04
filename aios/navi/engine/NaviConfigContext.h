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
#include "navi/engine/CreatorRegistry.h"
#include "navi/common.h"
#include <utility>

namespace navi {

class NodeDef;

class NaviConfigContext {
protected:
    NaviConfigContext();
    NaviConfigContext(const std::string &configPath,
                      autil::legacy::RapidValue *jsonAttrs,
                      autil::legacy::RapidValue *jsonConfig,
                      const NodeDef *nodeDef,
                      autil::legacy::RapidValue *jsonConfig2);
    NaviConfigContext(const std::string &configPath, autil::legacy::RapidValue *jsonAttrs, const NodeDef *nodeDef);
public:
    virtual ~NaviConfigContext();
    template <typename T>
    void Jsonize(const std::string &key, T &value);
    void JsonizeAsString(const std::string &key, std::string &value);
    void JsonizeAsString(const std::string &key, std::string &value, const std::string &defautValue);
    template <typename T>
    void Jsonize(const std::string &key, T &value, const T &defaultValue);
    void Jsonize(const std::string &key, std::string &value, const char *defaultValue);
    std::map<std::string, std::string> getBinaryAttrs() const;
    NaviConfigContext enter(const std::string &key);
    const std::string &getConfigPath() const;
    bool isArray() const;
    size_t size() const;
    NaviConfigContext enter(size_t index);
    bool hasKey(const std::string &key) const;
    bool hasJsonKey(const std::string &key) const;
    bool hasBinaryKey(const std::string &key) const;
    bool hasIntegerKey(const std::string &key) const;
private:
    friend class Node;
    friend class ResourceManager;
    friend class ResourceInitContext;
    friend class ResourceCreateKernel;
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
    template <typename T>
    bool JsonizeBuildin(const std::string &key, T &value);
protected:
    const std::string *_configPath = nullptr;
    autil::legacy::RapidValue *_jsonAttrs = nullptr;
    autil::legacy::RapidValue *_jsonConfig = nullptr;
    const NodeDef *_nodeDef = nullptr;
    autil::legacy::RapidValue *_jsonConfig2 = nullptr;
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
    if (JsonizeRapidValue(_jsonConfig2, key, value)) {
        return true;
    }
    if (JsonizeBuildin(key, value)) {
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
bool NaviConfigContext::JsonizeBuildin(const std::string &key, T &value) {
    if (NAVI_BUILDIN_ATTR_NODE == key || NAVI_BUILDIN_ATTR_KERNEL == key) {
        if (!_nodeDef) {
            AUTIL_LEGACY_THROW(autil::legacy::NotJsonizableException,
                               std::string("invalid buildin Jsonize key [") + key + "], not supported");
        } else {
            AUTIL_LEGACY_THROW(autil::legacy::NotJsonizableException,
                               std::string("invalid Jsonize type for key [") + key +
                                   "] in buildin attr, must be string");
        }
    }
    return false;
}

template <>
bool NaviConfigContext::JsonizeBuildin(const std::string &key, std::string &value);

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

inline void NaviConfigContext::Jsonize(const std::string &key, std::string &value, const char *defaultValue) {
    bool ok = JsonizeImpl(key, value);
    if (!ok) {
        value = defaultValue;
    }
}

class NaviJsonizeInfo : public autil::legacy::Jsonizable {
public:
    NaviJsonizeInfo() {
    }
    NaviJsonizeInfo(const std::string &memberName,
                    const std::string &memberType,
                    const std::string &functionName,
                    const std::string &jsonKey,
                    bool hasDefault)
        : _memberName(memberName)
        , _memberType(memberType)
        , _functionName(functionName)
        , _jsonKey(jsonKey)
        , _hasDefault(hasDefault)
    {
    }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
public:
    std::string _memberName;
    std::string _memberType;
    std::string _functionName;
    std::string _jsonKey;
    bool _hasDefault = false;
};

#define NAVI_JSONIZE_1(CTX, NAME, MEMBER) NAVI_JSONIZE_HELPER_1(__COUNTER__, CTX, NAME, MEMBER)
#define NAVI_JSONIZE_HELPER_1(ctr, CTX, NAME, MEMBER) NAVI_JSONIZE_REGISTER_RESOURCE_UNIQ_1(ctr, CTX, NAME, MEMBER)
#define NAVI_JSONIZE_REGISTER_RESOURCE_UNIQ_1(ctr, CTX, NAME, MEMBER)                                                  \
    struct NaviJsonizeHelper##ctr {                                                                                    \
        static void json_register##ctr() {                                                                             \
            ::navi::CreatorRegistry::current(navi::RT_KERNEL)->addJsonizeFunc(&json_log_##ctr);                        \
        }                                                                                                              \
        static void json_log_##ctr(navi::NaviJsonizeInfo &info) {                                                      \
            info = navi::NaviJsonizeInfo(#MEMBER, typeid(decltype(MEMBER)).name(), __PRETTY_FUNCTION__, NAME, false);  \
        }                                                                                                              \
        __attribute__((used)) static void json_constructor_##ctr() {                                                   \
            __attribute__((used)) __attribute__((section(".ctors"))) static void (*f)() = &json_register##ctr;         \
        }                                                                                                              \
    };                                                                                                                 \
    CTX.Jsonize(NAME, MEMBER);

#define NAVI_JSONIZE_2(CTX, NAME, MEMBER, DEFAULT_VALUE)                                                               \
    NAVI_JSONIZE_HELPER_2(__COUNTER__, CTX, NAME, MEMBER, DEFAULT_VALUE)
#define NAVI_JSONIZE_HELPER_2(ctr, CTX, NAME, MEMBER, DEFAULT_VALUE) NAVI_JSONIZE_REGISTER_RESOURCE_UNIQ_2(ctr, CTX, NAME, MEMBER, DEFAULT_VALUE)
#define NAVI_JSONIZE_REGISTER_RESOURCE_UNIQ_2(ctr, CTX, NAME, MEMBER, DEFAULT_VALUE)                                   \
    struct NaviJsonizeHelper##ctr {                                                                                    \
        static void json_register##ctr() {                                                                             \
            ::navi::CreatorRegistry::current(navi::RT_KERNEL)->addJsonizeFunc(&json_log_##ctr);                        \
        }                                                                                                              \
        static void json_log_##ctr(navi::NaviJsonizeInfo &info) {                                                      \
            info = navi::NaviJsonizeInfo(#MEMBER, typeid(decltype(MEMBER)).name(), __PRETTY_FUNCTION__, NAME, true);   \
        }                                                                                                              \
        __attribute__((used)) static void json_constructor_##ctr() {                                                   \
            __attribute__((used)) __attribute__((section(".ctors"))) static void (*f)() = &json_register##ctr;         \
        }                                                                                                              \
    };                                                                                                                 \
    CTX.Jsonize(NAME, MEMBER, DEFAULT_VALUE);

#define NAVI_GET_MACRO(_1,_2,_3,_4,NAME,...) NAME
#define NAVI_JSONIZE(...) NAVI_GET_MACRO(__VA_ARGS__, NAVI_JSONIZE_2, NAVI_JSONIZE_1)(__VA_ARGS__)

}  // namespace navi
