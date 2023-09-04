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
#include "navi/engine/NaviConfigContext.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/perf/NaviSymbolTable.h"

using namespace autil::legacy;

namespace navi {

NaviConfigContext::NaviConfigContext() {
}

NaviConfigContext::NaviConfigContext(
        const std::string &configPath,
        RapidValue *jsonAttrs,
        RapidValue *jsonConfig,
        const NodeDef *nodeDef,
        autil::legacy::RapidValue *jsonConfig2)
    : _configPath(&configPath)
    , _jsonAttrs(jsonAttrs)
    , _jsonConfig(jsonConfig)
    , _nodeDef(nodeDef)
    , _jsonConfig2(jsonConfig2)
{
}

NaviConfigContext::NaviConfigContext(const std::string &configPath,
                                     RapidValue *jsonAttrs,
                                     const NodeDef *nodeDef)
    : _configPath(&configPath)
    , _jsonAttrs(jsonAttrs)
    , _jsonConfig(nullptr)
    , _nodeDef(nodeDef)
    , _jsonConfig2(nullptr)
{
}

NaviConfigContext::~NaviConfigContext() {
}

std::map<std::string, std::string> NaviConfigContext::getBinaryAttrs() const {
    static std::map<std::string, std::string> defaultAttrs;
    if (!_nodeDef) {
        return defaultAttrs;
    }
    const auto &attrMap = _nodeDef->binary_attrs();
    if (attrMap.empty()) {
        return defaultAttrs;
    }
    std::map<std::string, std::string> binaryAttrs;
    for (const auto &attr : attrMap) {
        binaryAttrs.emplace(attr.first, attr.second);
    }
    return binaryAttrs;
}

NaviConfigContext NaviConfigContext::enter(const std::string &key) {
    autil::legacy::RapidValue *jsonAttrs = nullptr;
    Jsonize(key, jsonAttrs);
    return NaviConfigContext(*_configPath, jsonAttrs, _nodeDef);
}

const std::string &NaviConfigContext::getConfigPath() const {
    return *_configPath;
}

bool NaviConfigContext::isArray() const {
    if (!_jsonAttrs) {
        return false;
    }
    return _jsonAttrs->IsArray();
}

size_t NaviConfigContext::size() const {
    if (!_jsonAttrs) {
        return 0;
    }
    return _jsonAttrs->Size();
}

NaviConfigContext NaviConfigContext::enter(size_t index) {
    if (_jsonAttrs) {
        return NaviConfigContext(*_configPath, &(*_jsonAttrs)[index], _nodeDef);
    }

    return NaviConfigContext(*_configPath, nullptr, _nodeDef);
}

bool NaviConfigContext::hasKey(const std::string &key) const {
    return hasJsonKey(key) || hasBinaryKey(key) || hasIntegerKey(key);
}

void NaviConfigContext::JsonizeAsString(const std::string &key,
                                        std::string &value)
{
    autil::legacy::RapidValue *jsonValue = nullptr;
    Jsonize(key, jsonValue);
    if (jsonValue) {
        value = FastToJsonString(*jsonValue);
    }
}

void NaviConfigContext::JsonizeAsString(const std::string &key,
                                        std::string &value,
                                        const std::string &defaultValue)
{
    RapidValue *jsonValue = nullptr;
    Jsonize(key, jsonValue, jsonValue);
    if (jsonValue) {
        value = FastToJsonString(*jsonValue);
    } else {
        value = defaultValue;
    }
}

bool NaviConfigContext::hasJsonKey(const std::string &key) const {
    return (_jsonAttrs && _jsonAttrs->HasMember(key.c_str())) || (_jsonConfig && _jsonConfig->HasMember(key.c_str())) ||
           (_jsonConfig2 && _jsonConfig2->HasMember(key.c_str()));
}

bool NaviConfigContext::hasBinaryKey(const std::string &key) const {
    if (!_nodeDef) {
        return false;
    }
    const auto &attrMap = _nodeDef->binary_attrs();
    return attrMap.end() != attrMap.find(key);
}

template <>
bool NaviConfigContext::JsonizeBinary<std::string>(const std::string &key,
                                                   std::string &value)
{
    if (!_nodeDef) {
        return false;
    }
    const auto &attrMap = _nodeDef->binary_attrs();
    auto it = attrMap.find(key);
    if (attrMap.end() != it) {
        value = it->second;
        return true;
    }
    return false;
}

bool NaviConfigContext::hasIntegerKey(const std::string &key) const {
    if (!_nodeDef) {
        return false;
    }
    const auto &attrMap = _nodeDef->integer_attrs();
    return attrMap.end() != attrMap.find(key);
}

template <>
bool NaviConfigContext::JsonizeInteger<int64_t>(const std::string &key,
                                                int64_t &value)
{
    if (!_nodeDef) {
        return false;
    }
    const auto &attrMap = _nodeDef->integer_attrs();
    auto it = attrMap.find(key);
    if (attrMap.end() != it) {
        value = it->second;
        return true;
    }
    return false;
}

template <>
bool NaviConfigContext::JsonizeBuildin(const std::string &key, std::string &value) {
    if (NAVI_BUILDIN_ATTR_NODE == key) {
        if (_nodeDef) {
            value = _nodeDef->name();
            return true;
        } else {
            AUTIL_LEGACY_THROW(autil::legacy::NotJsonizableException,
                               std::string("invalid buildin Jsonize key [") + key + "], not supported");
        }
    } else if (NAVI_BUILDIN_ATTR_KERNEL == key) {
        if (_nodeDef) {
            value = _nodeDef->kernel_name();
            return true;
        } else {
            AUTIL_LEGACY_THROW(autil::legacy::NotJsonizableException,
                               std::string("invalid buildin Jsonize key [") + key + "], not supported");
        }
    }
    return false;
}

void NaviJsonizeInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("member", _memberName);
    json.Jsonize("member_type", NaviSymbolTable::parseSymbol(_memberType.c_str()));
    json.Jsonize("json_key", _jsonKey);
    json.Jsonize("has_default", _hasDefault);
}

std::ostream &operator<<(std::ostream &os, const NaviConfigContext &ctx) {
    os << "NaviConfigContext ";
    if (ctx._jsonAttrs) {
        os << "attr: " << FastToJsonString(*ctx._jsonAttrs) << std::endl;
    }
    if (ctx._jsonConfig) {
        os << "config: " << FastToJsonString(*ctx._jsonConfig) << std::endl;
    }
    if (ctx._jsonConfig2) {
        os << "config2: " << FastToJsonString(*ctx._jsonConfig2) << std::endl;
    }
    auto nodeDef = ctx._nodeDef;
    if (nodeDef) {
        const auto &binaryAttrMap = nodeDef->binary_attrs();
        os << "binary attrs: " << std::endl;
        for (const auto &pair : binaryAttrMap) {
            os << " " << pair.first << ": <...>" << std::endl;
        }
        const auto &integerAttrMap = nodeDef->integer_attrs();
        os << "integer attrs: " << std::endl;
        for (const auto &pair : integerAttrMap) {
            os << " " << pair.first << ": " << pair.second << std::endl;
        }
    }
    return os;
}

}
