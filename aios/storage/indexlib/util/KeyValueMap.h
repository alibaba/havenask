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

#include <map>
#include <optional>
#include <string>

#include "autil/StringUtil.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"

namespace indexlib { namespace util {

typedef std::map<std::string, std::string> KeyValueMap;

inline const std::string& GetValueFromKeyValueMap(const KeyValueMap& keyValueMap, const std::string& key,
                                                  const std::string& defaultValue = "")
{
    KeyValueMap::const_iterator iter = keyValueMap.find(key);
    if (iter != keyValueMap.end()) {
        return iter->second;
    }
    return defaultValue;
}

template <typename T>
inline T GetTypeValueFromKeyValueMap(const KeyValueMap& keyValueMap, const std::string& key, T defaultValue)
{
    KeyValueMap::const_iterator iter = keyValueMap.find(key);
    if (iter == keyValueMap.end()) {
        return defaultValue;
    }

    T value {};
    if (!autil::StringUtil::fromString(iter->second, value)) {
        return defaultValue;
    }
    return value;
}

inline KeyValueMap ConvertFromJsonMap(const autil::legacy::json::JsonMap& jsonMap)
{
    KeyValueMap kvMap;
    for (const auto& [k, v] : jsonMap) {
        if (v.GetType() == typeid(std::string)) {
            kvMap[k] = autil::legacy::AnyCast<std::string>(v);
        }
    }
    return kvMap;
}

inline autil::legacy::json::JsonMap ConvertToJsonMap(const KeyValueMap& keyValueMap)
{
    autil::legacy::json::JsonMap jsonMap;
    for (const auto& [k, v] : keyValueMap) {
        jsonMap[k] = autil::legacy::ToJson(v);
    }
    return jsonMap;
}

template <typename T>
inline std::optional<T> GetTypeValueFromJsonMap(const autil::legacy::json::JsonMap& jsonMap, const std::string& key,
                                                const T& defaultValue)
{
    autil::legacy::json::JsonMap::const_iterator it = jsonMap.find(key);
    if (it == jsonMap.end()) {
        return defaultValue;
    }
    try {
        T value {};
        autil::legacy::FromJson(value, it->second);
        return value;
    } catch (...) {
        return std::nullopt;
    }
}

}} // namespace indexlib::util
