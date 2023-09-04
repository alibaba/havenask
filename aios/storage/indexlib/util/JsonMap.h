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

#include <optional>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"

namespace indexlib::util {
// TODO: autil::JsonMapWrapper
class JsonMap
{
public:
    JsonMap() = default;

public:
    template <typename T>
    static StatusOr<T> Value(const autil::legacy::json::JsonMap& jsonMap, const std::string& key);
    template <typename T>
    static std::pair<Status, T> Get(const autil::legacy::json::JsonMap& jsonMap, const std::string& key);

public:
    const autil::legacy::json::JsonMap& GetMap() const { return _jsonMap; }
    autil::legacy::json::JsonMap& GetMap() { return _jsonMap; }

    template <typename T>
    StatusOr<T> Value(const std::string& key) const;
    template <typename T>
    std::pair<Status, T> Get(const std::string& key) const;
    std::pair<bool, const autil::legacy::Any&> GetAny(const std::string& key) const;

public:
    typedef autil::legacy::json::JsonMap::const_iterator const_iterator;
    typedef autil::legacy::json::JsonMap::iterator iterator;
    typedef autil::legacy::json::JsonMap::value_type value_type;
    typedef autil::legacy::json::JsonMap::mapped_type mapped_type;
    bool empty() const { return _jsonMap.empty(); }
    const_iterator find(const std::string& key) const { return _jsonMap.find(key); }
    const_iterator begin() const { return _jsonMap.begin(); }
    const_iterator end() const { return _jsonMap.end(); }
    mapped_type& operator[](const std::string& key) { return _jsonMap.operator[](key); }

private:
    autil::legacy::json::JsonMap _jsonMap;
};

//////////////////////////////////////////////////////////////////////
template <typename T>
StatusOr<T> JsonMap::Value(const autil::legacy::json::JsonMap& jsonMap, const std::string& key)
{
    const auto& it = jsonMap.find(key);
    if (jsonMap.end() == it) {
        return {indexlib::Status::NotFound("[%s] is not exists", key.c_str())};
    }
    T value {};
    try {
        autil::legacy::FromJson(value, it->second);
    } catch (const std::exception& e) {
        return {indexlib::Status::ConfigError("parse json [%s] failed, exception[%s]", key.c_str(), e.what())};
    }
    return {value};
}

template <typename T>
StatusOr<T> JsonMap::Value(const std::string& key) const
{
    return Value<T>(_jsonMap, key);
}

template <typename T>
std::pair<Status, T> JsonMap::Get(const autil::legacy::json::JsonMap& jsonMap, const std::string& key)
{
    T value {};
    const auto& it = jsonMap.find(key);
    if (jsonMap.end() == it) {
        return {indexlib::Status::NotFound("[%s] is not exists", key.c_str()), value};
    }
    try {
        autil::legacy::FromJson(value, it->second);
    } catch (const std::exception& e) {
        return {indexlib::Status::ConfigError("parse json [%s] failed, exception[%s]", key.c_str(), e.what()), value};
    }
    return {indexlib::Status::OK(), value};
}

template <typename T>
std::pair<Status, T> JsonMap::Get(const std::string& key) const
{
    return Get<T>(_jsonMap, key);
}

inline std::pair<bool, const autil::legacy::Any&> JsonMap::GetAny(const std::string& key) const
{
    static autil::legacy::Any empty;
    auto iter = _jsonMap.find(key);
    if (iter != _jsonMap.end()) {
        return {true, iter->second};
    }
    return {false, empty};
}

} // namespace indexlib::util
