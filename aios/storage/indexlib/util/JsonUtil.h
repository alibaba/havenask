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
#include "autil/legacy/any.h"
#include "autil/legacy/json.h"

namespace indexlib::util {

// the main purpose is show how to make a json conversion, so you don't have to use this facility.
class JsonUtil
{
public:
    using JsonMap = autil::legacy::json::JsonMap;
    using JsonArray = autil::legacy::json::JsonArray;

public:
    // parse json format string to any, return nullptr if the value is invalid
    static std::optional<autil::legacy::Any> JsonStringToAny(const std::string& jsonStr) noexcept;

public:
    // any to value, return nullptr if the type is mismatch
    static const std::string* AnyToString(const autil::legacy::Any& any) noexcept;
    static const JsonMap* AnyToJsonMap(const autil::legacy::Any& any) noexcept;
    static const JsonArray* AnyToJsonArray(const autil::legacy::Any& any) noexcept;

    // value to any
    static autil::legacy::Any StringToAny(const std::string& value) noexcept;
};

} // namespace indexlib::util
