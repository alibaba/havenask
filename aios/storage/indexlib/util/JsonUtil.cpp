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
#include "indexlib/util/JsonUtil.h"

#include <exception>
#include <typeinfo>

#include "autil/Log.h"
#include "autil/legacy/any.h"               // Any, AnyCast
#include "autil/legacy/json.h"              // JsonArray, JsonMap, JsonNumber, ParseJson in namespace json
#include "autil/legacy/legacy_jsonizable.h" // FromJson, ToJson, FromJsonString, ToJsonString
// #include "autil/legacy/jsonizable.h" // JsonWrapper

namespace indexlib::util {

AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.util, JsonUtil);

std::optional<autil::legacy::Any> JsonUtil::JsonStringToAny(const std::string& jsonStr) noexcept
{
    try {
        return autil::legacy::json::ParseJson(jsonStr);
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "parse json string failed[%s], content[%s]", e.what(), jsonStr.c_str());
    } catch (...) {
        AUTIL_LOG(ERROR, "parse json string failed, content[%s]", jsonStr.c_str());
    }
    return std::nullopt;
}

// refer to AnyCast in any.h
template <typename ValueType>
static const ValueType* AnyToType(const autil::legacy::Any* any) noexcept
{
    if (auto* value = autil::legacy::AnyCast<ValueType>(any); value) {
        return value;
    }
    AUTIL_LOG(ERROR, "cast from [%s] to [%s] failed, content[%s]", any->GetType().name(), typeid(ValueType).name(),
              autil::legacy::json::ToString(*any, true, "").c_str());
    return nullptr;
}

const std::string* JsonUtil::AnyToString(const autil::legacy::Any& any) noexcept
{
    return AnyToType<std::string>(&any);
}

const JsonUtil::JsonMap* JsonUtil::AnyToJsonMap(const autil::legacy::Any& any) noexcept
{
    // return AnyToType<JsonUtil::JsonMap>(&any);
    return autil::legacy::AnyCast<JsonUtil::JsonMap>(&any);
}

const JsonUtil::JsonArray* JsonUtil::AnyToJsonArray(const autil::legacy::Any& any) noexcept
{
    return AnyToType<JsonUtil::JsonArray>(&any);
}

autil::legacy::Any JsonUtil::StringToAny(const std::string& value) noexcept { return autil::legacy::ToJson(value); }

} // namespace indexlib::util
