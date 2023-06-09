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
#include "indexlib/util/counter/StringCounter.h"

#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, StringCounter);

StringCounter::StringCounter(const std::string& path) : CounterBase(path, CT_STRING) {}

StringCounter::~StringCounter() {}

void StringCounter::Set(const std::string& value)
{
    autil::ScopedLock l(_mutex);
    _value = value;
}

std::string StringCounter::Get() const
{
    autil::ScopedLock l(_mutex);
    return _value;
}

autil::legacy::Any StringCounter::ToJson() const
{
    autil::legacy::json::JsonMap jsonMap;
    jsonMap[TYPE_META] = CounterTypeToStr(_type);
    jsonMap["value"] = Get();
    return jsonMap;
}

void StringCounter::FromJson(const autil::legacy::Any& any, FromJsonType fromJsonType)
{
    autil::ScopedLock l(_mutex);
    autil::legacy::json::JsonMap jsonMap = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(any);
    auto iter = jsonMap.find("value");
    if (iter == jsonMap.end()) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "no value found in counter[%s]", _path.c_str());
    }
    _value = autil::legacy::AnyCast<std::string>(iter->second);
}

}} // namespace indexlib::util
