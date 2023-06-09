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
#include "indexlib/util/counter/MultiCounter.h"

#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/counter/CounterCreator.h"

using namespace std;
using namespace autil::legacy;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, MultiCounter);

MultiCounter::MultiCounter(const string& path) : CounterBase(path, CT_DIRECTORY) {}

MultiCounter::~MultiCounter() {}

CounterBasePtr MultiCounter::GetCounter(const std::string& name) const
{
    auto iter = _counterMap.find(name);
    if (iter == _counterMap.end()) {
        return CounterBasePtr();
    }
    return iter->second;
}

CounterBasePtr MultiCounter::CreateCounter(const std::string& name, CounterType type)
{
    assert(name.find('.') == string::npos);
    assert(!GetCounter(name));
    assert(type != CT_UNKNOWN);
    auto counter = CounterCreator::CreateCounter(GetFullPath(name), type);
    _counterMap[name] = counter;
    return counter;
}

autil::legacy::Any MultiCounter::ToJson() const
{
    autil::legacy::json::JsonMap jsonMap;
    jsonMap[TYPE_META] = CounterTypeToStr(_type);
    for (const auto& item : _counterMap) {
        jsonMap[item.first] = item.second->ToJson();
    }
    return jsonMap;
}

void MultiCounter::FromJson(const autil::legacy::Any& any, FromJsonType fromJsonType)
{
    json::JsonMap jsonMap = AnyCast<json::JsonMap>(any);
    for (const auto& item : jsonMap) {
        if (item.first == TYPE_META) {
            auto counterType = StrToCounterType(AnyCast<string>(item.second));
            if (counterType != CT_DIRECTORY) {
                INDEXLIB_FATAL_ERROR(InconsistentState,
                                     "counter[%s], type[%u] fromJson failed due to inconsistent type [%u]",
                                     _path.c_str(), CT_DIRECTORY, counterType);
            }
            continue;
        }
        CounterBasePtr counter;
        try {
            json::JsonMap jsonMap = AnyCast<json::JsonMap>(item.second);
            auto iter = jsonMap.find(TYPE_META);
            if (iter == jsonMap.end()) {
                INDEXLIB_FATAL_ERROR(InconsistentState, "no %s found in counter[%s]", TYPE_META.c_str(),
                                     item.first.c_str());
            }
            auto counterType = StrToCounterType(AnyCast<string>(iter->second));
            string counterFullPath = GetFullPath(item.first);
            if (counterType == CT_UNKNOWN) {
                INDEXLIB_FATAL_ERROR(InconsistentState, "invalid counter type defined for [%s]",
                                     counterFullPath.c_str());
            }

            auto counterIter = _counterMap.find(item.first);
            if (counterIter == _counterMap.end()) {
                counter = CounterCreator::CreateCounter(counterFullPath, counterType);
                _counterMap[item.first] = counter;
            } else {
                counter = counterIter->second;
                if (counter->GetType() != counterType) {
                    INDEXLIB_FATAL_ERROR(InconsistentState,
                                         "merge counter[%s], type[%u] failed due to inconsistent type [%u]",
                                         _path.c_str(), counter->GetType(), counterType);
                }
            }

            counter->FromJson(item.second, fromJsonType);
        } catch (const autil::legacy::BadAnyCast& e) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "jsonize for multiCounter[%s] failed! exception[%s]", _path.c_str(),
                                 e.what());
        }
    }
}

size_t MultiCounter::Sum()
{
    size_t total = 0;
    for (auto iter = _counterMap.begin(); iter != _counterMap.end(); ++iter) {
        CounterPtr counter = std::dynamic_pointer_cast<Counter>(iter->second);
        if (counter) {
            total += counter->Get();
            continue;
        }

        MultiCounterPtr multiCounter = std::dynamic_pointer_cast<MultiCounter>(iter->second);
        if (multiCounter) {
            total += multiCounter->Sum();
        }
    }
    return total;
}
}} // namespace indexlib::util
