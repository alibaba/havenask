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
#include "indexlib/util/counter/CounterMap.h"

#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/Counter.h"
#include "indexlib/util/counter/MultiCounter.h"
#include "indexlib/util/counter/StateCounter.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

using namespace indexlib::util;
namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, CounterMap);

CounterMap::CounterMap() : _root(new MultiCounter("")) {}

CounterMap::~CounterMap() {}

std::string CounterMap::Get(const std::string& nodePath) const
{
    ScopedLock l(_lock);
    vector<string> pathVec = StringUtil::split(nodePath, ".", false);
    CounterBasePtr current = _root;
    const string INVALID_RESULT = "";
    for (const auto& name : pathVec) {
        MultiCounterPtr multiCounter = std::dynamic_pointer_cast<MultiCounter>(current);
        if (!multiCounter) {
            return INVALID_RESULT;
        }
        current = multiCounter->GetCounter(name);
    }
    if (!current) {
        return INVALID_RESULT;
    }
    return autil::legacy::json::ToString(current->ToJson());
}

CounterBasePtr CounterMap::GetCounter(const string& nodePath, CounterBase::CounterType type)
{
    ScopedLock l(_lock);
    vector<string> pathVec = StringUtil::split(nodePath, ".", false);
    CounterBasePtr current = _root;
    for (size_t i = 0; i < pathVec.size(); ++i) {
        const auto& name = pathVec[i];
        MultiCounterPtr multiCounter = std::dynamic_pointer_cast<MultiCounter>(current);
        if (!multiCounter) {
            return CounterPtr();
        }
        current = multiCounter->GetCounter(name);
        if (i == pathVec.size() - 1) {
            if (!current) {
                current = multiCounter->CreateCounter(name, type);
            }
            return current;
        } else {
            if (!current) {
                current = multiCounter->CreateCounter(name, CounterBase::CT_DIRECTORY);
            }
        }
    }
    return CounterPtr();
}

MultiCounterPtr CounterMap::GetMultiCounter(const std::string& nodePath)
{
    return std::dynamic_pointer_cast<MultiCounter>(GetCounter(nodePath, CounterBase::CT_DIRECTORY));
}

AccumulativeCounterPtr CounterMap::GetAccCounter(const std::string& nodePath)
{
    return std::dynamic_pointer_cast<AccumulativeCounter>(GetCounter(nodePath, CounterBase::CT_ACCUMULATIVE));
}

StateCounterPtr CounterMap::GetStateCounter(const std::string& nodePath)
{
    return std::dynamic_pointer_cast<StateCounter>(GetCounter(nodePath, CounterBase::CT_STATE));
}

std::vector<CounterBasePtr> CounterMap::FindCounters(const std::string& nodePath) const
{
    ScopedLock l(_lock);
    vector<string> pathVec = StringUtil::split(nodePath, ".", true);
    vector<CounterBasePtr> retCounters;
    MultiCounterPtr parent = _root;
    CounterBasePtr current = parent;

    for (size_t i = 0; parent && i < pathVec.size(); ++i) {
        current = parent->GetCounter(pathVec[i]);
        if (!current) {
            break;
        }

        if (i < pathVec.size() - 1) {
            parent = std::dynamic_pointer_cast<MultiCounter>(current);
            current.reset();
        }
    }

    if (!current) {
        return retCounters;
    }

    CollectSingleCounters(current, &retCounters);
    return retCounters;
}

void CounterMap::CollectSingleCounters(const CounterBasePtr& current, std::vector<CounterBasePtr>* retCounters) const
{
    if (!current) {
        return;
    }
    MultiCounterPtr multiCounter = std::dynamic_pointer_cast<MultiCounter>(current);
    if (multiCounter) {
        const auto& subCounterMap = multiCounter->GetCounterMap();
        for (auto& kv : subCounterMap) {
            CollectSingleCounters(kv.second, retCounters);
        }
        return;
    }
    retCounters->push_back(current);
    return;
}

std::string CounterMap::ToJsonString(bool isCompact) const
{
    ScopedLock l(_lock);
    return autil::legacy::json::ToString(_root->ToJson(), isCompact);
}

void CounterMap::FromJsonString(const std::string& jsonString)
{
    ScopedLock l(_lock);
    _root.reset(new MultiCounter(""));
    json::JsonMap jsonMap;
    autil::legacy::FromJsonString(jsonMap, jsonString);
    _root->FromJson(jsonMap, CounterBase::FJT_OVERWRITE);
}

void CounterMap::Merge(const std::string& jsonString, CounterBase::FromJsonType fromJsonType)
{
    ScopedLock l(_lock);
    if (!_root) {
        _root.reset(new MultiCounter(""));
    }
    json::JsonMap jsonMap;
    autil::legacy::FromJsonString(jsonMap, jsonString);
    _root->FromJson(jsonMap, fromJsonType);
}
}} // namespace indexlib::util
