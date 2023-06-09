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

#include <memory>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/util/counter/CounterBase.h"

namespace indexlib { namespace util {

class Counter;
typedef std::shared_ptr<Counter> CounterPtr;
class MultiCounter;
typedef std::shared_ptr<MultiCounter> MultiCounterPtr;
class AccumulativeCounter;
typedef std::shared_ptr<AccumulativeCounter> AccumulativeCounterPtr;
class StateCounter;
typedef std::shared_ptr<StateCounter> StateCounterPtr;

class CounterMap
{
public:
    CounterMap();
    ~CounterMap();

public:
    static constexpr char EMPTY_COUNTER_MAP_JSON[] = "{}";

public:
    std::vector<std::shared_ptr<CounterBase>> FindCounters(const std::string& nodePath) const;
    std::string Get(const std::string& nodePath) const;

    std::shared_ptr<MultiCounter> GetMultiCounter(const std::string& nodePath);
    std::shared_ptr<AccumulativeCounter> GetAccCounter(const std::string& nodePath);
    std::shared_ptr<StateCounter> GetStateCounter(const std::string& nodePath);

    std::string ToJsonString(bool isCompact = true) const;
    void FromJsonString(const std::string& jsonString);
    void Merge(const std::string& jsonString, CounterBase::FromJsonType fromJsonType);

    std::shared_ptr<CounterBase> GetCounter(const std::string& nodePath, CounterBase::CounterType type);

private:
    void CollectSingleCounters(const std::shared_ptr<CounterBase>& current,
                               std::vector<std::shared_ptr<CounterBase>>* retCounters) const;

private:
    mutable autil::ThreadMutex _lock;
    std::shared_ptr<MultiCounter> _root;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CounterMap> CounterMapPtr;
}} // namespace indexlib::util
