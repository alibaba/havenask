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
#include <unordered_map>

#include "autil/AtomicCounter.h"
#include "indexlib/common_define.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace index {

class IndexMetricsBase
{
public:
    typedef util::AccessCounterMap AccessCounterMap;

public:
    IndexMetricsBase() {}
    virtual ~IndexMetricsBase() {}

public:
    const AccessCounterMap& GetAccessCounters() const { return mAccessCounters; }
    void IncAccessCounter(const std::string& indexName)
    {
        AccessCounterMap::iterator iter = mAccessCounters.find(indexName);
        if (iter != mAccessCounters.end() && iter->second.get() != nullptr) {
            iter->second->Increase(1);
        }
    }
    void AddAccessCounter(const std::string& indexName, const util::AccumulativeCounterPtr& counter)
    {
        if (mAccessCounters.count(indexName) == 0) {
            mAccessCounters.insert({indexName, counter});
        }
    }

    void IncAccessCounter(const std::string& field, index::TemperatureProperty property, size_t count)
    {
        std::string key;
        switch (property) {
        case TemperatureProperty::HOT:
            key = field + ".HOT";
            break;
        case TemperatureProperty::WARM:
            key = field + ".WARM";
            break;
        case TemperatureProperty::COLD:
            key = field + ".COLD";
            break;
        default:
            key = field;
        }
        AccessCounterMap::iterator iter = mAccessCounters.find(key);
        if (iter != mAccessCounters.end() && iter->second.get() != nullptr) {
            iter->second->Increase(count);
        }
    }

private:
    AccessCounterMap mAccessCounters;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexMetricsBase);
}} // namespace indexlib::index
