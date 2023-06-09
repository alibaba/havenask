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
#include <memory>

#include "autil/Log.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/Counter.h"
#include "indexlib/util/counter/CounterBase.h"
#include "indexlib/util/counter/StateCounter.h"

namespace indexlib { namespace util {

class MultiCounter;
typedef std::shared_ptr<MultiCounter> MultiCounterPtr;

class MultiCounter : public CounterBase
{
public:
    MultiCounter(const std::string& path);
    ~MultiCounter();

public:
    CounterBasePtr GetCounter(const std::string& name) const;
    CounterBasePtr CreateCounter(const std::string& name, CounterType type);

    const std::map<std::string, CounterBasePtr>& GetCounterMap() const { return _counterMap; }

public:
    inline MultiCounterPtr CreateMultiCounter(const std::string& name)
    {
        return std::dynamic_pointer_cast<MultiCounter>(CreateCounter(name, CounterBase::CT_DIRECTORY));
    }
    inline AccumulativeCounterPtr CreateAccCounter(const std::string& name)
    {
        return std::dynamic_pointer_cast<AccumulativeCounter>(CreateCounter(name, CounterBase::CT_ACCUMULATIVE));
    }
    inline StateCounterPtr CreateStateCounter(const std::string& name)
    {
        return std::dynamic_pointer_cast<StateCounter>(CreateCounter(name, CounterBase::CT_STATE));
    }
    size_t Sum();

public:
    autil::legacy::Any ToJson() const override;
    void FromJson(const autil::legacy::Any& any, FromJsonType fromJsonType) override;

private:
    std::string GetFullPath(const std::string& name) const { return _path.empty() ? name : _path + "." + name; }

private:
    std::map<std::string, CounterBasePtr> _counterMap;

private:
    AUTIL_LOG_DECLARE();
};
}} // namespace indexlib::util
