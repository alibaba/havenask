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

#include "autil/Log.h"
#include "autil/ThreadLocal.h"
#include "indexlib/util/counter/CounterBase.h"

namespace indexlib { namespace util {

class Counter : public CounterBase
{
public:
    Counter(const std::string& path, CounterType type);
    virtual ~Counter();

public:
    virtual int64_t Get() const = 0;

public:
    autil::legacy::Any ToJson() const override;
    void FromJson(const autil::legacy::Any& any, FromJsonType fromJsonType) override;

protected:
    // AccumulativeCounter: Sum of thread-specific values  will be
    // added to the _mergedSum when thread terminates or in ThreadLocalPtr's destructor.
    // StatusCounter: _mergedSum is the final value since there are no thread-specific values
    std::atomic_int_fast64_t _mergedSum;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<Counter> CounterPtr;
}} // namespace indexlib::util
