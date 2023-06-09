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
#include "indexlib/util/counter/Counter.h"

namespace indexlib { namespace util {

class StateCounter : public Counter
{
public:
    StateCounter(const std::string& path);
    ~StateCounter();

private:
    StateCounter(const StateCounter&);
    StateCounter& operator=(const StateCounter&);

public:
    void Set(int64_t value) { _mergedSum.store(value, std::memory_order_relaxed); }

    int64_t Get() const override final { return _mergedSum.load(std::memory_order_relaxed); }

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<StateCounter> StateCounterPtr;
}} // namespace indexlib::util
