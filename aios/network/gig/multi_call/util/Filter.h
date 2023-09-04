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
#ifndef ISEARCH_MULTI_CALL_FILTER_H
#define ISEARCH_MULTI_CALL_FILTER_H

#include "aios/network/anet/atomic.h"
#include "aios/network/gig/multi_call/common/ControllerParam.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/util/CompareFlag.h"

namespace multi_call {

class Filter
{
public:
    Filter(int32_t windowSize);
    ~Filter();

private:
    Filter(const Filter &);
    Filter &operator=(const Filter &);

public:
    bool update(float input, float truncate = INVALID_FILTER_VALUE);
    bool isValid() const;
    float output() const;
    int64_t count() const;
    void truncate(float value);
    void setCurrent(float current);
    void setCounter(int64_t counter);
    int32_t getWindowSize() const;
    CompareFlag compare(const Filter &rhs) const;
    float getAlpha() const;
    void reset();
    std::string toString() const;
    void setMinValue(float minValue);

private:
    int32_t _windowSize;
    float _alpha;
    float _minValue;
    volatile FloatIntUnion _current;
    atomic64_t _filterCounter;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(Filter);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_FILTER_H
