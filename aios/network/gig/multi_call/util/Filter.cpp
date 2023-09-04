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
#include "aios/network/gig/multi_call/util/Filter.h"

#include <assert.h>
#include <math.h>

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, Filter);

Filter::Filter(int32_t windowSize) : _windowSize(windowSize), _minValue(0.0f) {
    assert(_windowSize > 0);
    reset();
    // the magic number is ln(0.15)
    constexpr float ln15 = -1.897120f;
    if (unlikely(_windowSize > 100000)) {
        _windowSize = 100000;
    }
    _alpha = 1.0f - ::exp(ln15 / _windowSize);
}

Filter::~Filter() {
}

bool Filter::update(float input, float truncate) {
    if (likely(input >= _minValue)) {
        FloatIntUnion current;
        FloatIntUnion newValue;
        do {
            current.f = _current.f;
            newValue.f = std::min(_alpha * input + (1.0f - _alpha) * current.f, truncate);
        } while (!cmpxchg(&_current.i, current.i, newValue.i));
        atomic_inc(&_filterCounter);
    }
    return isValid();
}

float Filter::getAlpha() const {
    return _alpha;
}

bool Filter::isValid() const {
    return atomic_read(&_filterCounter) >= _windowSize;
}

float Filter::output() const {
    return _current.f;
}

CompareFlag Filter::compare(const Filter &rhs) const {
    int32_t sum = isValid() | (rhs.isValid() << 1u);
    switch (sum) {
    case 2:
        return WorseCompareFlag;
    case 1:
        return BetterCompareFlag;
    case 3:
        return CompareFlag(output() < rhs.output());
    case 0:
    default:
        return UnknownCompareFlag;
    }
}

int64_t Filter::count() const {
    return atomic_read(&_filterCounter);
}

void Filter::truncate(float value) {
    if (_current.f > value) {
        _current.f = value;
    }
}

void Filter::setCurrent(float current) {
    _current.f = current;
}

void Filter::setCounter(int64_t counter) {
    atomic_set(&_filterCounter, counter);
}

void Filter::setMinValue(float minValue) {
    _minValue = minValue;
}

int32_t Filter::getWindowSize() const {
    return _windowSize;
}

void Filter::reset() {
    setCounter(0);
    setCurrent(0.0f);
}

std::string Filter::toString() const {
    return StringUtil::toString(output()) + " O " + StringUtil::toString(count()) + " C " +
           StringUtil::toString(getWindowSize()) + " W";
}

} // namespace multi_call
