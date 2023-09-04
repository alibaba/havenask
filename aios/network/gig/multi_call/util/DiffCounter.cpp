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
#include "aios/network/gig/multi_call/util/DiffCounter.h"

#include "autil/StringUtil.h"

using namespace std;

namespace multi_call {

DiffCounter::DiffCounter() : _snapshot(0) {
    atomic_set(&_counter, 0);
}

DiffCounter::~DiffCounter() {
}

void DiffCounter::inc() {
    atomic_inc(&_counter);
}

int64_t DiffCounter::snapshot() {
    auto currentValue = current();
    auto diff = currentValue - _snapshot;
    _snapshot = currentValue;
    return diff;
}

int64_t DiffCounter::current() const {
    return atomic_read(&_counter);
}

void DiffCounter::setCurrent(int64_t value) {
    atomic_set(&_counter, value);
}

std::string DiffCounter::snapshotStr() {
    auto diff = snapshot();
    return "" + autil::StringUtil::toString(current()) + "(" + autil::StringUtil::toString(diff) +
           ")";
}

} // namespace multi_call
