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

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/anet/atomic.h"

namespace multi_call {

class DiffCounter
{
public:
    DiffCounter();
    ~DiffCounter();
private:
    DiffCounter(const DiffCounter &);
    DiffCounter &operator=(const DiffCounter &);
public:
    void inc();
    int64_t snapshot();
    int64_t current() const;
    void setCurrent(int64_t value);
    std::string snapshotStr();
private:
    int64_t _snapshot;
    atomic64_t _counter;
};

}
