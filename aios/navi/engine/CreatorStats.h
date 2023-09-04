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

#include "navi/common.h"
#include <multi_call/util/Filter.h>

namespace navi {

class CreatorStats
{
public:
    CreatorStats();
    ~CreatorStats();
private:
    CreatorStats(const CreatorStats &);
    CreatorStats &operator=(const CreatorStats &);
public:
    void updateCreateLatency(float latencyUs);
    void updateInitLatency(float latencyUs);
    float getCreateAvgLatency();
    float getInitAvgLatency();
private:
    multi_call::Filter _createLatencyFilter;
    multi_call::Filter _initLatencyFilter;
private:
    static constexpr int32_t FILTER_WINDOW_SIZE = 50;
};

}

