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
#include "navi/engine/CreatorStats.h"

namespace navi {

CreatorStats::CreatorStats()
    : _createLatencyFilter(FILTER_WINDOW_SIZE)
    , _initLatencyFilter(FILTER_WINDOW_SIZE)
{
    _createLatencyFilter.setCurrent(2000);
    _initLatencyFilter.setCurrent(2000);
}

CreatorStats::~CreatorStats() {
}

void CreatorStats::updateCreateLatency(float latencyUs) {
    _createLatencyFilter.update(latencyUs);
}

void CreatorStats::updateInitLatency(float latencyUs) {
    _initLatencyFilter.update(latencyUs);
}

float CreatorStats::getCreateAvgLatency() {
    return _createLatencyFilter.output();
}

float CreatorStats::getInitAvgLatency() {
    return _initLatencyFilter.output();
}

}

