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

#include <stdint.h>

namespace navi {

struct NaviSnapshotStat {
    size_t activeThreadCount = 0;
    size_t activeThreadQueueSize = 0;
    size_t processingCount = 0;
    size_t queueCount = 0;
    size_t processingCountRatio = 0;
    size_t queueCountRatio = 0;
};

}
