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
#include <string>
#include <vector>

namespace indexlib { namespace file_system {

struct BlockCacheMetrics {
    std::string name;
    uint64_t totalAccessCount;
    uint64_t totalHitCount;
    uint32_t last1000HitCount;

    BlockCacheMetrics() : totalAccessCount(0), totalHitCount(0), last1000HitCount(0) {}
};
typedef std::vector<BlockCacheMetrics> BlockCacheMetricsVec;
}} // namespace indexlib::file_system
