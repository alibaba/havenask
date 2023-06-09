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

#include <cstdint>
#include <limits>

namespace suez {

enum class WriterState : int
{
    LOG = 0,
    ASYNC = 1,
    SYNC = 2,
};

struct WriteResult {
    WriterState state;

    struct WatermarkInfo {
        int64_t maxCp = -1;
        int64_t buildGap = std::numeric_limits<int64_t>::min();
        int64_t buildLocatorOffset = -1;
    };
    WatermarkInfo watermark;

    void setWatermark(int64_t logOffset, int64_t buildOffset) {
        watermark.maxCp = logOffset;
        watermark.buildLocatorOffset = buildOffset;
        watermark.buildGap = logOffset - buildOffset;
    }

    struct RequestStat {
        size_t inMessageCount = 0;
        int64_t parseLatency = 0;
        int64_t processLatency = 0;
        int64_t formatLogLatency = 0;
        int64_t logLatency = 0;
        int64_t buildLatency = 0;
    };
    RequestStat stat;
};

} // namespace suez
