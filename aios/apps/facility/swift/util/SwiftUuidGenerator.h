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

namespace swift {
namespace util {

struct RequestId {

    uint64_t timestampMs : 40; // 40 bit timestamp, ms
    uint64_t partid      : 8;  // 8 bit, partition id
    uint64_t logFlag     : 1;
    uint64_t seq         : 8; // 8 bit, sequence
    uint64_t reserved    : 7;
};

union IdData {
    RequestId requestId;
    uint64_t id;
};

class SwiftUuidGenerator {
public:
    SwiftUuidGenerator(){};

public:
    static uint64_t genRequestUuid(uint64_t timestampMs, uint16_t partId, bool logFlag, uint64_t seq) {
        IdData data;
        data.id = 0;
        data.requestId.timestampMs = timestampMs;
        data.requestId.partid = partId % 256;
        data.requestId.logFlag = logFlag ? 1 : 0;
        data.requestId.seq = seq % 256;
        return data.id;
    }
};

} // namespace util
} // namespace swift
