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

#include <memory>
#include <stdint.h>

#include "autil/Log.h" // IWYU pragma: keep
#include "matchdoc/Trait.h"

namespace suez {
namespace turing {

class SimpleMatchData {
public:
    SimpleMatchData();
    ~SimpleMatchData();

private:
    SimpleMatchData(const SimpleMatchData &) = delete;
    SimpleMatchData &operator=(const SimpleMatchData &) = delete;

public:
    // return need bit count.
    static uint32_t sizeOf(uint32_t termCount) {
        if (termCount == 0) {
            return 0;
        }
        return ((termCount - 1) / (sizeof(uint32_t) * 8) + 1) * sizeof(uint32_t);
    }

private:
    static const uint32_t SLOT_TAIL = 0x1f;

public:
    bool isMatch(uint32_t idx) const {
        uint32_t s = idx >> 5;
        uint32_t f = idx & SLOT_TAIL;
        return _matchBuffer[s] & (1 << f);
    }

public:
    void setMatch(uint32_t idx, bool isMatch) {
        uint32_t s = idx >> 5;
        uint32_t f = idx & SLOT_TAIL;
        if (isMatch) {
            _matchBuffer[s] |= (1 << f);
        } else {
            _matchBuffer[s] &= ~(1 << f);
        }
    }

private:
    uint32_t _matchBuffer[1];

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SimpleMatchData> SimpleMatchDataPtr;

} // namespace turing
} // namespace suez
