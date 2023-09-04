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

#include <assert.h>
#include <stdint.h>

namespace autil {
class DataBuffer;
} // namespace autil

namespace suez {
namespace turing {

class EvaluateBitmap {
public:
    EvaluateBitmap() : _evaulatedFlag(0) {}
    void setBit(uint32_t idx) {
        assert(idx < sizeof(*this) * 8);
        _evaulatedFlag |= (1ull << idx);
    }
    bool getBit(uint32_t idx) const {
        assert(idx < sizeof(*this) * 8);
        return (_evaulatedFlag & (1ull << idx));
    }
    void serialize(autil::DataBuffer &dataBuffer) const { assert(false); }
    void deserialize(autil::DataBuffer &dataBuffer) { assert(false); }

private:
    uint32_t _evaulatedFlag;
};

} // namespace turing
} // namespace suez
