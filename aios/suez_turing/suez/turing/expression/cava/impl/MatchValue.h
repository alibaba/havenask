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

class CavaCtx;

namespace ha3 {
class MatchValue {
public:
    MatchValue();
    ~MatchValue();

public:
    int8_t GetInt8(CavaCtx *ctx);

    uint8_t GetUint8(CavaCtx *ctx);

    int16_t GetInt16(CavaCtx *ctx);

    uint16_t GetUint16(CavaCtx *ctx);

    int32_t GetInt32(CavaCtx *ctx);

    uint32_t GetUint32(CavaCtx *ctx);

    float GetFloat(CavaCtx *ctx);
};

} // namespace ha3
