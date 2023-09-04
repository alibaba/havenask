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
class MChar;
class MatchDoc;
} // namespace ha3

namespace unsafe {

class Reference {
public:
#define SET_SINGLE_VALUE_WITH_TYPE(name, type) void set##name(CavaCtx *ctx, ha3::MatchDoc *doc, type val);

    SET_SINGLE_VALUE_WITH_TYPE(Int8, int8_t);
    SET_SINGLE_VALUE_WITH_TYPE(Int16, int16_t);
    SET_SINGLE_VALUE_WITH_TYPE(Int32, int32_t);
    SET_SINGLE_VALUE_WITH_TYPE(Int64, int64_t);
    SET_SINGLE_VALUE_WITH_TYPE(UInt8, uint8_t);
    SET_SINGLE_VALUE_WITH_TYPE(UInt16, uint16_t);
    SET_SINGLE_VALUE_WITH_TYPE(UInt32, uint32_t);
    SET_SINGLE_VALUE_WITH_TYPE(UInt64, uint64_t);
    SET_SINGLE_VALUE_WITH_TYPE(Float, float);
    SET_SINGLE_VALUE_WITH_TYPE(Double, double);
    SET_SINGLE_VALUE_WITH_TYPE(MChar, ha3::MChar *);
    SET_SINGLE_VALUE_WITH_TYPE(STLString, ha3::MChar *);
#undef SET_SINGLE_VALUE_WITH_TYPE
};

} // namespace unsafe
