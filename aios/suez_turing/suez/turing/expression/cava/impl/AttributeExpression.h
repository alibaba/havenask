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
class MDouble;
class MFloat;
class MInt16;
class MInt32;
class MInt64;
class MInt8;
class MString;
class MUInt16;
class MUInt32;
class MUInt64;
class MUInt8;
class MatchDoc;
} // namespace ha3

namespace unsafe {

class AttributeExpression {
public:
    AttributeExpression() {}
    virtual ~AttributeExpression() {}

private:
    AttributeExpression(const AttributeExpression &);
    AttributeExpression &operator=(const AttributeExpression &);

public:
#define GET_SINGLE_VALUE_WITH_TYPE(name, type) type get##name(CavaCtx *ctx, ha3::MatchDoc *doc);

    GET_SINGLE_VALUE_WITH_TYPE(Boolean, bool);
    GET_SINGLE_VALUE_WITH_TYPE(Int8, int8_t);
    GET_SINGLE_VALUE_WITH_TYPE(Int16, int16_t);
    GET_SINGLE_VALUE_WITH_TYPE(Int32, int32_t);
    GET_SINGLE_VALUE_WITH_TYPE(Int64, int64_t);
    GET_SINGLE_VALUE_WITH_TYPE(UInt8, uint8_t);
    GET_SINGLE_VALUE_WITH_TYPE(UInt16, uint16_t);
    GET_SINGLE_VALUE_WITH_TYPE(UInt32, uint32_t);
    GET_SINGLE_VALUE_WITH_TYPE(UInt64, uint64_t);
    GET_SINGLE_VALUE_WITH_TYPE(Float, float);
    GET_SINGLE_VALUE_WITH_TYPE(Double, double);
    GET_SINGLE_VALUE_WITH_TYPE(MChar, ha3::MChar *);
#undef GET_SINGLE_VALUE_WITH_TYPE

#define GET_MULTI_VALUE_WITH_TYPE(type) ha3::type *get##type(CavaCtx *ctx, ha3::MatchDoc *doc, ha3::type *result);

    GET_MULTI_VALUE_WITH_TYPE(MInt8);
    GET_MULTI_VALUE_WITH_TYPE(MInt16);
    GET_MULTI_VALUE_WITH_TYPE(MInt32);
    GET_MULTI_VALUE_WITH_TYPE(MInt64);
    GET_MULTI_VALUE_WITH_TYPE(MUInt8);
    GET_MULTI_VALUE_WITH_TYPE(MUInt16);
    GET_MULTI_VALUE_WITH_TYPE(MUInt32);
    GET_MULTI_VALUE_WITH_TYPE(MUInt64);
    GET_MULTI_VALUE_WITH_TYPE(MFloat);
    GET_MULTI_VALUE_WITH_TYPE(MDouble);
    GET_MULTI_VALUE_WITH_TYPE(MString);
#undef GET_MULTI_VALUE_WITH_TYPE
};

} // namespace unsafe
