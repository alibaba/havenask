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
#include "suez/turing/expression/cava/impl/MatchValue.h"

#include "indexlib/indexlib.h"

class CavaCtx;

namespace ha3 {
MatchValue::MatchValue() {}
MatchValue::~MatchValue() {}

int8_t MatchValue::GetInt8(CavaCtx *ctx) { return ((matchvalue_t *)this)->GetInt8(); }

uint8_t MatchValue::GetUint8(CavaCtx *ctx) { return ((matchvalue_t *)this)->GetUint8(); }

int16_t MatchValue::GetInt16(CavaCtx *ctx) { return ((matchvalue_t *)this)->GetInt16(); }

uint16_t MatchValue::GetUint16(CavaCtx *ctx) { return ((matchvalue_t *)this)->GetUint16(); }

int32_t MatchValue::GetInt32(CavaCtx *ctx) { return ((matchvalue_t *)this)->GetInt32(); }

uint32_t MatchValue::GetUint32(CavaCtx *ctx) { return ((matchvalue_t *)this)->GetUint32(); }

float MatchValue::GetFloat(CavaCtx *ctx) { return ((matchvalue_t *)this)->GetFloat(); }
} // namespace ha3
