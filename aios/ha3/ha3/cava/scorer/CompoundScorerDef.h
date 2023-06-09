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

#include <float.h>
#include <stdint.h>
#include <functional>
#include "matchdoc/MatchDoc.h"

#include "ha3/isearch.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace compound_scorer {

const static std::string ITEMSEP = ";";
const static std::string KVSEP = "#";
const static std::string FIELDSEP = "|";

typedef std::function<bool(score_t &, matchdoc::MatchDoc &)> WorkerType;

#define   INVALID_INT8      INT8_MAX
#define   INVALID_INT16     INT16_MAX
#define   INVALID_INT32     INT32_MAX
#define   INVALID_INT64     INT64_MAX
#define   INVALID_UINT8     UINT8_MAX
#define   INVALID_UINT16    UINT16_MAX
#define   INVALID_UINT32    UINT32_MAX
#define   INVALID_UINT64    UINT64_MAX
#define   INVALID_FLOAT     FLT_MAX
#define   INVALID_DOUBLE    DBL_MAX

#define SCORE_MAX INT32_MAX
#define SCORE_MIN -2147483647

template<typename T>
inline T getNullValue() {
    assert(false);
    return (T)0;
}

template<>
inline int8_t getNullValue() { return INVALID_INT8; }

template<>
inline int16_t getNullValue() { return INVALID_INT16; }

template<>
inline int32_t getNullValue() { return INVALID_INT32; }

template<>
inline int64_t getNullValue() { return INVALID_INT64; }

template<>
inline uint8_t getNullValue() { return INVALID_UINT8; }

template<>
inline uint16_t getNullValue() { return INVALID_UINT16; }

template<>
inline uint32_t getNullValue() { return INVALID_UINT32; }

template<>
inline uint64_t getNullValue() { return INVALID_UINT64; }

template<>
inline float getNullValue() { return INVALID_FLOAT; }

template<>
inline double getNullValue() { return INVALID_DOUBLE; }

} // namespace compound_scorer
} // namespace isearch
