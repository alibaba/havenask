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

#include <bitset>
#include <memory> // IWYU pragma: keep
#include <stddef.h>
#include <stdint.h>

#define SWIFT_TYPEDEF_PTR(x) typedef std::shared_ptr<x> x##Ptr

#define SWIFT_DELETE(x)                                                                                                \
    do {                                                                                                               \
        if (x) {                                                                                                       \
            delete x;                                                                                                  \
            x = NULL;                                                                                                  \
        }                                                                                                              \
    } while (0)

#define SWIFT_DELETE_ARRAY(x)                                                                                          \
    do {                                                                                                               \
        if (x) {                                                                                                       \
            delete[] x;                                                                                                \
            x = NULL;                                                                                                  \
        }                                                                                                              \
    } while (0)

// common variable

namespace swift {
namespace common {

constexpr int64_t DEFAULT_POST_TIMEOUT = 30 * 1000;
constexpr int64_t MAX_MESSAGE_SIZE = 1 << 26; // 64M
constexpr char TRACING_MESSAGE_TOPIC_NAME[] = "BUILDIN_TOPIC_FOR_TRACE_MESSAGE__";
constexpr uint32_t MAX_MSG_TRACING_MASK = 250;    // [250-255] for other use
constexpr uint32_t MAX_MSG_TRACING_RANGE = 65530; // [65530-65535] for other use
constexpr uint64_t UINT64_FULL_SET = 0xFFFFFFFFFFFFFFFF;
constexpr uint64_t UINT64_FULL_LSH_1 = 0xFFFFFFFFFFFFFFFE;
constexpr int32_t OFFSET_IN_MERGE_MSG_BASE = 1 << 24;
constexpr char DEFAULT_METRIC_ACCESSID[] = "anonymous";

template <int N>
class DependMatcher {};

// define for module init once
#define INIT_ONCE_FUNC_1
#define INIT_ONCE_FUNC_2
#define INIT_ONCE_FUNC_3
#define INIT_ONCE_FUNC_4
#define INIT_ONCE_FUNC_5
#define INIT_ONCE_FUNC_6
#define INIT_ONCE_FUNC_7
#define INIT_ONCE_FUNC_8

enum RegisterType : int32_t {
    RT_MODULE = 0,
    RT_DATA = 1,
};

using LoadStatus = std::bitset<6>;

} // namespace common
} // namespace swift
