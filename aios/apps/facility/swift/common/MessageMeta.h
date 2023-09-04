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
namespace common {

struct MergedMessageMeta {
    MergedMessageMeta() : endPos(0), isCompress(false), reserved1(0), maskPayload(0), payload(0), reserved2(0) {}
    uint64_t endPos      : 30;
    bool isCompress      : 1;
    bool reserved1       : 1;
    uint64_t maskPayload : 8;
    uint64_t payload     : 16;
    uint64_t reserved2   : 8;
};

} // namespace common
} // namespace swift
