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

#include <cstdint>

namespace indexlibv2::index {

struct ValueMeta {
    uint32_t valueCount = 0;
    uint32_t valueCountAfterUnique = 0;
    // TODO: add some meta information such as sum/min/max for some specified field
};

struct KeyMeta {
    uint64_t key = 0;
    uint64_t offset = 0;
    ValueMeta valueMeta;

    // requires no duplicate keys
    bool operator<(const KeyMeta& other) const { return key < other.key; }
};

} // namespace indexlibv2::index
