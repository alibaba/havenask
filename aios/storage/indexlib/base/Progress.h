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
#include <utility>
#include <vector>

namespace indexlibv2::base {

// TODO(tianxiao) multiprogress && progressvec
struct Progress {
    using Offset = std::pair<int64_t, uint32_t>;
    static constexpr Offset INVALID_OFFSET = {-1, 0};
    static constexpr Offset MIN_OFFSET = {0, 0};
    Progress(const Offset& offset) : Progress(0, 65535, offset) {}
    Progress() : Progress(0, 65535, INVALID_OFFSET) {}
    Progress(uint32_t from, uint32_t to, const Offset& offset) : from(from), to(to), offset(offset) {}
    uint32_t from;
    uint32_t to;
    Offset offset;
    bool operator==(const Progress& other) const
    {
        return offset == other.offset && from == other.from && to == other.to;
    }
};

typedef std::vector<Progress> ProgressVector;
typedef std::vector<ProgressVector> MultiProgress;

} // namespace indexlibv2::base
