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

namespace indexlibv2::base {

struct Progress {
    Progress(int64_t offset) : Progress(0, 65535, offset) {}
    Progress() : Progress(0, 65535, -1) {}
    Progress(uint32_t from, uint32_t to, int64_t offset) : offset(offset), from(from), to(to) {}
    int64_t offset;
    uint32_t from;
    uint32_t to;
    bool operator==(const Progress& other) const
    {
        return offset == other.offset && from == other.from && to == other.to;
    }
};

} // namespace indexlibv2::base
