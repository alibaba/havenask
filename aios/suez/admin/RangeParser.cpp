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
#include "suez/admin/RangeParser.h"

#include "autil/StringUtil.h"
#include "suez/sdk/JsonNodeRef.h"

namespace suez {

std::pair<uint32_t, uint32_t> RangeParser::parse(const std::string &range) {
    auto rangeVec = autil::StringUtil::split(range, "_");
    if (rangeVec.size() != 2) {
        FORMAT_THROW("invalid range %s", range.c_str());
    }
    uint32_t from, to;
    if (!autil::StringUtil::fromString(rangeVec[0], from)) {
        FORMAT_THROW("invalid range %s", range.c_str());
    }
    if (!autil::StringUtil::fromString(rangeVec[1], to)) {
        FORMAT_THROW("invalid range %s", range.c_str());
    }
    return std::make_pair(from, to);
}

} // namespace suez
