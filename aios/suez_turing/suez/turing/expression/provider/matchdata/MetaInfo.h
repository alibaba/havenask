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
#include <algorithm>
#include <stdint.h>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "suez/turing/expression/provider/TermMetaInfo.h"

namespace suez {
namespace turing {

class MetaInfo {
public:
    MetaInfo(uint32_t reserveSize = DEFAULT_RESERVE_SIZE) { _termMetaInfos.reserve(reserveSize); }
    ~MetaInfo() {}

public:
    uint32_t size() const { return (uint32_t)(_termMetaInfos.size()); }

    const TermMetaInfo &operator[](uint32_t idx) const { return _termMetaInfos[idx]; }

    void pushBack(const TermMetaInfo &termMetaInfo) { _termMetaInfos.push_back(termMetaInfo); }

    void setPartTotalDocCount(int64_t docCount) { _partTotalDocCount = docCount; }
    int64_t getPartTotalDocCount() { return _partTotalDocCount; }

private:
    std::vector<TermMetaInfo> _termMetaInfos;
    static const uint32_t DEFAULT_RESERVE_SIZE = 4;
    int64_t _partTotalDocCount;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
