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

#include "indexlib/index/kkv/Constant.h"

namespace indexlibv2::index {

// TODO(chekong.ygm): need pod

#pragma pack(push, 4)
struct SKeyListInfo {
    uint32_t skeyHeader;
    uint32_t blockHeader;
    uint32_t count;

    SKeyListInfo() : skeyHeader(INVALID_SKEY_OFFSET), blockHeader(INVALID_SKEY_OFFSET), count(0) {}

    SKeyListInfo(uint32_t _skeyHeader, uint32_t _blockHeader, uint32_t _count)
        : skeyHeader(_skeyHeader)
        , blockHeader(_blockHeader)
        , count(_count)
    {
    }

    inline bool operator==(const SKeyListInfo& rhs) const
    {
        return skeyHeader == rhs.skeyHeader && blockHeader == rhs.blockHeader && count == rhs.count;
    }
};
#pragma pack(pop)

} // namespace indexlibv2::index