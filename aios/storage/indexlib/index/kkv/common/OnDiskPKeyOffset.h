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

#include <cassert>
#include <cstdint>
#include <type_traits>

namespace indexlibv2::index {

// TODO(chekong.ygm): need pod

struct OnDiskPKeyOffset {
    uint64_t chunkOffset   : 40;
    uint64_t inChunkOffset : 12;
    uint64_t _reserved     : 10; // 保留字段占位，用于索引兼容
    uint64_t blockHint     : 2;

    OnDiskPKeyOffset() : chunkOffset(INVALID_PKEY_CHUNK_OFFSET), inChunkOffset(0), _reserved(0), blockHint(0) {}

    OnDiskPKeyOffset(uint64_t _chunkOffset, uint64_t _inChunkOffset)
        : chunkOffset(_chunkOffset)
        , inChunkOffset(_inChunkOffset)
        , _reserved(0)
    {
    }

    OnDiskPKeyOffset(uint64_t value)
    {
        static_assert(sizeof(*this) == sizeof(value), "to guarantee equal size");
        void* addr = &value;
        *this = *((OnDiskPKeyOffset*)addr);
    }

    inline bool operator==(const OnDiskPKeyOffset& rhs) const
    {
        return chunkOffset == rhs.chunkOffset && inChunkOffset == rhs.inChunkOffset && _reserved == rhs._reserved;
    }

    inline bool operator<(const OnDiskPKeyOffset& other) const
    {
        if (chunkOffset != other.chunkOffset) {
            return chunkOffset < other.chunkOffset;
        }
        return inChunkOffset < other.inChunkOffset;
    }

    bool IsValidOffset() const { return chunkOffset != INVALID_PKEY_CHUNK_OFFSET; }

    uint64_t ToU64Value() const { return *((uint64_t*)this); }

    void SetBlockHint(uint64_t offset) { blockHint = CalculateBlockHint(chunkOffset, offset); }

    uint64_t GetBlockOffset() const { return chunkOffset - chunkOffset % HINT_BLOCK_SIZE; }

    uint64_t GetHintSize() const { return (blockHint + 1) * HINT_BLOCK_SIZE; }

public:
    static constexpr uint64_t INVALID_PKEY_CHUNK_OFFSET = ((uint64_t)1 << 40) - 1;
    static constexpr uint64_t MAX_VALID_CHUNK_OFFSET = INVALID_PKEY_CHUNK_OFFSET - 1;
    static constexpr uint64_t HINT_BLOCK_SIZE = 4 * 1024; // 4 KB

private:
    static uint64_t CalculateBlockHint(uint64_t startOffset, uint64_t endOffset)
    {
        assert(endOffset > startOffset);
        int64_t endBlockIdx = endOffset / HINT_BLOCK_SIZE;
        if (endOffset % HINT_BLOCK_SIZE != 0) {
            endBlockIdx++;
        }
        int64_t hint = endBlockIdx - startOffset / HINT_BLOCK_SIZE;
        if (hint > 4) {
            hint = 4;
        }
        return hint - 1;
    }

} __attribute__((packed));

static_assert(sizeof(OnDiskPKeyOffset) == sizeof(uint64_t));

} // namespace indexlibv2::index
