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

struct ChunkMeta {
    uint32_t length    : 24;
    uint32_t _reserved : 8; // 老版本索引用于记录是否包含chunk编码逻辑，实际未使用
};

static_assert(std::is_trivially_copyable_v<ChunkMeta>);
static_assert(sizeof(ChunkMeta) == 4);

struct ChunkItemOffset {
    uint64_t chunkOffset   : 52;
    uint64_t inChunkOffset : 12;

    inline bool operator==(const ChunkItemOffset& other) const
    {
        return chunkOffset == other.chunkOffset && inChunkOffset == other.inChunkOffset;
    }

    inline bool IsValidOffset() const { return chunkOffset != INVALID_CHUNK_OFFSET; }

    static inline ChunkItemOffset Of(uint64_t chunkOffset, uint64_t inChunkOffset)
    {
        assert(!IsOffsetOutOfRange(chunkOffset, inChunkOffset));
        return ChunkItemOffset {chunkOffset, inChunkOffset};
    }
    static constexpr ChunkItemOffset Invalid() { return ChunkItemOffset {INVALID_CHUNK_OFFSET, 0UL}; }

    static inline bool IsOffsetOutOfRange(uint64_t chunkOffset, uint64_t inChunkOffset)
    {
        return chunkOffset > MAX_CHUNK_OFFSET || inChunkOffset > MAX_IN_CHUNK_OFFSET;
    }

    static constexpr uint64_t MAX_IN_CHUNK_OFFSET = (1UL << 12) - 1;
    static constexpr uint64_t INVALID_CHUNK_OFFSET = (1UL << 52) - 1;
    static constexpr uint64_t MAX_CHUNK_OFFSET = INVALID_CHUNK_OFFSET - 1;
};

static_assert(sizeof(ChunkItemOffset) == 8);
static_assert(std::is_trivially_copyable_v<ChunkItemOffset>);

using ValueOffset = ChunkItemOffset;
struct ChunkData {
    const char* data = nullptr;
    uint32_t length = 0;

    static constexpr ChunkData Invalid() { return ChunkData {nullptr, 0}; }
    static inline ChunkData Of(const char* data, uint32_t length) { return ChunkData {data, length}; }
    bool IsValid() const { return data != nullptr; }
};
static_assert(std::is_trivially_copyable_v<ChunkData>);

} // namespace indexlibv2::index