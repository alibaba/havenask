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
#include <type_traits>

#include "indexlib/index/kkv/common/ChunkDefine.h"

namespace indexlibv2::index {

// value chunk will align to 8 byte, so on disk skey meta cut down 3 bits storgage
static constexpr uint64_t ON_DISK_VALUE_CHUNK_ALIGN_BIT = 3;

struct NormalOnDiskSKeyNodeMeta {
    uint64_t chunkOffset   : 35;
    uint64_t inChunkOffset : 12;
    uint64_t isLastNode    : 1;

private:
    static constexpr uint64_t INVALID_NODE_OFFSET = ((uint64_t)1 << 35) - 1;
    static constexpr uint64_t NODE_DELETE_SKEY_OFFSET = ((uint64_t)1 << 35) - 2;
    static constexpr uint64_t NODE_DELETE_PKEY_OFFSET = ((uint64_t)1 << 35) - 3;
    using Meta = NormalOnDiskSKeyNodeMeta;

public:
    inline bool IsSKeyDeleted() const { return chunkOffset == NODE_DELETE_SKEY_OFFSET; }
    inline bool IsPKeyDeleted() const { return chunkOffset == NODE_DELETE_PKEY_OFFSET; }
    inline bool IsLastNode() const { return isLastNode == 1; }
    inline bool IsValidNode() const { return chunkOffset != INVALID_NODE_OFFSET; }
    inline ValueOffset GetValueOffset() const
    {
        return ValueOffset::Of(chunkOffset << ON_DISK_VALUE_CHUNK_ALIGN_BIT, inChunkOffset);
    }
    inline bool operator==(const Meta& rhs) const
    {
        return chunkOffset == rhs.chunkOffset && inChunkOffset == rhs.inChunkOffset && isLastNode == rhs.isLastNode;
    }
    static inline Meta PKeyDeletedMeta(bool isLastNode) { return {NODE_DELETE_PKEY_OFFSET, 0, isLastNode}; }
    static inline Meta SKeyDeletedMeta(bool isLastNode) { return {NODE_DELETE_SKEY_OFFSET, 0, isLastNode}; }
    static inline Meta Of(ValueOffset valueOffset, bool isLastNode)
    {
        uint64_t onDiskChunkOffset = valueOffset.chunkOffset >> ON_DISK_VALUE_CHUNK_ALIGN_BIT;
        return {onDiskChunkOffset, valueOffset.inChunkOffset, isLastNode};
    }

    static constexpr uint64_t MAX_VALID_CHUNK_OFFSET = (NODE_DELETE_PKEY_OFFSET - 1) << ON_DISK_VALUE_CHUNK_ALIGN_BIT;
} __attribute__((packed));

static_assert(sizeof(NormalOnDiskSKeyNodeMeta) == 6);
static_assert(std::is_trivially_copyable_v<NormalOnDiskSKeyNodeMeta>);
static constexpr uint64_t NAX_VALUE_CHUNK_OFFSET = NormalOnDiskSKeyNodeMeta::MAX_VALID_CHUNK_OFFSET;

template <typename SKeyType>
struct NormalOnDiskSKeyNode {
private:
    using Meta = NormalOnDiskSKeyNodeMeta;

public:
    Meta meta;
    SKeyType skey;

public:
    inline bool IsValidNode() const { return meta.IsValidNode(); }
    inline bool IsLastNode() const { return meta.IsLastNode(); }
    inline bool IsPKeyDeleted() const { return meta.IsPKeyDeleted(); }
    inline bool IsSKeyDeleted() const { return meta.IsSKeyDeleted(); }
    inline ValueOffset GetValueOffset() const { return meta.GetValueOffset(); }
    inline bool operator==(const NormalOnDiskSKeyNode& rhs) const { return meta == rhs.meta && skey == rhs.skey; }

    static inline NormalOnDiskSKeyNode PKeyDeletedNode(bool isLastNode)
    {
        return {Meta::PKeyDeletedMeta(isLastNode), SKeyType()};
    }
    static inline NormalOnDiskSKeyNode SKeyDeletedNode(SKeyType skey, bool isLastNode)
    {
        return {Meta::SKeyDeletedMeta(isLastNode), skey};
    }
    static inline NormalOnDiskSKeyNode Of(SKeyType skey, ValueOffset valueOffset, bool isLastNode)
    {
        return {Meta::Of(valueOffset, isLastNode), skey};
    }
} __attribute__((packed));

// static_assert(std::is_trivially_copyable_v<NormalOnDiskSKeyNode>);

} // namespace indexlibv2::index
