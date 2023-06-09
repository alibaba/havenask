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
#ifndef __INDEXLIB_ON_DISK_SKEY_NODE_H
#define __INDEXLIB_ON_DISK_SKEY_NODE_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

// value chunk will align to 8 byte, so on disk skey offset cut down 3 bits storgage
const size_t OnDiskValueChunkAlignBit = 3;

struct OnDiskSKeyOffset {
    OnDiskSKeyOffset(uint64_t _chunkOffset = INVALID_NODE_OFFSET, uint64_t _inChunkOffset = 0, bool _isLastNode = false)
        : chunkOffset(_chunkOffset)
        , inChunkOffset(_inChunkOffset)
        , isLastNode(_isLastNode ? 1 : 0)
    {
    }

    uint64_t chunkOffset   : 35;
    uint64_t inChunkOffset : 12;
    uint64_t isLastNode    : 1;

    void SetPKeyDeleted() { chunkOffset = NODE_DELETE_PKEY_OFFSET; }

    void SetSKeyDeleted() { chunkOffset = NODE_DELETE_SKEY_OFFSET; }

    bool IsSKeyDeleted() const { return chunkOffset == NODE_DELETE_SKEY_OFFSET; }

    bool IsPKeyDeleted() const { return chunkOffset == NODE_DELETE_PKEY_OFFSET; }

    void SetLastNode(bool _isLastNode) { isLastNode = _isLastNode ? 1 : 0; }
    bool IsLastNode() const { return isLastNode == 1; }

    inline bool operator==(const OnDiskSKeyOffset& rhs) const
    {
        return chunkOffset == rhs.chunkOffset && inChunkOffset == rhs.inChunkOffset && isLastNode == rhs.isLastNode;
    }

    inline OnDiskSKeyOffset& operator=(const OnDiskSKeyOffset& other)
    {
        chunkOffset = other.chunkOffset;
        inChunkOffset = other.inChunkOffset;
        isLastNode = other.isLastNode;
        return *this;
    }

public:
    static constexpr uint64_t INVALID_NODE_OFFSET = ((uint64_t)1 << 35) - 1;
    static constexpr uint64_t NODE_DELETE_SKEY_OFFSET = ((uint64_t)1 << 35) - 2;
    static constexpr uint64_t NODE_DELETE_PKEY_OFFSET = ((uint64_t)1 << 35) - 3;
    static constexpr uint64_t MAX_VALID_CHUNK_OFFSET = (NODE_DELETE_PKEY_OFFSET - 1) << OnDiskValueChunkAlignBit;

} __attribute__((packed));

template <typename SKeyType>
struct NormalOnDiskSKeyNode {
    typedef SKeyType Type;
    OnDiskSKeyOffset offset;
    SKeyType skey;

    NormalOnDiskSKeyNode() : skey(SKeyType()) {}

public:
    void SetPKeyDeleted(uint32_t _ts) { offset.SetPKeyDeleted(); }

    void SetSKeyDeleted(SKeyType _skey, uint32_t _ts)
    {
        skey = _skey;
        offset.SetSKeyDeleted();
    }

    void SetSKeyNodeInfo(SKeyType _skey, uint32_t _ts, uint64_t _chunkOffset, uint64_t _inChunkOffset)
    {
        skey = _skey;
        offset.chunkOffset = _chunkOffset;
        offset.inChunkOffset = _inChunkOffset;
    }

    void SetLastNode(bool isLastNode) { offset.SetLastNode(isLastNode); }

    bool IsValidNode() const { return offset.chunkOffset != OnDiskSKeyOffset::INVALID_NODE_OFFSET; }

    bool IsLastNode() const { return offset.IsLastNode(); }

    bool IsPKeyDeleted() const { return offset.IsPKeyDeleted(); }

    bool IsSKeyDeleted() const { return offset.IsSKeyDeleted(); }

    const OnDiskSKeyOffset& GetOffset() const { return offset; }
    OnDiskSKeyOffset& GetOffset() { return offset; }

    inline bool operator==(const NormalOnDiskSKeyNode& rhs) const { return offset == rhs.offset && skey == rhs.skey; }
} __attribute__((packed));
}} // namespace indexlib::index

#endif //__INDEXLIB_ON_DISK_SKEY_NODE_H
