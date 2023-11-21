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

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/on_disk_skey_decoder.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename SKeyType>
class NormalOnDiskSkeyDecoder final : public OnDiskSKeyDecoder<SKeyType, false>
{
private:
    using Base = OnDiskSKeyDecoder<SKeyType, false>;
    using SKeyNode = typename SuffixKeyTraits<SKeyType, false>::SKeyNode;

public:
    NormalOnDiskSkeyDecoder(bool storeTs, uint32_t defaultTs, bool storeExpireTime)
        : Base(storeTs, defaultTs, storeExpireTime)
    {
    }

public:
    void InitChunkFileDecoder(autil::mem_pool::Pool* pool, file_system::FileReader* skeyReader) override
    {
        size_t recordSize = sizeof(SKeyNode);
        if (Base::mStoreTs) {
            recordSize += sizeof(uint32_t);
        }
        if (Base::mStoreExpireTime) {
            recordSize += sizeof(uint32_t);
        }
        Base::mChunkFileDecoder.Init(pool, skeyReader, recordSize);
    }

    void MoveToNext() override { Base::mChunkFileDecoder.ReadRecord(mData, Base::mReadOption); }

    void GetValueOffset(uint64_t& chunkOffset, uint32_t& inChunkOffset)
    {
        ResolveSkeyOffset(GetSKeyNode().offset, chunkOffset, inChunkOffset);
    }

private:
    const SKeyNode& GetSKeyNode() const override { return *(SKeyNode*)mData.data(); }

    uint32_t DoGetTs() const override
    {
        assert(Base::mStoreTs);
        uint32_t ts = 0;
        const char* tsAddr = mData.data() + sizeof(SKeyNode);
        ::memcpy(&ts, tsAddr, sizeof(ts));
        return ts;
    }

    uint32_t DoGetExpireTime() const override
    {
        assert(Base::mStoreExpireTime);
        uint32_t expireTime = 0;
        const char* addr = mData.data() + sizeof(SKeyNode) + (Base::mStoreTs ? sizeof(uint32_t) : 0);
        ::memcpy(&expireTime, addr, sizeof(expireTime));
        return expireTime;
    }

private:
    autil::StringView mData;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
