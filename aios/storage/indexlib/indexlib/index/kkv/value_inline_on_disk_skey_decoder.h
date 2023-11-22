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
#include "indexlib/index/kkv/on_disk_value_inline_skey_node.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

// TODO(xingwo): Fix this
template <typename SKeyType>
class ValueInlineOnDiskSKeyDecoder final : public OnDiskSKeyDecoder<SKeyType, true>
{
private:
    using Base = OnDiskSKeyDecoder<SKeyType, true>;
    using SKeyNode = typename SuffixKeyTraits<SKeyType, true>::SKeyNode;

public:
    ValueInlineOnDiskSKeyDecoder(bool storeTs, uint32_t defaultTs, bool storeExpireTime, int32_t fixedValueLen)
        : Base(storeTs, defaultTs, storeExpireTime)
        , mFixedValueLen(fixedValueLen)
    {
    }
    ~ValueInlineOnDiskSKeyDecoder() {}

public:
    void InitChunkFileDecoder(autil::mem_pool::Pool* pool, file_system::FileReader* skeyReader) override final
    {
        Base::mChunkFileDecoder.Init(pool, skeyReader, 0);
    }

    void MoveToNext() override
    {
        bool hasValue = false;
        ReadSKeyArea(mSKeyData);
        hasValue = GetSKeyNode().HasValue();
        if (hasValue) {
            ReadValue(mValueData);
        }
    }

    void GetValue(autil::StringView& value) { value = mValueData; }

private:
    void ReadSKeyArea(autil::StringView& data)
    {
        size_t recordSize = sizeof(SKeyNode);
        if (Base::mStoreTs) {
            recordSize += sizeof(uint32_t);
        }
        if (Base::mStoreExpireTime) {
            recordSize += sizeof(uint32_t);
        }
        Base::mChunkFileDecoder.Read(data, recordSize, Base::mReadOption);
    }

    void ReadValue(autil::StringView& data)
    {
        if (mFixedValueLen > 0) {
            Base::mChunkFileDecoder.Read(data, mFixedValueLen, Base::mReadOption);
        } else {
            Base::mChunkFileDecoder.ReadRecord(data, Base::mReadOption);
        }
    }

    const SKeyNode& GetSKeyNode() const override { return *(SKeyNode*)mSKeyData.data(); }

    uint32_t DoGetTs() const override
    {
        assert(Base::mStoreTs);
        uint32_t ts = 0;
        const char* tsAddr = mSKeyData.data() + sizeof(SKeyNode);
        ::memcpy(&ts, tsAddr, sizeof(ts));
        return ts;
    }

    uint32_t DoGetExpireTime() const override
    {
        assert(Base::mStoreExpireTime);
        uint32_t expireTime = 0;
        const char* addr = mSKeyData.data() + sizeof(SKeyNode) + (Base::mStoreTs ? sizeof(uint32_t) : 0);
        ::memcpy(&expireTime, addr, sizeof(expireTime));
        return expireTime;
    }

    const OnDiskNode& GetOnDiskSKeyNode() const override { return *(OnDiskNode*)mSKeyData.data(); }

private:
    const int32_t mFixedValueLen;

    autil::StringView mSKeyData;
    autil::StringView mValueData;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
