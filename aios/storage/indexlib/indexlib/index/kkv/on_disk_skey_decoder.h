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
#ifndef __INDEXLIB_ON_DISK_SKEY_DECODER_H
#define __INDEXLIB_ON_DISK_SKEY_DECODER_H

#include <memory>

#include "indexlib/common/chunk/chunk_file_decoder.h"
#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/kkv_traits.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename SKeyType, bool valueInline>
class OnDiskSKeyDecoder
{
private:
    using SKeyNode = typename SuffixKeyTraits<SKeyType, valueInline>::SKeyNode;

public:
    OnDiskSKeyDecoder(bool storeTs, uint32_t defaultTs, bool storeExpireTime)
        : mStoreTs(storeTs)
        , mDefaultTs(defaultTs)
        , mStoreExpireTime(storeExpireTime)
        , mPKeyDeletedTs(0)
        , mHasPKeyDeleted(false)
    {
    }
    virtual ~OnDiskSKeyDecoder() {}

public:
    void Init(OnDiskPKeyOffset skeyOffset, file_system::FileReader* skeyReader, autil::mem_pool::Pool* pool)
    {
        InitChunkFileDecoder(pool, skeyReader);
        auto blockOffset = skeyOffset.GetBlockOffset();
        auto hintSize = skeyOffset.GetHintSize();
        hintSize = std::min(hintSize, skeyReader->GetLength() - blockOffset);
        mReadOption.advice = file_system::IO_ADVICE_LOW_LATENCY;
        if (hintSize > OnDiskPKeyOffset::HINT_BLOCK_SIZE) {
            mChunkFileDecoder.Prefetch(hintSize, blockOffset, mReadOption);
        }
        mChunkFileDecoder.Seek(skeyOffset.chunkOffset, skeyOffset.inChunkOffset, mReadOption);
        MoveToNext();

        mHasPKeyDeleted = GetSKeyNode().IsPKeyDeleted();
        mPKeyDeletedTs = mStoreTs ? DoGetTs() : mDefaultTs;
    }

    virtual void MoveToNext() = 0;

    SKeyType GetSKey() const { return GetSKeyNode().skey; }

    uint32_t GetTs() const { return mStoreTs ? DoGetTs() : mDefaultTs; }

    uint32_t GetExpireTime() const { return mStoreExpireTime ? DoGetExpireTime() : UNINITIALIZED_EXPIRE_TIME; }

    bool HasPKeyDeleted() const { return mHasPKeyDeleted; }

    uint32_t GetPKeyDeletedTs() const
    {
        assert(HasPKeyDeleted());
        return mPKeyDeletedTs;
    }

    bool IsLastNode() const { return GetSKeyNode().IsLastNode(); }

    bool IsDeleted() const { return GetSKeyNode().IsSKeyDeleted(); }

protected:
    virtual void InitChunkFileDecoder(autil::mem_pool::Pool* pool, file_system::FileReader* skeyReader) = 0;

protected:
    virtual const SKeyNode& GetSKeyNode() const = 0;
    virtual uint32_t DoGetTs() const = 0;
    virtual uint32_t DoGetExpireTime() const = 0;

protected:
    common::ChunkFileDecoder mChunkFileDecoder;
    bool mStoreTs;
    uint32_t mDefaultTs;
    bool mStoreExpireTime;
    uint32_t mPKeyDeletedTs;
    bool mHasPKeyDeleted;
    file_system::ReadOption mReadOption;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index

#endif //__INDEXLIB_ON_DISK_SKEY_DECODER_H
