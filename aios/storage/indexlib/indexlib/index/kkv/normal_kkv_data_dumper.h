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
#include "indexlib/index/kkv/kkv_data_dumper.h"
#include "indexlib/index/kkv/kkv_value_dumper.h"
#include "indexlib/index/kkv/suffix_key_dumper.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename SKeyType>
class NormalKKVDataDumper final : public KKVDataDumper<SKeyType>
{
    using Base = KKVDataDumper<SKeyType>;

public:
    NormalKKVDataDumper(const config::IndexPartitionSchemaPtr& schema, const config::KKVIndexConfigPtr& kkvConfig,
                        bool storeTs, const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* pool)
        : Base(schema, kkvConfig, storeTs)
    {
        // TODO check each dumper initialization
        auto storeExpireTime = kkvConfig->StoreExpireTime();
        mSKeyDumper.reset(new SuffixKeyDumper(pool));
        if (!storeTs && !storeExpireTime) {
            mValueDumper.reset(new KKVValueDumper<SKeyType, 0>(pool, storeTs));
        } else if (storeTs && storeExpireTime) {
            mValueDumper.reset(new KKVValueDumper<SKeyType, 8>(pool, storeTs));
        } else {
            mValueDumper.reset(new KKVValueDumper<SKeyType, 4>(pool, storeTs));
        }

        mValueDumper->Init(kkvConfig, directory);
        DumpableSKeyNodeIteratorPtr skeyIter = mValueDumper->CreateSkeyNodeIterator();
        const config::KKVIndexPreference& indexPreference = kkvConfig->GetIndexPreference();
        mSKeyDumper->Init(indexPreference, skeyIter, directory);
        mPKeyIter = mSKeyDumper->CreatePkeyOffsetIterator();
    }
    ~NormalKKVDataDumper() {}

public:
    DumpablePKeyOffsetIteratorPtr GetPKeyOffsetIterator() override { return mPKeyIter; }

    void ResetBufferParam(size_t writerBufferSize, bool useAsyncWriter) override
    {
        mSKeyDumper->ResetBufferParam(writerBufferSize, useAsyncWriter);
        mValueDumper->ResetBufferParam(writerBufferSize, useAsyncWriter);
    }
    void Reserve(size_t totalSkeyCount, size_t totalValueLen) override
    {
        mSKeyDumper->Reserve(SuffixKeyDumper::GetTotalDumpSize<SKeyType>(totalSkeyCount, Base::mStoreTs,
                                                                         Base::mKKVConfig->StoreExpireTime()));
        mValueDumper->Reserve(ValueDumper<SKeyType>::GetTotalDumpSize(totalValueLen));
    }
    void Close() override
    {
        // notice : not change sequence valueDumper-> skeyDumper
        mValueDumper->Close();
        mSKeyDumper->Close();
    }
    size_t GetSKeyCount() const override { return mSKeyDumper->Size(); }
    size_t GetMaxSKeyCount() const override { return mSKeyDumper->GetMaxSKeyCount(); }
    size_t GetMaxValueLen() const override { return mValueDumper->GetMaxValueLen(); }

    void Collect(uint64_t pkey, SKeyType skey, uint32_t ts, uint32_t expireTime, bool isDeletedPkey, bool isDeletedSkey,
                 bool isLastNode, const autil::StringView& value, regionid_t regionId) override
    {
        assert(mValueDumper);
        assert(mSKeyDumper);

        auto regionKKVConfig = DYNAMIC_POINTER_CAST(
            config::KKVIndexConfig,
            Base::mSchema->GetRegionSchema(regionId)->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        assert(regionKKVConfig);
        const auto& valueConfig = regionKKVConfig->GetValueConfig();

        // notice : not change sequence valueDumper-> skeyDumper
        mValueDumper->Insert(pkey, skey, ts, expireTime, isDeletedPkey, isDeletedSkey, isLastNode, value, regionId,
                             valueConfig->GetFixedLength());
        mSKeyDumper->Flush();
    }

private:
    SuffixKeyDumperPtr mSKeyDumper;
    std::unique_ptr<ValueDumper<SKeyType>> mValueDumper;
    DumpablePKeyOffsetIteratorPtr mPKeyIter;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
