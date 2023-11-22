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
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index/kkv/kkv_index_format.h"
#include "indexlib/index/kkv/kkv_value_dumper.h"
#include "indexlib/index/kkv/normal_kkv_data_dumper.h"
#include "indexlib/index/kkv/prefix_key_dumper.h"
#include "indexlib/index/kkv/sort_collect_info_processor.h"
#include "indexlib/index/kkv/suffix_key_dumper.h"
#include "indexlib/index/kkv/truncate_collect_info_processor.h"
#include "indexlib/index/kkv/value_inline_kkv_data_dumper.h"
#include "indexlib/index/kv/multi_region_info.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
namespace indexlib { namespace index {

enum CollectProcessorType {
    CPT_NORMAL,
    CPT_TRUNC,
    CPT_SORT,
    CPT_UNKOWN,
};

enum DataCollectorPhrase {
    DCP_BUILD_DUMP,
    DCP_RT_BUILD_DUMP,
    DCP_MERGE_BOTTOMLEVEL,
    DCP_MERGE,
    DCP_UNKOWN,
};
template <typename SKeyType>
class KKVDataCollector
{
public:
    KKVDataCollector(const config::IndexPartitionSchemaPtr& schema, const config::KKVIndexConfigPtr& kkvConfig,
                     bool storeTs)
        : mPkeyHash(0)
        , mValidSkeyCount(0)
        , mSchema(schema)
        , mKKVConfig(kkvConfig)
        , mPool(NULL)
        , mStoreTs(storeTs)
        , mKeepSkeySorted(false)
        , mPhrase(DCP_UNKOWN)
    {
    }

    virtual ~KKVDataCollector() {}

public:
    void Init(const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* pool, size_t totalPkeyCount,
              DataCollectorPhrase phrase)
    {
        mPool = pool;
        mPhrase = phrase;
        const config::KKVIndexPreference& indexPreference = mKKVConfig->GetIndexPreference();

        if ((phrase == DCP_MERGE_BOTTOMLEVEL || phrase == DCP_MERGE) && indexPreference.IsValueInline()) {
            mDataDumper.reset(new ValueInlineKKVDataDumper<SKeyType>(mSchema, mKKVConfig, mStoreTs, directory, pool));
        } else {
            mDataDumper.reset(new NormalKKVDataDumper<SKeyType>(mSchema, mKKVConfig, mStoreTs, directory, pool));
        }

        mPkeyDumper.reset(new PrefixKeyDumper());
        auto pkeyIter = mDataDumper->GetPKeyOffsetIterator();
        mPkeyDumper->Init(indexPreference, pkeyIter, directory, totalPkeyCount);
        mDirectory = directory;

        // add collect processor for each region
        for (regionid_t id = 0; id < (regionid_t)mSchema->GetRegionCount(); id++) {
            const config::IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema(id);
            config::KKVIndexConfigPtr regionKkvConfig =
                DYNAMIC_POINTER_CAST(config::KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
            CollectInfoProcessorPtr collectProcessor;
            CollectProcessorType cpType = GetCollectProcessorType(regionKkvConfig, phrase);
            switch (cpType) {
            case CPT_NORMAL:
                collectProcessor.reset(new CollectInfoProcessor);
                break;
            case CPT_SORT:
                collectProcessor.reset(new SortCollectInfoProcessor);
                mKeepSkeySorted = true;
                break;
            case CPT_TRUNC:
                collectProcessor.reset(new TruncateCollectInfoProcessor);
                break;
            default:
                assert(false);
            }
            collectProcessor->Init(regionKkvConfig);
            mCollectProcessors.push_back(collectProcessor);
        }
        if (mSchema->GetRegionCount() > 1) {
            mRegionInfo.reset(new MultiRegionInfo(mSchema->GetRegionCount()));
        }
    }

    static CollectProcessorType GetCollectProcessorType(const config::KKVIndexConfigPtr& config,
                                                        DataCollectorPhrase phrase)
    {
        assert(config);
        CollectProcessorType type = CPT_NORMAL;
        switch (phrase) {
        case DCP_BUILD_DUMP:
            break;
        case DCP_RT_BUILD_DUMP:
            type = CPT_TRUNC;
            break;
        case DCP_MERGE_BOTTOMLEVEL:
            if (config->EnableSuffixKeyKeepSortSequence()) {
                type = CPT_SORT;
            } else if (config->NeedSuffixKeyTruncate()) {
                type = CPT_TRUNC;
            }
            break;
        case DCP_MERGE:
            type = CPT_TRUNC;
            break;
        default:
            assert(false);
        }

        return type;
    }

    void ResetBufferParam(size_t writerBufferSize, bool useAsyncWriter)
    {
        assert(mDataDumper);
        mDataDumper->ResetBufferParam(writerBufferSize, useAsyncWriter);
    }

    void Reserve(size_t totalSkeyCount, size_t totalValueLen)
    {
        assert(mDataDumper);
        mDataDumper->Reserve(totalSkeyCount, totalValueLen);
    }

    void CollectDeletedPKey(uint64_t pkey, uint32_t ts, bool isLastNode, regionid_t regionId = DEFAULT_REGIONID);
    void CollectDeletedSKey(uint64_t pkey, SKeyType skey, uint32_t ts, bool isLastNode,
                            regionid_t regionId = DEFAULT_REGIONID);
    void CollectValidSKey(uint64_t pkey, SKeyType skey, uint32_t ts, uint32_t expireTime,
                          const autil::StringView& value, bool isLastNode, regionid_t regionId = DEFAULT_REGIONID);

    void Close()
    {
        assert(mDataDumper);
        assert(mPkeyDumper);

        // notice : not change sequence dataDumper-> pkeyDumper
        mDataDumper->Close();
        mPkeyDumper->Close();

        StoreKKVIndexFormat();
        if (mRegionInfo) {
            mRegionInfo->Store(mDirectory);
        }
    }

    size_t GetMaxValueLen() const { return mDataDumper->GetMaxValueLen(); }
    size_t GetPKeyCount() const { return mPkeyDumper->Size(); }
    size_t GetSKeyCount() const { return mDataDumper->GetSKeyCount(); }
    uint32_t GetMaxSKeyCount() const { return mDataDumper->GetMaxSKeyCount(); }

protected:
    void AddSKeyCollectInfo(uint64_t pkey, SKeyType skey, uint32_t ts, uint32_t expireTime, bool isDeletedPKey,
                            bool isDeletedSKey, const autil::StringView& value, regionid_t regionId);

    void FlushCurrentPKey(regionid_t regionId);

    void DoCollect(uint64_t pkey, SKeyType skey, uint32_t ts, uint32_t expireTime, bool isDeletedPkey,
                   bool isDeletedSkey, bool isLastNode, const autil::StringView& value, regionid_t regionId)
    {
        // check pkey & skey sequence with last DoCollect & isLastNode
        // must call DoCollect by sequence

        assert(mDataDumper);
        assert(mPkeyDumper);

        // TODO: avoid virtual function call
        mDataDumper->Collect(pkey, skey, ts, expireTime, isDeletedPkey, isDeletedSkey, isLastNode, value, regionId);
        mPkeyDumper->Flush();
        if (mRegionInfo) {
            mRegionInfo->Get(regionId)->AddItemCount(1);
        }
    }

    virtual void StoreKKVIndexFormat()
    {
        bool valueInline = false;
        if (mPhrase == DCP_MERGE || mPhrase == DCP_MERGE_BOTTOMLEVEL) {
            valueInline = mKKVConfig->GetIndexPreference().IsValueInline();
        }
        KKVIndexFormat format(mStoreTs, mKeepSkeySorted, valueInline);
        format.Store(mDirectory);
    }

protected:
    typedef std::vector<SKeyCollectInfo*> SKeyCollectInfos;

    SKeyCollectInfos mCollectInfos;
    uint64_t mPkeyHash;
    uint32_t mValidSkeyCount;
    SKeyCollectInfoPool mCollectInfoPool;

    config::IndexPartitionSchemaPtr mSchema;
    config::KKVIndexConfigPtr mKKVConfig;

    PrefixKeyDumperPtr mPkeyDumper;
    std::shared_ptr<KKVDataDumper<SKeyType>> mDataDumper;
    autil::mem_pool::PoolBase* mPool;
    bool mStoreTs;
    file_system::DirectoryPtr mDirectory;
    std::vector<CollectInfoProcessorPtr> mCollectProcessors;
    bool mKeepSkeySorted;

    MultiRegionInfoPtr mRegionInfo;
    DataCollectorPhrase mPhrase;

private:
    IE_LOG_DECLARE();
};

/////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, KKVDataCollector);

template <typename SKeyType>
inline void KKVDataCollector<SKeyType>::CollectDeletedPKey(uint64_t pkey, uint32_t ts, bool isLastNode,
                                                           regionid_t regionId)
{
    AddSKeyCollectInfo(pkey, SKeyType(), ts, UNINITIALIZED_EXPIRE_TIME, true, false,
                       autil::StringView::empty_instance(), regionId);
    if (isLastNode) {
        FlushCurrentPKey(regionId);
    }
}

template <typename SKeyType>
inline void KKVDataCollector<SKeyType>::CollectDeletedSKey(uint64_t pkey, SKeyType skey, uint32_t ts, bool isLastNode,
                                                           regionid_t regionId)
{
    AddSKeyCollectInfo(pkey, skey, ts, UNINITIALIZED_EXPIRE_TIME, false, true, autil::StringView::empty_instance(),
                       regionId);
    if (isLastNode) {
        FlushCurrentPKey(regionId);
    }
}

template <typename SKeyType>
inline void KKVDataCollector<SKeyType>::CollectValidSKey(uint64_t pkey, SKeyType skey, uint32_t ts, uint32_t expireTime,
                                                         const autil::StringView& value, bool isLastNode,
                                                         regionid_t regionId)
{
    AddSKeyCollectInfo(pkey, skey, ts, expireTime, false, false, value, regionId);
    ++mValidSkeyCount;
    if (isLastNode) {
        FlushCurrentPKey(regionId);
    }
}

template <typename SKeyType>
inline void KKVDataCollector<SKeyType>::AddSKeyCollectInfo(uint64_t pkey, SKeyType skey, uint32_t ts,
                                                           uint32_t expireTime, bool isDeletedPKey, bool isDeletedSKey,
                                                           const autil::StringView& value, regionid_t regionId)
{
    if (!mCollectInfos.empty() && pkey != mPkeyHash) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "pkey not consistent equal!");
    }

    mPkeyHash = pkey;
    SKeyCollectInfo* info = mCollectInfoPool.CreateCollectInfo();
    assert(info);
    info->skey = (uint64_t)skey;
    info->ts = ts;
    info->expireTime = expireTime;
    info->isDeletedPKey = isDeletedPKey;
    info->isDeletedSKey = isDeletedSKey;
    info->value = value;
    mCollectInfos.push_back(info);
}

template <typename SKeyType>
inline void KKVDataCollector<SKeyType>::FlushCurrentPKey(regionid_t regionId)
{
    uint32_t flushCount = mCollectProcessors[regionId]->Process(mCollectInfos, mValidSkeyCount);
    for (uint32_t i = 0; i < flushCount; i++) {
        bool isLastNode = ((i + 1) == flushCount);
        SKeyCollectInfo* info = mCollectInfos[i];
        this->DoCollect(mPkeyHash, (SKeyType)info->skey, info->ts, info->expireTime, info->isDeletedPKey,
                        info->isDeletedSKey, isLastNode, info->value, regionId);
    }
    mCollectInfos.clear();
    mValidSkeyCount = 0;
    mCollectInfoPool.Reset();
}
}} // namespace indexlib::index
