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
#ifndef __INDEXLIB_KKV_MERGER_TYPED_H
#define __INDEXLIB_KKV_MERGER_TYPED_H

#include <memory>

#include "autil/TimeUtility.h"
#include "indexlib/common/hash_table/closed_hash_table_traits.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kkv/closed_hash_prefix_key_table.h"
#include "indexlib/index/kkv/kkv_data_collector.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/kkv_index_dumper.h"
#include "indexlib/index/kkv/on_disk_kkv_iterator.h"
#include "indexlib/index/kkv/separate_chain_prefix_key_table.h"
#include "indexlib/index/kv/kv_ttl_decider.h"
#include "indexlib/index_base/index_meta/segment_topology_info.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/SimplePool.h"
#include "indexlib/util/metrics/ProgressMetrics.h"

namespace indexlib { namespace index {

class KKVMerger
{
public:
    KKVMerger() {}
    virtual ~KKVMerger() {}

public:
    void SetMergeIOConfig(const config::MergeIOConfig& ioConfig) { mIOConfig = ioConfig; }

    virtual void Merge(const file_system::DirectoryPtr& segmentDir, const file_system::DirectoryPtr& indexDir,
                       const index_base::SegmentDataVector& segmentDataVec,
                       const index_base::SegmentTopologyInfo& targetTopoInfo) = 0;

    virtual int64_t EstimateMemoryUse(const index_base::SegmentDataVector& segmentDataVec,
                                      const index_base::SegmentTopologyInfo& targetTopoInfo) const = 0;

    void SetMergeItemMetrics(const util::ProgressMetricsPtr& itemMetrics) { mMergeItemMetrics = itemMetrics; }

    static config::KKVIndexConfigPtr GetDataKKVIndexConfig(const config::IndexPartitionSchemaPtr& schema)
    {
        if (schema->GetRegionCount() > 1) {
            return CreateKKVIndexConfigForMultiRegionData(schema);
        }
        return DYNAMIC_POINTER_CAST(config::KKVIndexConfig,
                                    schema->GetIndexSchema(DEFAULT_REGIONID)->GetPrimaryKeyIndexConfig());
    }

    static void FillSegmentMetrics(const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics, uint32_t shardId,
                                   size_t pkeyCount, size_t skeyCount, size_t maxValueLen, size_t maxSkeyCount)
    {
        std::string groupName = index_base::SegmentWriter::GetMetricsGroupName(shardId);
        segmentMetrics->Set<size_t>(groupName, index::KKV_PKEY_COUNT, pkeyCount);
        segmentMetrics->Set<size_t>(groupName, index::KKV_SKEY_COUNT, skeyCount);
        segmentMetrics->Set<size_t>(groupName, index::KKV_MAX_VALUE_LEN, maxValueLen);
        segmentMetrics->Set<size_t>(groupName, index::KKV_MAX_SKEY_COUNT, maxSkeyCount);
    }

protected:
    uint64_t EstimateMaxKeyCount(const index_base::SegmentDataVector& segmentDataVec) const
    {
        uint64_t maxKeyCount = 0;
        for (const auto& segmentData : segmentDataVec) {
            framework::SegmentMetrics segMetrics;
            segMetrics.Load(segmentData.GetDirectory());

            uint32_t shardId = segmentData.GetShardId();
            std::string groupName = index_base::SegmentWriter::GetMetricsGroupName(shardId);
            size_t pkeyCount = 0;
            segMetrics.Get(groupName, KKV_PKEY_COUNT, pkeyCount);
            maxKeyCount += pkeyCount;
        }
        return maxKeyCount;
    }

    uint64_t GetMaxValueLength(const index_base::SegmentDataVector& segmentDataVec) const
    {
        uint64_t maxValueLen = 0;
        for (const auto& segmentData : segmentDataVec) {
            framework::SegmentMetrics segMetrics;
            segMetrics.Load(segmentData.GetDirectory());

            uint32_t shardId = segmentData.GetShardId();
            std::string groupName = index_base::SegmentWriter::GetMetricsGroupName(shardId);
            size_t valueLen = 0;
            segMetrics.Get(groupName, KKV_MAX_VALUE_LEN, valueLen);
            maxValueLen = std::max(maxValueLen, valueLen);
        }
        return maxValueLen;
    }

protected:
    config::MergeIOConfig mIOConfig;
    util::ProgressMetricsPtr mMergeItemMetrics;
};

DEFINE_SHARED_PTR(KKVMerger);

//////////////////////////////////////////////////////////////////////////
template <typename SKeyType>
class KKVMergerTyped : public KKVMerger
{
private:
    typedef KKVDataCollector<SKeyType> DataCollector;
    DEFINE_SHARED_PTR(DataCollector);

public:
    KKVMergerTyped(int64_t currentTs, const config::IndexPartitionSchemaPtr& schema, bool storeTs = true)
        : mSchema(schema)
        , mCurrentTsInSecond(MicrosecondToSecond(currentTs))
        , mStoreTs(storeTs)
    {
        assert(mSchema);
        mKKVConfig = KKVMerger::GetDataKKVIndexConfig(mSchema);
        mTTLDecider.reset(new KVTTLDecider);
        mTTLDecider->Init(mSchema);
    }

    ~KKVMergerTyped()
    {
        mIterator.reset();
        mCollector.reset();
    }

public:
    void Merge(const file_system::DirectoryPtr& segmentDir, const file_system::DirectoryPtr& indexDir,
               const index_base::SegmentDataVector& segmentDataVec,
               const index_base::SegmentTopologyInfo& targetTopoInfo) override final;

    int64_t EstimateMemoryUse(const index_base::SegmentDataVector& segmentDataVec,
                              const index_base::SegmentTopologyInfo& targetTopoInfo) const override final;

private:
    void CollectSinglePrefixKey(OnDiskSinglePKeyIterator* dataIter, bool isBottomLevel);
    void MoveToFirstValidSKeyPosition(OnDiskSinglePKeyIterator* dataIter, bool isBottomLevel);
    void MoveToNextValidSKeyPosition(OnDiskSinglePKeyIterator* dataIter, bool isBottomLevel);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::KKVIndexConfigPtr mKKVConfig;
    uint64_t mCurrentTsInSecond;
    OnDiskKKVIteratorPtr mIterator;
    DataCollectorPtr mCollector;
    KVTTLDeciderPtr mTTLDecider;
    util::SimplePool mSimplePool;
    bool mStoreTs;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, KKVMergerTyped);
//////////////////////////////////////////////////////////////////////////
template <typename SKeyType>
inline void KKVMergerTyped<SKeyType>::Merge(const file_system::DirectoryPtr& segmentDir,
                                            const file_system::DirectoryPtr& indexDir,
                                            const index_base::SegmentDataVector& segmentDataVec,
                                            const index_base::SegmentTopologyInfo& targetTopoInfo)
{
    IE_LOG(INFO, "merge kkv begin, target dir[%s]", indexDir->DebugString().c_str());

    int64_t beginTime = autil::TimeUtility::currentTime();
    mIterator.reset(new OnDiskKKVIterator(mSchema, mKKVConfig, this->mIOConfig));
    mIterator->Init(segmentDataVec);

    mCollector.reset(new DataCollector(mSchema, mKKVConfig, mStoreTs));
    assert(mCollector);

    uint64_t maxKeyCount = mIterator->EstimatePkeyCount();
    IE_LOG(INFO, "estimate max pkey count [%lu]", maxKeyCount);

    if (indexDir->IsExist(mKKVConfig->GetIndexName())) {
        indexDir->RemoveDirectory(mKKVConfig->GetIndexName());
    }
    file_system::DirectoryPtr kkvDir = indexDir->MakeDirectory(mKKVConfig->GetIndexName());

    auto phrase = targetTopoInfo.isBottomLevel ? DCP_MERGE_BOTTOMLEVEL : DCP_MERGE;
    mCollector->Init(kkvDir, &mSimplePool, maxKeyCount, phrase);
    mCollector->ResetBufferParam(this->mIOConfig.writeBufferSize, this->mIOConfig.enableAsyncWrite);

    uint64_t count = 0;
    while (mIterator->IsValid()) {
        OnDiskSinglePKeyIterator* dataIter = mIterator->GetCurrentIterator();
        CollectSinglePrefixKey(dataIter, targetTopoInfo.isBottomLevel);
        mIterator->MoveToNext();
        ++count;
        if (count % 100000 == 0) {
            IE_LOG(INFO, "already merge [%lu] pkey, memory peak for simple pool is [%lu]", count,
                   mSimplePool.getPeakOfUsedBytes());
        }
        if (mMergeItemMetrics && count % 1000 == 0) {
            mMergeItemMetrics->UpdateCurrentRatio(mIterator->GetMergeProgressRatio());
        }
    }

    mCollector->Close();
    std::shared_ptr<framework::SegmentMetrics> segmentMetrics(new framework::SegmentMetrics());
    assert(segmentDataVec.empty() || segmentDataVec[0].GetShardId() == targetTopoInfo.columnIdx);
    KKVMerger::FillSegmentMetrics(segmentMetrics, targetTopoInfo.columnIdx, mCollector->GetPKeyCount(),
                                  mCollector->GetSKeyCount(), mCollector->GetMaxValueLen(),
                                  mCollector->GetMaxSKeyCount());
    segmentMetrics->Store(indexDir);
    int64_t endTime = autil::TimeUtility::currentTime();
    IE_LOG(INFO, "merge kkv end, target dir[%s], time interval [%ld]ms", indexDir->DebugString().c_str(),
           (endTime - beginTime) / 1000);
}

template <typename SKeyType>
inline int64_t KKVMergerTyped<SKeyType>::EstimateMemoryUse(const index_base::SegmentDataVector& segmentDataVec,
                                                           const index_base::SegmentTopologyInfo& targetTopoInfo) const
{
    PKeyTableType type = GetPrefixKeyTableType(mKKVConfig->GetIndexPreference().GetHashDictParam().GetHashType());
    int32_t occupancyPct = mKKVConfig->GetIndexPreference().GetHashDictParam().GetOccupancyPct();
    int64_t memoryUse = 0;
    size_t hashMemUse = 0;
    size_t iteratorMemUse = 0;
    size_t maxKeyCount = EstimateMaxKeyCount(segmentDataVec);
    size_t targetKeyCount = maxKeyCount;
    if (type == PKT_SEPARATE_CHAIN) {
        uint32_t bucketCount = SeparateChainPrefixKeyTable<OnDiskPKeyOffset>::GetRecommendBucketCount(maxKeyCount);
        hashMemUse = bucketCount * sizeof(uint32_t);
    } else if (type == PKT_DENSE) {
        // read segments & writer segment
        OnDiskKKVIteratorPtr iterator;
        iterator.reset(new OnDiskKKVIterator(mSchema, mKKVConfig, this->mIOConfig));
        iterator->Init(segmentDataVec);
        targetKeyCount = iterator->EstimatePkeyCount();
        hashMemUse = ClosedHashPrefixKeyTable<OnDiskPKeyOffset, PKT_DENSE>::CalculateBuildMemoryUse(
            PKOT_WRITE, targetKeyCount, occupancyPct);
        typedef ClosedHashPrefixKeyTableTraits<OnDiskPKeyOffset, PKT_DENSE>::Traits Traits;
        typedef typename Traits::template FileIterator<false> FileIterator;
        iteratorMemUse = FileIterator().EstimateMemoryUse(maxKeyCount);
    } else if (type == PKT_CUCKOO) {
        // read segments & writer segment
        OnDiskKKVIteratorPtr iterator;
        iterator.reset(new OnDiskKKVIterator(mSchema, mKKVConfig, this->mIOConfig));
        iterator->Init(segmentDataVec);
        targetKeyCount = iterator->EstimatePkeyCount();
        hashMemUse = ClosedHashPrefixKeyTable<OnDiskPKeyOffset, PKT_CUCKOO>::CalculateBuildMemoryUse(
            PKOT_WRITE, targetKeyCount, occupancyPct);
        typedef ClosedHashPrefixKeyTableTraits<OnDiskPKeyOffset, PKT_CUCKOO>::Traits Traits;
        typedef typename Traits::template FileIterator<false> FileIterator;
        iteratorMemUse = FileIterator().EstimateMemoryUse(maxKeyCount);
    } else {
        assert(false);
    }

    memoryUse += (hashMemUse + iteratorMemUse);
    memoryUse += OnDiskKKVIterator::RESET_POOL_THRESHOLD;
    uint32_t maxValueLen = GetMaxValueLength(segmentDataVec);
    memoryUse += index::KKVIndexDumper<SKeyType>::EstimateTempDumpMem(maxValueLen);

    // skey + value + pkey(not support set buffersize)
    size_t oneSegReadBufferSize = mIOConfig.readBufferSize * 2;
    if (mIOConfig.enableAsyncRead) {
        // aync reader has 2 inner buffer
        oneSegReadBufferSize *= 2;
    }
    if (type != PKT_DENSE && type != PKT_CUCKOO) // dense hash not hold reader buffer
    {
        oneSegReadBufferSize += mIOConfig.DEFAULT_READ_BUFFER_SIZE;
    }

    // skey + value + pkey(not support set buffersize)
    size_t oneSegWriteBufferSize = mIOConfig.writeBufferSize * 2;
    if (mIOConfig.enableAsyncWrite) {
        oneSegWriteBufferSize *= 2;
    }
    oneSegWriteBufferSize += mIOConfig.DEFAULT_WRITE_BUFFER_SIZE;

    size_t bufferSize = (segmentDataVec.size() * oneSegReadBufferSize) + oneSegWriteBufferSize;
    memoryUse += bufferSize;
    IE_LOG(INFO,
           "estimate memory use [%ld] for kkv merger, "
           "hash memory use [%lu], iterator mem use [%lu], "
           "buffer memory use [%lu], "
           "segmentCount[%lu], currentKeyCount[%lu], targetKeyCount [%lu]",
           memoryUse, hashMemUse, iteratorMemUse, bufferSize, segmentDataVec.size(), maxKeyCount, targetKeyCount);
    return memoryUse;
}

template <typename SKeyType>
inline void KKVMergerTyped<SKeyType>::MoveToFirstValidSKeyPosition(OnDiskSinglePKeyIterator* dataIter,
                                                                   bool isBottomLevel)
{
    assert(dataIter);
    while (dataIter->IsValid()) {
        assert(mTTLDecider);
        if (mTTLDecider->IsExpiredItem(dataIter->GetRegionId(), dataIter->GetCurrentTs(), mCurrentTsInSecond) &&
            !mKKVConfig->StoreExpireTime()) {
            // over ttl
            dataIter->MoveToNext();
            continue;
        }

        if (isBottomLevel) {
            uint64_t skey64 = 0;
            bool isDeleted = false;
            dataIter->GetCurrentSkey(skey64, isDeleted);
            if (isDeleted || dataIter->CurrentSKeyExpired(mCurrentTsInSecond)) {
                dataIter->MoveToNext();
                continue;
            }
        }
        break;
    }
}

template <typename SKeyType>
inline void KKVMergerTyped<SKeyType>::MoveToNextValidSKeyPosition(OnDiskSinglePKeyIterator* dataIter,
                                                                  bool isBottomLevel)
{
    dataIter->MoveToNext();
    MoveToFirstValidSKeyPosition(dataIter, isBottomLevel);
}

template <typename SKeyType>
void KKVMergerTyped<SKeyType>::CollectSinglePrefixKey(OnDiskSinglePKeyIterator* dataIter, bool isBottomLevel)
{
    assert(dataIter);
    assert(mCollector);

    MoveToFirstValidSKeyPosition(dataIter, isBottomLevel);
    regionid_t regionId = dataIter->GetRegionId();
    if (!isBottomLevel && dataIter->HasPKeyDeleted()) {
        assert(mTTLDecider);
        uint32_t deletePKeyTs = dataIter->GetPKeyDeletedTs();
        if (!mTTLDecider->IsExpiredItem(regionId, deletePKeyTs, mCurrentTsInSecond)) {
            mCollector->CollectDeletedPKey(dataIter->GetPKeyHash(), deletePKeyTs, !dataIter->IsValid(), regionId);
        }
    }

    while (dataIter->IsValid()) {
        uint64_t skey64 = 0;
        bool isDeleted = false;
        dataIter->GetCurrentSkey(skey64, isDeleted);
        uint32_t ts = dataIter->GetCurrentTs();
        uint32_t expireTime = dataIter->GetCurrentExpireTime();
        SKeyType skey = (SKeyType)skey64;
        auto pkeyHash = dataIter->GetPKeyHash();
        if (!isDeleted) {
            autil::StringView value;
            dataIter->GetCurrentValue(value);
            auto isExpired = dataIter->CurrentSKeyExpired(mCurrentTsInSecond);
            MoveToNextValidSKeyPosition(dataIter, isBottomLevel);
            if (isExpired) {
                // optimization: do not store value if skey expired
                mCollector->CollectDeletedSKey(pkeyHash, skey, ts, !dataIter->IsValid(), regionId);

            } else {
                mCollector->CollectValidSKey(pkeyHash, skey, ts, expireTime, value, !dataIter->IsValid(), regionId);
            }
        } else {
            MoveToNextValidSKeyPosition(dataIter, isBottomLevel);
            assert(!isBottomLevel);
            mCollector->CollectDeletedSKey(pkeyHash, skey, ts, !dataIter->IsValid(), regionId);
        }
    }
}

}} // namespace indexlib::index

#endif //__INDEXLIB_KKV_MERGER_TYPED_H
