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
#ifndef __INDEXLIB_KKV_INDEX_DUMPER_H
#define __INDEXLIB_KKV_INDEX_DUMPER_H

#include <memory>

#include "autil/mem_pool/PoolBase.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index/kkv/building_kkv_segment_iterator.h"
#include "indexlib/index/kkv/kkv_data_collector.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/prefix_key_table_iterator_base.h"
#include "indexlib/index/kkv/suffix_key_writer.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/PrimeNumberTable.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace index {

class KKVIndexDumperBase
{
public:
    virtual ~KKVIndexDumperBase() {}
    virtual void Dump(const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* dumpPool) = 0;
};
DEFINE_SHARED_PTR(KKVIndexDumperBase);

template <typename SKeyType>
class KKVIndexDumper : public KKVIndexDumperBase
{
public:
    typedef typename index::PrefixKeyTableBase<SKeyListInfo> PKeyTable;
    DEFINE_SHARED_PTR(PKeyTable);

    typedef PrefixKeyTableIteratorBase<SKeyListInfo> PKeyIterator;
    DEFINE_SHARED_PTR(PKeyIterator);

public:
    KKVIndexDumper(const config::IndexPartitionSchemaPtr& schema, const config::KKVIndexConfigPtr& kkvConfig,
                   const config::IndexPartitionOptions& options)
        : mSchema(schema)
        , mKKVConfig(kkvConfig)
        , mOptions(options)
    {
    }

    ~KKVIndexDumper() {}

public:
    typedef SuffixKeyWriter<SKeyType> SKeyWriter;
    DEFINE_SHARED_PTR(SKeyWriter);

public:
    void Init(const PKeyIteratorPtr& pkeyIterator, const SKeyWriterPtr& skeyWriter, const ValueWriterPtr& valueWriter);

    void Dump(const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* dumpPool);

    static size_t EstimateTempDumpMem(size_t maxValueLen)
    {
        size_t chunkBufSize = KKV_CHUNK_SIZE_THRESHOLD * 2 + maxValueLen + sizeof(NormalOnDiskSKeyNode<SKeyType>) + 8;

        // use threshold represent count
        size_t maxChunkWriterLocationBuffer = KKV_CHUNK_SIZE_THRESHOLD * 2 * sizeof(uint64_t);

        // use threshold represent count
        size_t maxDequeBufferSize =
            KKV_CHUNK_SIZE_THRESHOLD * (sizeof(DumpablePKeyOffsetIterator::InDequeItem) +
                                        sizeof(typename DumpableSKeyNodeIteratorTyped<SKeyType, 8>::SKeyNodeItem));

        size_t hintBufferSize = KKV_CHUNK_SIZE_THRESHOLD * sizeof(std::pair<PKeyType, OnDiskPKeyOffset>);
        return chunkBufSize + maxChunkWriterLocationBuffer + maxDequeBufferSize + hintBufferSize;
    }

    static size_t EstimateDumpFileSize(const PKeyTablePtr& pKeyTable, const SKeyWriterPtr& skeyWriter,
                                       const ValueWriterPtr& valueWriter, bool storeTs, bool storeExpireTime,
                                       double skeyCompressRatio = 1, double valueCompressRatio = 1)
    {
        assert(pKeyTable);
        assert(skeyWriter);
        size_t fileSize =
            PrefixKeyDumper::GetTotalDumpSize(pKeyTable) +
            SuffixKeyDumper::GetTotalDumpSize<SKeyType>(skeyWriter->GetTotalSkeyCount(), storeTs, storeExpireTime) *
                skeyCompressRatio;
        if (!valueWriter) {
            return fileSize;
        }

        fileSize += ValueDumper<SKeyType>::GetTotalDumpSize(valueWriter->GetUsedBytes()) * valueCompressRatio;
        return fileSize;
    }

private:
    typedef KKVDataCollector<SKeyType> DataCollector;
    typedef std::shared_ptr<DataCollector> DataCollectorPtr;

    void CollectSinglePrefixKey(const DataCollectorPtr& dataCollector, uint64_t pKey, const SKeyListInfo& listInfo);

private:
    PKeyIteratorPtr mPkeyIterator;
    SKeyWriterPtr mSkeyWriter;
    ValueWriterPtr mValueWriter;

    config::IndexPartitionSchemaPtr mSchema;
    config::KKVIndexConfigPtr mKKVConfig;
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};
IE_LOG_SETUP_TEMPLATE(index, KKVIndexDumper);

//////////////////////////////////////////////////////////////
template <typename SKeyType>
inline void KKVIndexDumper<SKeyType>::Init(const PKeyIteratorPtr& pkeyIterator, const SKeyWriterPtr& skeyWriter,
                                           const ValueWriterPtr& valueWriter)
{
    mPkeyIterator = pkeyIterator;
    mSkeyWriter = skeyWriter;
    mValueWriter = valueWriter;
}

template <typename SKeyType>
inline void KKVIndexDumper<SKeyType>::Dump(const file_system::DirectoryPtr& directory,
                                           autil::mem_pool::PoolBase* dumpPool)
{
    IE_LOG(INFO, "Begin Dump kkv index!");
    DataCollectorPtr collector(new KKVDataCollector<SKeyType>(mSchema, mKKVConfig, true));

    auto phrase = mOptions.IsOffline() ? DCP_BUILD_DUMP : DCP_RT_BUILD_DUMP;
    collector->Init(directory, dumpPool, mPkeyIterator->Size(), phrase);
    collector->Reserve(mSkeyWriter->GetTotalSkeyCount(), mValueWriter->GetUsedBytes());
    while (mPkeyIterator->IsValid()) {
        uint64_t key = 0;
        SKeyListInfo listInfo;
        mPkeyIterator->Get(key, listInfo);
        CollectSinglePrefixKey(collector, key, listInfo);
        mPkeyIterator->MoveToNext();
    }
    collector->Close();
    IE_LOG(INFO, "End Dump kkv index!");
}

template <typename SKeyType>
inline void KKVIndexDumper<SKeyType>::CollectSinglePrefixKey(const DataCollectorPtr& dataCollector, uint64_t pKey,
                                                             const SKeyListInfo& listInfo)
{
    BuildingKKVSegmentIterator<SKeyType> iter(NULL);
    auto regionId = listInfo.regionId;
    auto regionKkvConfig = DYNAMIC_POINTER_CAST(
        config::KKVIndexConfig, mSchema->GetRegionSchema(regionId)->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    iter.Init(mSkeyWriter, mValueWriter, listInfo, regionKkvConfig);
    if (iter.HasPKeyDeleted()) {
        dataCollector->CollectDeletedPKey(pKey, iter.GetPKeyDeletedTs(), !iter.IsValid(), regionId);
    }

    while (iter.IsValid()) {
        uint64_t skey64 = 0;
        bool isDeleted = false;
        iter.GetCurrentSkey(skey64, isDeleted);
        uint32_t ts = iter.GetCurrentTs();
        uint32_t expireTime = iter.GetCurrentExpireTime();
        SKeyType skey = (SKeyType)skey64;
        if (!isDeleted) {
            autil::StringView value;
            iter.GetCurrentValue(value);
            iter.MoveToNext();
            dataCollector->CollectValidSKey(pKey, skey, ts, expireTime, value, !iter.IsValid(), regionId);
        } else {
            iter.MoveToNext();
            dataCollector->CollectDeletedSKey(pKey, skey, ts, !iter.IsValid(), regionId);
        }
    }
}
}} // namespace indexlib::index

#endif //__INDEXLIB_KKV_INDEX_DUMPER_H
