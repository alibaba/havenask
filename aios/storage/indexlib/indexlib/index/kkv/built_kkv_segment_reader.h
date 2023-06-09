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
#ifndef __INDEXLIB_BUILT_KKV_SEGMENT_READER_H
#define __INDEXLIB_BUILT_KKV_SEGMENT_READER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_segment_reader.h"
#include "indexlib/index/kkv/prefix_key_table_creator.h"
#include "indexlib/indexlib.h"
//#include "indexlib/index/kkv/prefix_key_table_seeker.h"
#include "indexlib/codegen/codegen_object.h"
#include "indexlib/common/chunk/chunk_file_decoder.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index/kkv/closed_hash_prefix_key_table.h"
#include "indexlib/index/kkv/kkv_coro_define.h"
#include "indexlib/index/kkv/kkv_index_format.h"
#include "indexlib/index/kkv/prefix_key_table_base.h"
#include "indexlib/index/kkv/separate_chain_prefix_key_table.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"

namespace indexlib { namespace index {

template <typename SKeyType>
class BuiltKKVSegmentReader : public codegen::CodegenObject
{
public:
    BuiltKKVSegmentReader()
        : mTimestamp(0)
        , mSegmentId(INVALID_SEGMENTID)
        , mIsRealtimeSegment(false)
        , mStoreTs(false)
        , mValueInline(false)
    {
    }
    ~BuiltKKVSegmentReader() {}

private:
    typedef PrefixKeyTableBase<OnDiskPKeyOffset> PKeyTable;
    DEFINE_SHARED_PTR(PKeyTable);

public:
    void Open(const config::KKVIndexConfigPtr& config, const index_base::SegmentData& segmentData);

    template <class BuiltKKVSegmentIteratorTyped>
    FL_LAZY(BuiltKKVSegmentIteratorTyped*)
    Lookup(PKeyType pKey, autil::mem_pool::Pool* sessionPool, const KKVIndexOptions* options,
           KVMetricsCollector* metricsCollector = nullptr) const
    {
        assert(options);
        OnDiskPKeyOffset skeyOffset;
        if (!FL_COAWAIT((PKeyTable*)mPKeyTable.get())->FindForRead(pKey, skeyOffset, metricsCollector)) {
            FL_CORETURN nullptr;
        }

        regionid_t regionId = options->GetRegionId();
        if (regionId != INVALID_REGIONID && regionId != (regionid_t)skeyOffset.regionId) {
            IE_LOG(ERROR, "Lookup pKey [%lu] not match regionId [%d:%d] from segment [%d]", pKey, regionId,
                   (regionid_t)skeyOffset.regionId, mSegmentId);
            FL_CORETURN nullptr;
        }

        BuiltKKVSegmentIteratorTyped* iter =
            IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, BuiltKKVSegmentIteratorTyped, sessionPool);
        FL_COAWAIT iter->Init(mSkeyReader, mValueReader, options->kkvConfig.get(), skeyOffset, mTimestamp, mStoreTs,
                              !options->sortParams.empty(), metricsCollector);
        FL_CORETURN iter;
    }

    bool IsRealtimeSegment() const { return mIsRealtimeSegment; }
    uint32_t GetTimestampInSecond() const { return mTimestamp; }
    segmentid_t GetSegmentId() const { return mSegmentId; }
    void TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id);
    bool doCollectAllCode();

private:
    file_system::FileReaderPtr CreateValueReader(const file_system::DirectoryPtr& dir);

private:
    config::KKVIndexConfigPtr mKKVIndexConfig;
    PKeyTablePtr mPKeyTable;
    file_system::FileReaderPtr mSkeyReader;
    file_system::FileReaderPtr mValueReader;
    uint32_t mTimestamp;
    segmentid_t mSegmentId;
    bool mIsRealtimeSegment;
    bool mStoreTs;
    bool mValueInline;
    friend class KKVReaderTest;

private:
    IE_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, BuiltKKVSegmentReader);

template <typename SKeyType>
inline void BuiltKKVSegmentReader<SKeyType>::Open(const config::KKVIndexConfigPtr& config,
                                                  const index_base::SegmentData& segmentData)
{
    if (segmentData.GetSegmentInfo()->HasMultiSharding()) {
        INDEXLIB_THROW(util::UnSupportedException, "kkv can not open segment with multi sharding data");
    }

    mSegmentId = segmentData.GetSegmentId();
    assert(config);
    mKKVIndexConfig = config;
    const config::KKVIndexPreference& kkvIndexPreference = mKKVIndexConfig->GetIndexPreference();
    file_system::DirectoryPtr indexDirectory = segmentData.GetIndexDirectory(config->GetIndexName(), true);
    mPKeyTable.reset(
        (PKeyTable*)PrefixKeyTableCreator<OnDiskPKeyOffset>::Create(indexDirectory, kkvIndexPreference, PKOT_READ));

    const config::KKVIndexPreference::SuffixKeyParam& skeyParam = kkvIndexPreference.GetSkeyParam();
    file_system::ReaderOption readOption(file_system::FSOT_LOAD_CONFIG);
    readOption.supportCompress = skeyParam.EnableFileCompress();
    mSkeyReader = indexDirectory->CreateFileReader(SUFFIX_KEY_FILE_NAME, readOption);

    mIsRealtimeSegment = index_base::RealtimeSegmentDirectory::IsRtSegmentId(segmentData.GetSegmentId());
    mTimestamp = MicrosecondToSecond(segmentData.GetSegmentInfo()->timestamp);
    KKVIndexFormat indexFormat;
    indexFormat.Load(indexDirectory);
    mStoreTs = indexFormat.StoreTs();
    mValueInline = indexFormat.ValueInline();

    if (!mValueInline) {
        mValueReader = CreateValueReader(indexDirectory);
    }
}

template <typename SKeyType>
inline file_system::FileReaderPtr
BuiltKKVSegmentReader<SKeyType>::CreateValueReader(const file_system::DirectoryPtr& indexDirectory)
{
    bool swapMmapOpen = mKKVIndexConfig->GetUseSwapMmapFile() &&
                        indexDirectory->GetStorageType(KKV_VALUE_FILE_NAME) == file_system::FSST_MEM;
    file_system::ReaderOption readerOption =
        swapMmapOpen ? file_system::ReaderOption::SwapMmap() : file_system::ReaderOption(file_system::FSOT_LOAD_CONFIG);
    const config::KKVIndexPreference& kkvIndexPreference = mKKVIndexConfig->GetIndexPreference();
    const config::KKVIndexPreference::ValueParam& valueParam = kkvIndexPreference.GetValueParam();
    readerOption.supportCompress = valueParam.EnableFileCompress();
    return indexDirectory->CreateFileReader(KKV_VALUE_FILE_NAME, readerOption);
}

template <typename SKeyType>
inline bool BuiltKKVSegmentReader<SKeyType>::doCollectAllCode()
{
    INIT_CODEGEN_INFO(BuiltKKVSegmentReader, false);
    std::string pkeyTableType;
    PKeyTableType tableType = mPKeyTable->GetTableType();
    switch (tableType) {
    case PKT_SEPARATE_CHAIN:
        pkeyTableType = "SeparateChainPrefixKeyTable<OnDiskPKeyOffset>";
        break;
    case PKT_DENSE:
        pkeyTableType = "ClosedHashPrefixKeyTable<OnDiskPKeyOffset, PKT_DENSE>";
        break;
    case PKT_CUCKOO:
        pkeyTableType = "ClosedHashPrefixKeyTable<OnDiskPKeyOffset, PKT_CUCKOO>";
        break;
    case PKT_ARRAY:
    default:
        assert(false);
    }

    COLLECT_TYPE_REDEFINE(PKeyTable, pkeyTableType);
    return true;
}

template <typename SKeyType>
inline void
BuiltKKVSegmentReader<SKeyType>::TEST_collectCodegenResult(codegen::CodegenObject::CodegenCheckers& checkers,
                                                           std::string id)
{
    codegen::CodegenCheckerPtr checker(new codegen::CodegenChecker);
    COLLECT_TYPE_DEFINE(checker, PKeyTable);
    checkers["BuiltKKVSegmentReader"] = checker;
}
}} // namespace indexlib::index

#endif //__INDEXLIB_BUILT_KKV_SEGMENT_READER_H
