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
#ifndef __INDEXLIB_KKV_BUILT_SEGMENT_DOC_ITERATOR_H
#define __INDEXLIB_KKV_BUILT_SEGMENT_DOC_ITERATOR_H

#include <memory>

#include "autil/ConstString.h"
#include "indexlib/codegen/codegen_object.h"
#include "indexlib/common/chunk/chunk_file_decoder.h"
#include "indexlib/common/chunk/integrated_plain_chunk_decoder.h"
#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_segment_iterator.h"
#include "indexlib/index/kkv/kkv_traits.h"
#include "indexlib/index/kkv/kkv_value_fetcher.h"
#include "indexlib/index/kkv/normal_on_disk_skey_decoder.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename SKeyType>
class BuiltKKVSegmentReader;

#define SKEY_NODE_FUNC(funcName)                                                                                       \
    do {                                                                                                               \
        if (!mValueInline) {                                                                                           \
            return GetSKeyNode<false>()->funcName();                                                                   \
        } else {                                                                                                       \
            return GetSKeyNode<true>()->funcName();                                                                    \
        }                                                                                                              \
    } while (0)

// TODO(xignwo): Fix this
template <typename SKeyType>
class KKVBuiltSegmentDocIterator final : public KKVSegmentIterator
{
private:
    using PooledSKeySet = typename SKeyHashContainerTraits<SKeyType>::PooledSKeySet;
    typedef SKeySearchContext<SKeyType> SearchSKeyContext;

public:
    KKVBuiltSegmentDocIterator() = default;
    KKVBuiltSegmentDocIterator(autil::mem_pool::Pool* sessionPool, bool isOnline = true)
        : KKVSegmentIterator(sessionPool)
        , mDefaultTs(0)
        , mHasHitLastNode(false)
        , mValueInline(false)
        , mStoreTs(true)
        , mStoreExpireTime(false)
        , mNodeSize(0)
        , mIsOnline(isOnline)
    {
        mHasSkeyContext = false;
        mIsOnlySeg = false;
        mHasMetricsCollector = true;
    }
    KKVBuiltSegmentDocIterator(const KKVBuiltSegmentDocIterator&) = delete;
    KKVBuiltSegmentDocIterator& operator=(const KKVBuiltSegmentDocIterator&) = delete;
    KKVBuiltSegmentDocIterator(KKVBuiltSegmentDocIterator&& rhs)
        : mValueCompressReader(std::move(rhs.mValueCompressReader))
        , mSkeyCompressReader(std::move(rhs.mSkeyCompressReader))
        , mSKeyDecoder(std::move(rhs.mSKeyDecoder))
        , mValueDecoder(std::move(rhs.mValueDecoder))
        , mSKeyChunk(std::move(rhs.mSKeyChunk))
        , mTypedSKeyChunk(rhs.mTypedSKeyChunk)
        , mMetricsCollector(std::move(rhs.mMetricsCollector))
        , mSKeyData(std::move(rhs.mSKeyData))
        , mValueData(std::move(rhs.mValueData))
        , mChunkOffset(rhs.mChunkOffset)
        , mPKeyDeletedTs(rhs.mPKeyDeletedTs)
        , mReadSize(rhs.mReadSize)
        , mDefaultTs(rhs.mDefaultTs)
        , mFixedValueLen(rhs.mFixedValueLen)
        , mReadOption(rhs.mReadOption)
        , mHasHitLastNode(rhs.mHasHitLastNode)
        , mValueInline(rhs.mValueInline)
        , mHasPKeyDeleted(rhs.mHasPKeyDeleted)
        , mStoreTs(rhs.mStoreTs)
        , mStoreExpireTime(rhs.mStoreExpireTime)
        , mNodeSize(rhs.mNodeSize)
        , mIsOnline(rhs.mIsOnline)
    {
        rhs.mMetricsCollector = nullptr;
        rhs.mTypedSKeyChunk = nullptr;
    }
    ~KKVBuiltSegmentDocIterator()
    {
        if (mHasMetricsCollector && mMetricsCollector) {
            mMetricsCollector->IncSKeyDataSizeInBlocks(mReadSize);
            if (mSKeyDecoder.get()) {
                mMetricsCollector->IncSKeyChunkCountInBlocks(mSKeyDecoder->GetLoadChunkCount());
            }
        }
    }

    static void CollectAllCode(bool hasSkeyContext, bool isOnlySeg, bool hasMetricsCollector,
                               codegen::AllCodegenInfo& allCodegenInfos);
    bool TEST_collectCodegenResult(codegen::CodegenObject::CodegenCheckers& checkers);

private:
    inline SKeyStatus ValidateSKey(SKeySearchContext<SKeyType>* skeyContext, uint64_t minimumTsInSecond,
                                   uint64_t currentTsInSecond,
                                   typename SKeyHashContainerTraits<SKeyType>::PooledSKeySet& foundSkeys, SKeyType skey,
                                   uint32_t ts, bool isDeleted);

    template <bool valueInline>
    decltype(auto) GetSKeyNode() const
    {
        using SKeyNode = typename SuffixKeyTraits<SKeyType, valueInline>::SKeyNode;
        return (SKeyNode*)mSKeyData.data();
    }

    util::PooledUniquePtr<common::ChunkFileDecoder>
    CreateOnDiskSKeyDecoder(const config::KKVIndexPreference::SuffixKeyParam& skeyParam,
                            file_system::FileReader* skeyReader);

    util::PooledUniquePtr<common::ChunkFileDecoder>
    CreateOnDiskValueDecoder(const config::KKVIndexPreference::ValueParam& valueParam,
                             const file_system::FileReaderPtr& valueReader);

    uint64_t GetTs() const
    {
        if (!mStoreTs) {
            return mDefaultTs;
        }
        uint32_t ts = 0;
        const char* tsAddr = mSKeyData.data() + mNodeSize;
        ::memcpy(&ts, tsAddr, sizeof(ts));
        return ts;
    }

    uint32_t GetExpireTime() const
    {
        if (!mStoreExpireTime) {
            return UNINITIALIZED_EXPIRE_TIME;
        }
        uint32_t expireTime = UNINITIALIZED_EXPIRE_TIME;
        const char* addr = mSKeyData.data() + mNodeSize + (mStoreTs ? sizeof(uint32_t) : 0);
        ::memcpy(&expireTime, addr, sizeof(expireTime));
        return expireTime;
    }

    void DoNext();
    bool IsLastNode() const { SKEY_NODE_FUNC(IsLastNode); }

public:
    FL_LAZY(future_lite::Unit)
    Init(const file_system::FileReaderPtr& skeyReader, const file_system::FileReaderPtr& valueReader,
         const config::KKVIndexConfig* kkvConfig, OnDiskPKeyOffset offset, uint32_t defaultTs, bool storeTs,
         bool keepSortSeq, KVMetricsCollector* metricsCollector = nullptr);
    FL_LAZY(future_lite::Unit) SwitchChunk(uint64_t offset = 0);

    bool IsValid() const override final { return mTypedSKeyChunk != nullptr; }
    bool MoveToNext() override final;
    bool MoveToValidPosition(SearchSKeyContext* skeyContext, uint64_t minimumTsInSecond, uint64_t currentTsInSecond,
                             PooledSKeySet& foundSkeys, SKeyType& skey, uint32_t& ts);
    bool HasPKeyDeleted() const override final { return mHasPKeyDeleted; }
    uint32_t GetPKeyDeletedTs() const override final { return mPKeyDeletedTs; }
    void GetCurrentSkey(uint64_t& skey, bool& isDeleted) const override final;
    void GetCurrentValue(autil::StringView& value) override final;
    void InnerGetValue(OnDiskSKeyOffset offset, autil::StringView& value);
    uint32_t GetCurrentTs() override;
    uint32_t GetCurrentExpireTime() override;
    int Compare(const KKVSegmentIterator* other) override final;
    KKVDoc GetCurrentDoc();
    bool IsDeleted() const { SKEY_NODE_FUNC(IsSKeyDeleted); }
    bool HasHitLastNode() const { return mHasHitLastNode; }
    void FillResultBuffer(SearchSKeyContext* skeyContext, uint64_t minimumTsInSecond, uint64_t currentTsInSecond,
                          PooledSKeySet& foundSkeys, KKVResultBuffer& resultBuffer);
    KKVValueFetcher StealValueFetcher()
    {
        KKVValueFetcher ret(std::move(mValueCompressReader), std::move(mValueDecoder), mReadOption);
        return ret;
    }

private:
    util::PooledUniquePtr<file_system::CompressFileReader> mValueCompressReader;
    util::PooledUniquePtr<file_system::CompressFileReader> mSkeyCompressReader;
    util::PooledUniquePtr<common::ChunkFileDecoder> mSKeyDecoder;
    util::PooledUniquePtr<common::ChunkFileDecoder> mValueDecoder;
    util::PooledUniquePtr<common::ChunkDecoder> mSKeyChunk;
    common::IntegratedPlainChunkDecoder* mTypedSKeyChunk = nullptr;
    KVMetricsCollector* mMetricsCollector = nullptr;
    autil::StringView mSKeyData;
    autil::StringView mValueData;
    OnDiskPKeyOffset mChunkOffset;
    uint64_t mPKeyDeletedTs = 0;
    int64_t mReadSize = 0;
    uint32_t mDefaultTs = 0;
    int32_t mFixedValueLen = 0;
    file_system::ReadOption mReadOption;
    bool mHasHitLastNode = false;
    bool mValueInline = false;
    bool mHasPKeyDeleted = false;
    bool mStoreTs = false;
    bool mStoreExpireTime = false;
    size_t mNodeSize = 0;
    bool mIsOnline = false;

    // to codegen
    bool mHasSkeyContext;
    bool mIsOnlySeg;
    bool mHasMetricsCollector;

private:
    friend class BuiltKKVSegmentReader<SKeyType>;
};

/////////////////////////////////////////////////
template <typename SKeyType>
inline util::PooledUniquePtr<common::ChunkFileDecoder> KKVBuiltSegmentDocIterator<SKeyType>::CreateOnDiskSKeyDecoder(
    const config::KKVIndexPreference::SuffixKeyParam& skeyParam, file_system::FileReader* skeyReader)
{
    util::PooledUniquePtr<common::ChunkFileDecoder> skeyDecoder;
    {
        auto p = IE_POOL_COMPATIBLE_NEW_CLASS(mPool, common::ChunkFileDecoder);
        skeyDecoder.reset(p, mPool);
    }
    if (mIsOnline && skeyParam.EnableFileCompress()) {
        assert(dynamic_cast<file_system::CompressFileReader*>(skeyReader));
        file_system::CompressFileReader* compressReader = static_cast<file_system::CompressFileReader*>(skeyReader);
        mSkeyCompressReader.reset(compressReader->CreateSessionReader(mPool), mPool);
        skeyReader = mSkeyCompressReader.get();
    }
    if (mValueInline) {
        skeyDecoder->Init(mPool, skeyReader, 0);
    } else {
        size_t recordSize = mNodeSize;
        if (mStoreTs) {
            recordSize += sizeof(uint32_t);
        }
        if (mStoreExpireTime) {
            recordSize += sizeof(uint32_t);
        }
        skeyDecoder->Init(mPool, skeyReader, recordSize);
    }
    return skeyDecoder;
}

template <typename SKeyType>
void KKVBuiltSegmentDocIterator<SKeyType>::FillResultBuffer(SearchSKeyContext* skeyContext, uint64_t minimumTsInSecond,
                                                            uint64_t currentTsInSecond, PooledSKeySet& foundSkeys,
                                                            KKVResultBuffer& resultBuffer)
{
    size_t beginPos = resultBuffer.Size();
    while (true) {
        if (!IsValid()) {
            break;
        }
        uint64_t skey;
        bool isDeleted;
        GetCurrentSkey(skey, isDeleted);
        auto ts = GetCurrentTs();
        resultBuffer.EmplaceBack(KKVDoc(ts, skey));
        if (mValueInline) {
            GetCurrentValue(resultBuffer.Back().value);
        } else {
            auto node = GetSKeyNode<false>();
            resultBuffer.Back().valueOffset = node->offset;
        }
        if (resultBuffer.IsFull() || resultBuffer.ReachLimit()) {
            break;
        }
        if (!MoveToNext() && !HasHitLastNode()) {
            future_lite::interface::syncAwait(SwitchChunk());
        }
        SKeyType tmpSkey = skey;
        if (!MoveToValidPosition(skeyContext, minimumTsInSecond, currentTsInSecond, foundSkeys, tmpSkey, ts)) {
            break;
        }
    }
    if (!mValueInline) {
        for (size_t i = beginPos; i < resultBuffer.Size(); i++) {
            InnerGetValue(resultBuffer[i].valueOffset, resultBuffer[i].value);
        }
    }
}

template <typename SKeyType>
util::PooledUniquePtr<common::ChunkFileDecoder>
KKVBuiltSegmentDocIterator<SKeyType>::CreateOnDiskValueDecoder(const config::KKVIndexPreference::ValueParam& valueParam,
                                                               const file_system::FileReaderPtr& valueReader)
{
    assert(!mValueInline);
    assert(valueReader);
    util::PooledUniquePtr<common::ChunkFileDecoder> valueDecoder;
    common::ChunkFileDecoder* decoder = IE_POOL_COMPATIBLE_NEW_CLASS(mPool, common::ChunkFileDecoder);
    valueDecoder.reset(decoder, mPool);
    file_system::FileReader* reader = valueReader.get();
    if (mIsOnline && valueParam.EnableFileCompress()) {
        assert(dynamic_cast<file_system::CompressFileReader*>(reader));
        file_system::CompressFileReader* compressReader = static_cast<file_system::CompressFileReader*>(reader);
        mValueCompressReader.reset(compressReader->CreateSessionReader(mPool), mPool);
        reader = mValueCompressReader.get();
    }

    if (mFixedValueLen > 0) {
        assert(mFixedValueLen != 0); // guranteeded by FieldConfig
        decoder->Init(mPool, reader, mFixedValueLen);
    } else {
        decoder->Init(mPool, reader, 0);
    }
    return valueDecoder;
}
template <typename SKeyType>
inline FL_LAZY(future_lite::Unit) KKVBuiltSegmentDocIterator<SKeyType>::Init(
    const file_system::FileReaderPtr& skeyReader, const file_system::FileReaderPtr& valueReader,
    const config::KKVIndexConfig* kkvConfig, OnDiskPKeyOffset offset, uint32_t defaultTs, bool storeTs,
    bool keepSortSeq, KVMetricsCollector* metricsCollector)
{
    mKKVIndexConfig = kkvConfig;
    mKeepSortSequence = keepSortSeq;
    mChunkOffset = offset;
    mDefaultTs = defaultTs;
    mStoreTs = storeTs;
    mStoreExpireTime = kkvConfig->StoreExpireTime();
    if (mHasMetricsCollector) {
        mMetricsCollector = metricsCollector;
    }
    if (valueReader.get() == nullptr) {
        mValueInline = true;
    }
    mNodeSize = mValueInline ? sizeof(*GetSKeyNode<true>()) : sizeof(*GetSKeyNode<false>());

    const auto& valueConfig = kkvConfig->GetValueConfig();
    assert(valueConfig);
    mFixedValueLen = valueConfig->GetFixedLength();
    const config::KKVIndexPreference& kkvIndexPreference = kkvConfig->GetIndexPreference();
    const config::KKVIndexPreference::SuffixKeyParam& skeyParam = kkvIndexPreference.GetSkeyParam();
    mSKeyDecoder = CreateOnDiskSKeyDecoder(skeyParam, skeyReader.get());
    assert(mSKeyDecoder.get());

    if (!mValueInline) {
        mValueDecoder = CreateOnDiskValueDecoder(kkvIndexPreference.GetValueParam(), valueReader);
    }

    auto blockOffset = mChunkOffset.GetBlockOffset();
    auto hintSize = mChunkOffset.GetHintSize();
    hintSize = std::min(hintSize, skeyReader->GetLength() - blockOffset);
    if (mHasMetricsCollector && mMetricsCollector) {
        mReadOption.blockCounter = mMetricsCollector->GetBlockCounter();
    }
    mReadOption.advice = file_system::IO_ADVICE_LOW_LATENCY;
    if (hintSize > OnDiskPKeyOffset::HINT_BLOCK_SIZE) {
        (FL_COAWAIT mSKeyDecoder->PrefetchAsync(hintSize, blockOffset, mReadOption)).GetOrThrow();
    }

    FL_COAWAIT SwitchChunk(mChunkOffset.inChunkOffset);
    mHasPKeyDeleted = [this]() -> bool { SKEY_NODE_FUNC(IsPKeyDeleted); }();
    if (mHasPKeyDeleted) {
        mPKeyDeletedTs = GetCurrentTs();
        auto ret = MoveToNext();
        (void)ret;
    }
    FL_CORETURN future_lite::Unit {};
}

template <typename SKeyType>
inline FL_LAZY(future_lite::Unit) KKVBuiltSegmentDocIterator<SKeyType>::SwitchChunk(uint64_t offset)
{
    mHasPKeyDeleted = false;
    mPKeyDeletedTs = 0;
    assert(!mHasHitLastNode);
    uint64_t newChunkOffset = mChunkOffset.chunkOffset;
    mSKeyChunk = FL_COAWAIT mSKeyDecoder->GetChunk(newChunkOffset, mReadOption);
    mChunkOffset.chunkOffset = newChunkOffset;
    assert(mSKeyChunk.get());
    assert(mSKeyChunk->GetType() == common::ct_plain_integrated);
    mTypedSKeyChunk = static_cast<common::IntegratedPlainChunkDecoder*>(mSKeyChunk.get());
    if (offset != 0) {
        mTypedSKeyChunk->Seek(offset);
    }
    if (IsValid()) {
        DoNext();
    }
    FL_CORETURN future_lite::Unit {};
}

template <typename SKeyType>
inline bool KKVBuiltSegmentDocIterator<SKeyType>::MoveToNext()
{
    if (IsLastNode()) {
        mTypedSKeyChunk = nullptr;
        return false;
    }
    if (mTypedSKeyChunk->IsEnd()) {
        if (!mIsOnline) {
            future_lite::interface::syncAwait(SwitchChunk());
            return true;
        }
        mTypedSKeyChunk = nullptr;
        return false;
    }
    DoNext();
    return true;
}

template <typename SKeyType>
inline void KKVBuiltSegmentDocIterator<SKeyType>::DoNext()
{
    if (!mValueInline) {
        // normal
        mReadSize += mTypedSKeyChunk->ReadRecord(mSKeyData);
    } else {
        size_t recordSize = mNodeSize;
        if (mStoreTs) {
            recordSize += sizeof(uint32_t);
        }
        if (mStoreExpireTime) {
            recordSize += sizeof(uint32_t);
        }
        mReadSize += mTypedSKeyChunk->Read(mSKeyData, recordSize);
        bool hasValue = GetSKeyNode<true>()->HasValue();
        if (hasValue) {
            if (mFixedValueLen > 0) {
                mReadSize += mTypedSKeyChunk->Read(mValueData, mFixedValueLen);
            } else {
                mReadSize += mTypedSKeyChunk->ReadRecord(mValueData);
            }
        }
    }
    if (IsLastNode()) {
        mHasHitLastNode = true;
    }
}

template <typename SKeyType>
inline uint32_t KKVBuiltSegmentDocIterator<SKeyType>::GetCurrentTs()
{
    if (!mStoreTs) {
        return mDefaultTs;
    }
    uint32_t ts;
    const char* addr = mSKeyData.data() + mNodeSize;
    ::memcpy((char*)&ts, addr, sizeof(ts));
    return ts;
}

template <typename SKeyType>
inline uint32_t KKVBuiltSegmentDocIterator<SKeyType>::GetCurrentExpireTime()
{
    if (!mStoreExpireTime) {
        return UNINITIALIZED_EXPIRE_TIME;
    }
    uint32_t expireTime = UNINITIALIZED_EXPIRE_TIME;
    const char* addr = mSKeyData.data() + mNodeSize + (mStoreTs ? sizeof(uint32_t) : 0);
    ::memcpy((char*)&expireTime, addr, sizeof(expireTime));
    return expireTime;
}

template <typename SKeyType>
inline KKVDoc KKVBuiltSegmentDocIterator<SKeyType>::GetCurrentDoc()
{
    KKVDoc doc;
    uint64_t skey;
    GetCurrentSkey(skey, doc.skeyDeleted);
    doc.skey = skey;
    doc.timestamp = GetCurrentTs();
    doc.expireTime = GetExpireTime();
    if (!mValueInline) {
        auto node = GetSKeyNode<false>();
        doc.SetValueOffset(node->offset);
    } else {
        doc.SetValue(mValueData);
    }
    return doc;
}

template <typename SKeyType>
inline void KKVBuiltSegmentDocIterator<SKeyType>::GetCurrentSkey(uint64_t& skey, bool& isDeleted) const
{
    if (!mValueInline) {
        auto node = GetSKeyNode<false>();
        skey = node->skey;
        isDeleted = node->IsSKeyDeleted();
    } else {
        auto node = GetSKeyNode<true>();
        skey = node->skey;
        isDeleted = node->IsSKeyDeleted();
    }
}

// this function should not be called by coroutine function
template <typename SKeyType>
inline void KKVBuiltSegmentDocIterator<SKeyType>::GetCurrentValue(autil::StringView& value)
{
    if (mValueInline) {
        value = mValueData;
    } else {
        assert(mValueDecoder);
        auto node = GetSKeyNode<false>();
        InnerGetValue(node->offset, value);
    }
}

template <typename SKeyType>
inline void KKVBuiltSegmentDocIterator<SKeyType>::InnerGetValue(OnDiskSKeyOffset offset, autil::StringView& value)
{
    uint64_t chunkOffset = 0;
    uint32_t inChunkPosition = 0;
    ResolveSkeyOffset(offset, chunkOffset, inChunkPosition);
    mValueDecoder->ReadRecord(value, chunkOffset, inChunkPosition, mReadOption);
}

template <typename SKeyType>
inline int KKVBuiltSegmentDocIterator<SKeyType>::Compare(const KKVSegmentIterator* other)
{
    assert(other);
    SKeyType leftSkey = mValueInline ? GetSKeyNode<true>()->skey : GetSKeyNode<false>()->skey;
    uint64_t otherSkey;
    bool isDeleted;
    other->GetCurrentSkey(otherSkey, isDeleted);
    SKeyType rightSkey = (SKeyType)otherSkey;
    if (leftSkey == rightSkey) {
        return 0;
    }
    return leftSkey < rightSkey ? -1 : 1;
}

#undef SKEY_NODE_FUNC

template <typename SKeyType>
inline bool
KKVBuiltSegmentDocIterator<SKeyType>::MoveToValidPosition(SearchSKeyContext* skeyContext, uint64_t minimumTsInSecond,
                                                          uint64_t currentTsInSecond, PooledSKeySet& foundSkeys,
                                                          SKeyType& skey, uint32_t& ts)
{
    while (IsValid()) {
        uint64_t curSkey = 0;
        bool isDeleted = false;
        uint32_t curTs = GetCurrentTs();
        GetCurrentSkey(curSkey, isDeleted);
        auto status = ValidateSKey(skeyContext, minimumTsInSecond, currentTsInSecond, foundSkeys, (SKeyType)curSkey,
                                   curTs, isDeleted);
        switch (status) {
        case FILTERED:
            if (!MoveToNext() && !HasHitLastNode()) {
                future_lite::interface::syncAwait(SwitchChunk());
                continue;
            }
            break;
        case VALID:
            skey = curSkey;
            ts = curTs;
            return true;
        case INVALID:
            return false;
        default:
            assert(false);
            return false;
        }
    }

    return false;
}

template <typename SKeyType>
inline KKVSegmentIterator::SKeyStatus KKVBuiltSegmentDocIterator<SKeyType>::ValidateSKey(
    SKeySearchContext<SKeyType>* skeyContext, uint64_t minimumTsInSecond, uint64_t currentTsInSecond,
    typename SKeyHashContainerTraits<SKeyType>::PooledSKeySet& foundSkeys, SKeyType curSkey, uint32_t skeyTs,
    bool isDeleted)
{
    mHasSkeyContext = skeyContext;
    if (skeyTs < minimumTsInSecond) {
        return FILTERED;
    }

    if (mHasSkeyContext && skeyContext) {
        const auto& requiredSkeys = skeyContext->GetRequiredSKeys();
        if (requiredSkeys.size() == foundSkeys.size() ||
            (!mKeepSortSequence && (SKeyType)curSkey > skeyContext->GetMaxRequiredSKey())) {
            return INVALID;
        }
        if (requiredSkeys.find(curSkey) == requiredSkeys.end()) {
            return FILTERED;
        }
    }

    bool found = false;
    if (!mIsOnlySeg) {
        if (mIsLastSeg && !skeyContext) {
            // last segment only find, not insert
            found = (foundSkeys.find(curSkey) != foundSkeys.end());
        } else {
            found = !(foundSkeys.insert(curSkey).second);
        }
    }
    if (isDeleted || found) {
        return FILTERED;
    }

    if (mStoreExpireTime && SKeyExpired(*mKKVIndexConfig, currentTsInSecond, skeyTs, GetCurrentExpireTime())) {
        return FILTERED;
    }

    return VALID;
}

template <typename SKeyType>
inline void KKVBuiltSegmentDocIterator<SKeyType>::CollectAllCode(bool hasSearchSKeyContext, bool isOnlySeg,
                                                                 bool hasMetricsCollector,
                                                                 codegen::AllCodegenInfo& allCodegenInfos)
{
    codegen::CodegenInfo codegenInfo;
    STATIC_INIT_CODEGEN_INFO(KKVBuiltSegmentDocIterator, true, codegenInfo);
    STATIC_COLLECT_CONST_MEM_VAR(mIsOnlySeg, isOnlySeg, codegenInfo);
    STATIC_COLLECT_CONST_MEM_VAR(mHasSkeyContext, hasSearchSKeyContext, codegenInfo);
    STATIC_COLLECT_CONST_MEM_VAR(mHasMetricsCollector, hasMetricsCollector, codegenInfo);
    allCodegenInfos.AppendAndRename(codegenInfo);
}

template <typename SKeyType>
inline bool
KKVBuiltSegmentDocIterator<SKeyType>::TEST_collectCodegenResult(codegen::CodegenObject::CodegenCheckers& checkers)
{
    codegen::CodegenCheckerPtr checker(new codegen::CodegenChecker);
    COLLECT_CONST_MEM(checker, mIsOnlySeg);
    COLLECT_CONST_MEM(checker, mHasSkeyContext);
    COLLECT_CONST_MEM(checker, mHasMetricsCollector);
    checkers["KKVBuiltSegmentDocIterator"] = checker;
    return true;
}
}} // namespace indexlib::index

#endif
