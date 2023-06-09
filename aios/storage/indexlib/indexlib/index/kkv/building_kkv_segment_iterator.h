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
#ifndef __INDEXLIB_BUILDING_KKV_SEGMENT_ITERATOR_H
#define __INDEXLIB_BUILDING_KKV_SEGMENT_ITERATOR_H

#include <memory>

#include "autil/MultiValueType.h"
#include "indexlib/codegen/codegen_object.h"
#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_index_options.h"
#include "indexlib/index/kkv/kkv_segment_iterator.h"
#include "indexlib/index/kkv/skey_search_context.h"
#include "indexlib/index/kkv/suffix_key_writer.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename SKeyType>
class BuildingKKVSegmentIterator final : public KKVSegmentIterator
{
private:
    using Base = KKVSegmentIterator;
    typedef SKeySearchContext<SKeyType> SearchSKeyContext;
    typedef SuffixKeyWriter<SKeyType> SKeyWriter;
    DEFINE_SHARED_PTR(SKeyWriter);
    using SKeySet = typename SKeyHashContainerTraits<SKeyType>::SKeySet;
    using PooledSKeySet = typename SKeyHashContainerTraits<SKeyType>::PooledSKeySet;

public:
    BuildingKKVSegmentIterator(autil::mem_pool::Pool* sessionPool)
        : KKVSegmentIterator(sessionPool)
        , mCurrentOffset(INVALID_SKEY_OFFSET)
        , mSkipListPos(INVALID_SKEY_OFFSET)
        , mCurrentSKeyNode(0, 0, 0, UNINITIALIZED_EXPIRE_TIME, INVALID_SKEY_OFFSET)
        , mPKeyDeletedTs(0)
        , mHasPKeyDeleted(false)
        , mFixedValueLen(-1)
        , mStoreExpireTime(false)
    {
        mHasSkeyContext = false;
        mIsOnlySeg = false;
    }
    ~BuildingKKVSegmentIterator() {};
    static void CollectAllCode(bool hasSearchSKeyContext, bool isOnlySeg, codegen::AllCodegenInfo& allCodegenInfos);

public:
    void Init(const SKeyWriterPtr& skeyWriter, const ValueWriterPtr& valueWriter, const SKeyListInfo& skeyListInfo,
              const KKVIndexOptions* options);

    void Init(const SKeyWriterPtr& skeyWriter, const ValueWriterPtr& valueWriter, const SKeyListInfo& skeyListInfo,
              const config::KKVIndexConfigPtr& kkvConfig);

    bool HasPKeyDeleted() const override final { return mHasPKeyDeleted; }

    uint32_t GetPKeyDeletedTs() const override final { return mPKeyDeletedTs; }

    void FillResultBuffer(SearchSKeyContext* skeyContext, uint64_t minimumTsInSecond, PooledSKeySet& foundSkeys,
                          KKVResultBuffer& resultBuffer);

    bool IsValid() const override final { return mCurrentOffset != INVALID_SKEY_OFFSET; }

    void FillResultBuffer(SearchSKeyContext* skeyContext, uint64_t minimumTsInSecond, uint64_t currentTsInSecond,
                          PooledSKeySet& foundSkeys, KKVResultBuffer& resultBuffer);

    bool MoveToNext() override final
    {
        if (!IsValid()) {
            return false;
        }
        mCurrentOffset = mCurrentSKeyNode.next;
        if (IsValid()) {
            mCurrentSKeyNode = mSKeyWriter->GetSKeyNode(mCurrentOffset);
            return true;
        }
        return false;
    }

    void GetCurrentSkey(uint64_t& skey, bool& isDeleted) const override final
    {
        assert(IsValid());
        skey = mCurrentSKeyNode.skey;
        isDeleted = (mCurrentSKeyNode.offset == INVALID_VALUE_OFFSET);
    }

    void GetCurrentValue(autil::StringView& value) override final
    {
        assert(IsValid());
        InnerGetValue(value, mCurrentSKeyNode.offset);
    }
    void InnerGetValue(autil::StringView& value, size_t offset)
    {
        char* addr = mValueWriter->GetValue(offset);
        if (mFixedValueLen > 0) {
            value = autil::StringView(addr, mFixedValueLen);
        } else {
            autil::MultiChar multiChar;
            multiChar.init(addr);
            value = autil::StringView(multiChar.data(), multiChar.size());
        }
    }

    uint32_t GetCurrentTs() override final { return mCurrentSKeyNode.timestamp; }

    uint32_t GetCurrentExpireTime() override final { return mCurrentSKeyNode.expireTime; }

    int Compare(const KKVSegmentIterator* other) override final
    {
        assert(other);
        assert(IsValid());
        SKeyType leftSkey = mCurrentSKeyNode.skey;
        uint64_t otherSkey;
        bool isDeleted;
        other->GetCurrentSkey(otherSkey, isDeleted);
        SKeyType rightSkey = (SKeyType)otherSkey;
        if (leftSkey == rightSkey) {
            return 0;
        }
        return leftSkey < rightSkey ? -1 : 1;
    }

    bool SeekBySKey(SKeyType skey);

    void Reset();

    bool HasSkipList() const { return mInitListInfo.blockHeader != INVALID_SKEY_OFFSET; }

    regionid_t GetRegionId() { return mRegionId; }

    bool MoveToValidPosition(SearchSKeyContext* skeyContext, uint64_t minimumTsInSecond, uint64_t currentTsInSecond,
                             PooledSKeySet& foundSkeys, SKeyType& skey, uint32_t& ts);
    void TEST_collectCodegenResult(codegen::CodegenObject::CodegenCheckers& checkers);

private:
    bool MoveToValidSKey(SearchSKeyContext* skeyContext, uint64_t minimumTsInSecond, uint64_t currentTsInSecond,
                         PooledSKeySet& foundSkeys, SKeyType& skey, uint32_t& ts);

    inline SKeyStatus ValidateSKey(SKeySearchContext<SKeyType>* skeyContext, uint64_t minimumTsInSecond,
                                   uint64_t currentTsInSecond,
                                   typename SKeyHashContainerTraits<SKeyType>::PooledSKeySet& foundSkeys, SKeyType skey,
                                   uint32_t ts, bool isDeleted);

private:
    SKeyWriterPtr mSKeyWriter;
    ValueWriterPtr mValueWriter;
    uint32_t mCurrentOffset;
    uint32_t mSkipListPos;
    typename SKeyWriter::SKeyNode mCurrentSKeyNode;
    uint32_t mPKeyDeletedTs;
    bool mHasPKeyDeleted;
    SKeyListInfo mInitListInfo;
    regionid_t mRegionId;
    int32_t mFixedValueLen;
    bool mStoreExpireTime; // TODO: codegen

    // to codegen
    bool mHasSkeyContext;
    bool mIsOnlySeg;

private:
    IE_LOG_DECLARE();
};

// DEFINE_SHARED_PTR(BuildingKKVSegmentIterator);

//////////////////////////////////////////////////////

template <typename SKeyType>
void BuildingKKVSegmentIterator<SKeyType>::Init(const SKeyWriterPtr& skeyWriter, const ValueWriterPtr& valueWriter,
                                                const SKeyListInfo& skeyListInfo, const KKVIndexOptions* options)
{
    mKKVIndexConfig = options->kkvConfig.get();
    mKeepSortSequence = false;
    mInitListInfo = skeyListInfo;
    mSKeyWriter = skeyWriter;
    mValueWriter = valueWriter;
    mRegionId = skeyListInfo.regionId;
    mFixedValueLen = options->fixedValueLen;
    mStoreExpireTime = options->kkvConfig->StoreExpireTime();
    Reset();
}

template <typename SKeyType>
void BuildingKKVSegmentIterator<SKeyType>::Init(const SKeyWriterPtr& skeyWriter, const ValueWriterPtr& valueWriter,
                                                const SKeyListInfo& skeyListInfo,
                                                const config::KKVIndexConfigPtr& kkvConfig)
{
    mKKVIndexConfig = kkvConfig.get();
    mKeepSortSequence = false;
    mInitListInfo = skeyListInfo;
    mSKeyWriter = skeyWriter;
    mValueWriter = valueWriter;
    mRegionId = skeyListInfo.regionId;
    const auto& valueConfig = kkvConfig->GetValueConfig();
    assert(valueConfig);
    mFixedValueLen = valueConfig->GetFixedLength();
    mStoreExpireTime = kkvConfig->StoreExpireTime();
    Reset();
}

template <typename SKeyType>
inline bool BuildingKKVSegmentIterator<SKeyType>::SeekBySKey(SKeyType skey)
{
    if (IsValid() && mCurrentSKeyNode.skey <= skey) {
        return mSKeyWriter->SeekTargetSKey(skey, mCurrentSKeyNode, mSkipListPos, mCurrentOffset);
    }

    Reset();
    return mSKeyWriter->SeekTargetSKey(skey, mCurrentSKeyNode, mSkipListPos, mCurrentOffset);
}

template <typename SKeyType>
inline void BuildingKKVSegmentIterator<SKeyType>::Reset()
{
    mCurrentSKeyNode = typename SKeyWriter::SKeyNode(0, 0, 0, UNINITIALIZED_EXPIRE_TIME, INVALID_SKEY_OFFSET);
    mCurrentOffset = mInitListInfo.skeyHeader;
    mSkipListPos = mInitListInfo.blockHeader;
    assert(IsValid());
    mCurrentSKeyNode = mSKeyWriter->GetSKeyNode(mCurrentOffset);
    if (mCurrentSKeyNode.offset != SKEY_ALL_DELETED_OFFSET) {
        mHasPKeyDeleted = false;
        mPKeyDeletedTs = 0;
        return;
    }
    mHasPKeyDeleted = true;
    mPKeyDeletedTs = mCurrentSKeyNode.timestamp;
    MoveToNext();
}

template <typename SKeyType>
inline void
BuildingKKVSegmentIterator<SKeyType>::FillResultBuffer(SearchSKeyContext* skeyContext, uint64_t minimumTsInSecond,
                                                       uint64_t currentTsInSecond, PooledSKeySet& foundSkeys,
                                                       KKVResultBuffer& resultBuffer)
{
    mHasSkeyContext = skeyContext;
    size_t beginPos = resultBuffer.Size();
    for (size_t i = resultBuffer.Size(); i < resultBuffer.GetCapacity(); i++) {
        if (!IsValid()) {
            break;
        }
        uint64_t skey;
        bool isDeleted;
        GetCurrentSkey(skey, isDeleted);
        resultBuffer.EmplaceBack(KKVDoc(GetCurrentTs(), skey, mCurrentSKeyNode.offset));
        if (resultBuffer.IsFull() || resultBuffer.ReachLimit()) {
            break;
        }
        if (mHasSkeyContext && skeyContext && skeyContext->NeedSeekBySKey()) {
            skeyContext->IncCurrentSeekSKeyPos();
        } else {
            MoveToNext();
        }
        SKeyType _skey;
        uint32_t ts;
        if (!MoveToValidPosition(skeyContext, minimumTsInSecond, currentTsInSecond, foundSkeys, _skey, ts)) {
            break;
        }
    }
    for (size_t i = beginPos; i < resultBuffer.Size(); i++) {
        InnerGetValue(resultBuffer[i].value, resultBuffer[i].offset);
    }
}

template <typename SKeyType>
inline bool BuildingKKVSegmentIterator<SKeyType>::MoveToValidSKey(SearchSKeyContext* skeyContext,
                                                                  uint64_t minimumTsInSecond,
                                                                  uint64_t currentTsInSecond, PooledSKeySet& foundSkeys,
                                                                  SKeyType& skey, uint32_t& ts)
{
    assert(skeyContext);
    auto& sortedRequiredSkeys = skeyContext->GetSortedRequiredSKeys();
    while (skeyContext->GetCurrentSeekSKeyPos() < sortedRequiredSkeys.size() && IsValid()) {
        bool seek = SeekBySKey(sortedRequiredSkeys[skeyContext->GetCurrentSeekSKeyPos()]);
        if (seek) {
            SKeyType curSkey = 0;
            uint32_t curTs = 0;
            bool isDeleted = false;
            uint64_t tmpSkey;
            GetCurrentSkey(tmpSkey, isDeleted);
            curTs = GetCurrentTs();
            curSkey = tmpSkey;
            auto status =
                ValidateSKey(NULL, minimumTsInSecond, currentTsInSecond, foundSkeys, curSkey, curTs, isDeleted);
            if (status == VALID) {
                skey = curSkey;
                ts = curTs;
                return true;
            }
        }
        skeyContext->IncCurrentSeekSKeyPos();
    }
    return false;
}

template <typename SKeyType>
inline bool
BuildingKKVSegmentIterator<SKeyType>::MoveToValidPosition(SearchSKeyContext* skeyContext, uint64_t minimumTsInSecond,
                                                          uint64_t currentTsInSecond, PooledSKeySet& foundSkeys,
                                                          SKeyType& skey, uint32_t& ts)
{
    if (mHasSkeyContext && skeyContext && skeyContext->NeedSeekBySKey()) {
        return MoveToValidSKey(skeyContext, minimumTsInSecond, currentTsInSecond, foundSkeys, skey, ts);
    }

    while (IsValid()) {
        bool isDeleted = false;
        uint64_t _skey;
        GetCurrentSkey(_skey, isDeleted);
        skey = _skey;
        ts = GetCurrentTs();
        auto status = ValidateSKey(skeyContext, minimumTsInSecond, currentTsInSecond, foundSkeys, skey, ts, isDeleted);
        switch (status) {
        case FILTERED:
            MoveToNext();
            break;
        case VALID:
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
inline KKVSegmentIterator::SKeyStatus BuildingKKVSegmentIterator<SKeyType>::ValidateSKey(
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
inline void BuildingKKVSegmentIterator<SKeyType>::CollectAllCode(bool hasSearchSKeyContext, bool isOnlySeg,
                                                                 codegen::AllCodegenInfo& allCodegenInfos)
{
    codegen::CodegenInfo codegenInfo;
    STATIC_INIT_CODEGEN_INFO(BuildingKKVSegmentIterator, true, codegenInfo);
    codegenInfo.DefineConstMemberVar("mIsOnlySeg", isOnlySeg ? "true" : "false");
    codegenInfo.DefineConstMemberVar("mHasSkeyContext", hasSearchSKeyContext ? "true" : "false");
    allCodegenInfos.AppendAndRename(codegenInfo);
}

template <typename SKeyType>
inline void
BuildingKKVSegmentIterator<SKeyType>::TEST_collectCodegenResult(codegen::CodegenObject::CodegenCheckers& checkers)
{
    codegen::CodegenCheckerPtr checker(new codegen::CodegenChecker);
    COLLECT_CONST_MEM(checker, mIsOnlySeg);
    COLLECT_CONST_MEM(checker, mHasSkeyContext);
    checkers["BuildingKKVSegmentIterator"] = checker;
}

IE_LOG_SETUP_TEMPLATE(index, BuildingKKVSegmentIterator);
}} // namespace indexlib::index

#endif //__INDEXLIB_BUILDING_KKV_SEGMENT_ITERATOR_H
