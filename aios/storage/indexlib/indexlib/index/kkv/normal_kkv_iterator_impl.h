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
#ifndef __INDEXLIB_NORMAL_KKV_ITERATOR_IMPL_H
#define __INDEXLIB_NORMAL_KKV_ITERATOR_IMPL_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/building_kkv_segment_iterator.h"
#include "indexlib/index/kkv/building_kkv_segment_reader.h"
#include "indexlib/index/kkv/built_kkv_iterator.h"
#include "indexlib/index/kkv/built_kkv_segment_reader.h"
#include "indexlib/index/kkv/kkv_iterator_impl_base.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename SKeyType>
class NormalKKVIteratorImpl final : public KKVIteratorImplBase
{
private:
    typedef BuildingKKVSegmentReader<SKeyType> BuildingSegReaderTyped;
    typedef std::shared_ptr<BuildingSegReaderTyped> BuildingSegReaderTypedPtr;
    typedef BuiltKKVSegmentReader<SKeyType> BuiltSegReaderTyped;
    DEFINE_SHARED_PTR(BuiltSegReaderTyped);

    typedef BuildingKKVSegmentIterator<SKeyType> BuildingKKVSegIter;
    typedef BuiltKKVIterator<SKeyType> BuiltKKVIter;
    typedef SKeySearchContext<SKeyType> SearchSKeyContext;

    using SKeySet = typename SKeyHashContainerTraits<SKeyType>::SKeySet;
    using PooledSKeySet = typename SKeyHashContainerTraits<SKeyType>::PooledSKeySet;

public:
    template <typename Alloc>
    NormalKKVIteratorImpl(autil::mem_pool::Pool* pool, KKVIndexOptions* indexOptions, PKeyType pKey,
                          const std::vector<uint64_t, Alloc>& suffixKeyHashVec,
                          const std::vector<BuiltSegReaderTypedPtr>& builtSegReaders,
                          const std::vector<BuildingSegReaderTypedPtr>& buildingSegReaders,
                          uint64_t currentTimeInSecond, KVMetricsCollector* metricsCollector = NULL);

    ~NormalKKVIteratorImpl();

public:
    void MoveToNext();
    inline bool IsValid() const { return mStatus != DONE; }
    inline void FillResultBuffer(KKVResultBuffer& resultBuffer) override final;

    static void CollectAllCode(bool hasMetricsCollector, bool hasSearchSKeyContext,
                               const std::vector<std::vector<BuiltSegReaderTypedPtr>>& builtSegReaders,
                               const std::vector<BuildingSegReaderTypedPtr>& buildingSegReaders,
                               codegen::AllCodegenInfo& allCodegenInfos);
    void TEST_collectCodegenResult(codegen::CodegenObject::CodegenCheckers& checkers, std::string id) override;

private:
    void MoveToValidPosition();
    void SwitchIterator();

    template <typename Iter>
    bool HasPKeyDeletedByTime(Iter* iter, uint64_t tsThresholdInSecond)
    {
        if (!iter->HasPKeyDeleted()) {
            return false;
        }
        auto deleteTs = iter->GetPKeyDeletedTs();
        return deleteTs >= tsThresholdInSecond;
    }
    bool doCollectAllCode() override;

private:
    PooledSKeySet mFoundSkeys;
    BuildingKKVSegIter* mBuildingSegIter;
    BuiltKKVIter* mBuiltKKVIter;
    uint32_t mMinimumTsInSecond;
    uint32_t mCurrentTsInSecond;
    uint32_t mCurBuildingSegIdx;
    uint32_t mIncTsInSecond;
    IterStatus mStatus;
    SearchSKeyContext* mSearchSKeyContext;
    const std::vector<BuiltSegReaderTypedPtr>& mBuiltSegReaders;
    const std::vector<BuildingSegReaderTypedPtr>& mBuildingSegReaders;

    // to codegen
    bool mHasMetricsCollector;
    bool mHasSearchSKeyContext;
    bool mHasBuilding;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, NormalKKVIteratorImpl);

////////////////////////////////////////

template <typename SKeyType>
template <typename Alloc>
inline NormalKKVIteratorImpl<SKeyType>::NormalKKVIteratorImpl(
    autil::mem_pool::Pool* pool, KKVIndexOptions* indexOptions, PKeyType pKey,
    const std::vector<uint64_t, Alloc>& suffixKeyHashVec, const std::vector<BuiltSegReaderTypedPtr>& builtSegReaders,
    const std::vector<BuildingSegReaderTypedPtr>& buildingSegReaders, uint64_t currentTimeInSecond,
    KVMetricsCollector* metricsCollector)
    : KKVIteratorImplBase(pool, pKey, indexOptions, metricsCollector)
    , mFoundSkeys(autil::mem_pool::pool_allocator<SKeyType>(mPool))
    , mBuildingSegIter(NULL)
    , mBuiltKKVIter(NULL)
    , mMinimumTsInSecond(0)
    , mCurrentTsInSecond(static_cast<uint32_t>(currentTimeInSecond))
    , mCurBuildingSegIdx(0)
    , mIncTsInSecond(0)
    , mStatus(NONE)
    , mSearchSKeyContext(NULL)
    , mBuiltSegReaders(builtSegReaders)
    , mBuildingSegReaders(buildingSegReaders)
{
    mHasMetricsCollector = metricsCollector;
    mHasSearchSKeyContext = false;
    mHasBuilding = buildingSegReaders.size() > 0;
    if (currentTimeInSecond > indexOptions->ttl && !indexOptions->kvConfig->StoreExpireTime()) {
        mMinimumTsInSecond = currentTimeInSecond - indexOptions->ttl;
    }

    mIncTsInSecond = (uint32_t)mIndexOptions->incTsInSecond;
    if (!suffixKeyHashVec.empty()) {
        mSearchSKeyContext = IE_POOL_COMPATIBLE_NEW_CLASS(mPool, SearchSKeyContext, mPool);
        mSearchSKeyContext->Init(suffixKeyHashVec);
        mFoundSkeys.reserve(suffixKeyHashVec.size());
        mHasSearchSKeyContext = true;
    }

    if (mHasMetricsCollector && mMetricsCollector) {
        mMetricsCollector->BeginMemTableQuery();
        mMetricsCollector->ResetBlockCounter();
    }

    SwitchIterator();
    MoveToValidPosition();
}

template <typename SKeyType>
inline NormalKKVIteratorImpl<SKeyType>::~NormalKKVIteratorImpl()
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mBuildingSegIter);
    IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mBuiltKKVIter);
    IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mSearchSKeyContext);
}

template <typename SKeyType>
void NormalKKVIteratorImpl<SKeyType>::FillResultBuffer(KKVResultBuffer& resultBuffer)
{
    while (true) {
        if (mStatus == BUILDING) {
            mBuildingSegIter->FillResultBuffer(mSearchSKeyContext, std::max(mMinimumTsInSecond, mIncTsInSecond),
                                               mCurrentTsInSecond, mFoundSkeys, resultBuffer);
        } else if (mStatus == BUILT) {
            mBuiltKKVIter->FillResultBuffer(resultBuffer);
        } else if (mStatus == DONE) {
            resultBuffer.SetEof();
            if (mHasMetricsCollector && mMetricsCollector) {
                mMetricsCollector->IncResultCount(resultBuffer.Size());
            }
            break;
        }
        if (resultBuffer.ReachLimit()) {
            mStatus = DONE;
            resultBuffer.SetEof();
            if (mHasMetricsCollector && mMetricsCollector) {
                mMetricsCollector->IncResultCount(resultBuffer.Size());
            }
            break;
        }
        if (resultBuffer.IsFull()) {
            MoveToNext();
            if (mHasMetricsCollector && mMetricsCollector) {
                mMetricsCollector->IncResultCount(resultBuffer.Size());
            }
            break;
        }
        MoveToNext();
    }
}

template <typename SKeyType>
void NormalKKVIteratorImpl<SKeyType>::MoveToNext()
{
    if (mStatus == BUILDING) {
        if (mHasSearchSKeyContext && mSearchSKeyContext->NeedSeekBySKey()) {
            mSearchSKeyContext->IncCurrentSeekSKeyPos();
        } else {
            mBuildingSegIter->MoveToNext();
        }
    } else if (mStatus == BUILT) {
        mBuiltKKVIter->MoveToNext();
    }
    MoveToValidPosition();
}

template <typename SKeyType>
void NormalKKVIteratorImpl<SKeyType>::MoveToValidPosition()
{
    while (mHasBuilding && mStatus == BUILDING) {
        assert(mBuildingSegIter);
        uint32_t ts;
        SKeyType skey;
        bool hasValidSKey =
            mBuildingSegIter->MoveToValidPosition(mSearchSKeyContext, std::max(mMinimumTsInSecond, mIncTsInSecond),
                                                  mCurrentTsInSecond, mFoundSkeys, skey, ts);
        if (hasValidSKey) {
            return;
        }
        if (HasPKeyDeletedByTime(mBuildingSegIter, mIncTsInSecond)) {
            mStatus = DONE;
            return;
        }
        if (mHasSearchSKeyContext && mSearchSKeyContext->MatchAllRequiredSKeys(mFoundSkeys.size())) {
            mStatus = DONE;
            return;
        }
        SwitchIterator();
    }

    if (mStatus == BUILT) {
        mBuiltKKVIter->MoveToValidPosition();
        if (!mBuiltKKVIter->IsValid()) {
            mStatus = DONE;
            return;
        }
    }
}

template <typename SKeyType>
void NormalKKVIteratorImpl<SKeyType>::SwitchIterator()
{
    if (mHasSearchSKeyContext) {
        mSearchSKeyContext->ResetSKeySKipInfo();
    }
    if (mStatus == NONE && !mHasBuilding) {
        mStatus = BUILT;
        if (mHasMetricsCollector && mMetricsCollector) {
            mMetricsCollector->BeginSSTableQuery();
        }
        mBuiltKKVIter = IE_POOL_COMPATIBLE_NEW_CLASS(
            mPool, BuiltKKVIter, mPool, mIndexOptions, mPKey, mSearchSKeyContext, mFoundSkeys, mBuiltSegReaders,
            mMinimumTsInSecond, mIncTsInSecond, mCurrentTsInSecond, mMetricsCollector);
        return;
    }

    if (mHasBuilding && mStatus == NONE) {
        mCurBuildingSegIdx = 0;
        mStatus = BUILDING;
        while (mCurBuildingSegIdx < (uint32_t)mBuildingSegReaders.size()) {
            mBuildingSegIter = mBuildingSegReaders[mCurBuildingSegIdx++]->template Lookup<BuildingKKVSegIter>(
                mPKey, mPool, mIndexOptions);

            if (!mBuildingSegIter) {
                continue;
            }

            if (mHasSearchSKeyContext) {
                mSearchSKeyContext->UpdateNeedSeekBySKey(mBuildingSegIter->HasSkipList());
            }
            return;
        }
    }

    if (mHasBuilding && mStatus == BUILDING) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mBuildingSegIter);
        mBuildingSegIter = NULL;
        while (mCurBuildingSegIdx < (uint32_t)mBuildingSegReaders.size()) {
            mBuildingSegIter = mBuildingSegReaders[mCurBuildingSegIdx++]->template Lookup<BuildingKKVSegIter>(
                mPKey, mPool, mIndexOptions);
            if (!mBuildingSegIter) {
                continue;
            }

            if (mHasSearchSKeyContext) {
                mSearchSKeyContext->UpdateNeedSeekBySKey(mBuildingSegIter->HasSkipList());
            }
            // MoveToValidPosition();
            return;
        }

        if (mHasMetricsCollector && mMetricsCollector) {
            mMetricsCollector->BeginSSTableQuery();
        }
        mStatus = BUILT;
        mBuiltKKVIter = IE_POOL_COMPATIBLE_NEW_CLASS(
            mPool, BuiltKKVIter, mPool, mIndexOptions, mPKey, mSearchSKeyContext, mFoundSkeys, mBuiltSegReaders,
            mMinimumTsInSecond, mIncTsInSecond, mCurrentTsInSecond, mMetricsCollector);
        return;
    }

    if (mStatus == BUILT) {
        mStatus = DONE;
        IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mBuiltKKVIter);
        mBuiltKKVIter = NULL;
    }
}

template <typename SKeyType>
bool NormalKKVIteratorImpl<SKeyType>::doCollectAllCode()
{
    assert(false);
    return false;
}

template <typename SKeyType>
inline void
NormalKKVIteratorImpl<SKeyType>::CollectAllCode(bool hasMetricsCollector, bool hasSearchSKeyContext,
                                                const std::vector<std::vector<BuiltSegReaderTypedPtr>>& builtSegReaders,
                                                const std::vector<BuildingSegReaderTypedPtr>& buildingSegReaders,
                                                codegen::AllCodegenInfo& allCodegenInfos)
{
    codegen::CodegenInfo codegenInfo;
    STATIC_INIT_CODEGEN_INFO(NormalKKVIteratorImpl, true, codegenInfo);
    bool hasBuilding = !buildingSegReaders.empty();
    STATIC_COLLECT_CONST_MEM_VAR(mHasBuilding, hasBuilding, codegenInfo);
    STATIC_COLLECT_CONST_MEM_VAR(mHasSearchSKeyContext, hasSearchSKeyContext, codegenInfo);
    STATIC_COLLECT_CONST_MEM_VAR(mHasMetricsCollector, hasMetricsCollector, codegenInfo);

    bool isOnlySeg = true;
    for (auto& segReaders : builtSegReaders) {
        if (segReaders.size() + buildingSegReaders.size() > 1) {
            isOnlySeg = false;
            break;
        }
    }

    if (hasBuilding) {
        BuildingKKVSegIter::CollectAllCode(hasSearchSKeyContext, isOnlySeg, allCodegenInfos);
        STATIC_COLLECT_TYPE_REDEFINE(BuildingKKVSegIter,
                                     allCodegenInfos.GetTargetClassName("BuildingKKVSegmentIterator") + "<SKeyType>",
                                     codegenInfo);
    }

    {
        BuiltKKVIter::CollectAllCode(hasSearchSKeyContext, isOnlySeg, hasMetricsCollector, allCodegenInfos);
        STATIC_COLLECT_TYPE_REDEFINE(
            BuiltKKVIter, allCodegenInfos.GetTargetClassName("BuiltKKVIterator") + "<SKeyType>", codegenInfo);
    }
    allCodegenInfos.AppendAndRename(codegenInfo);
}

template <typename SKeyType>
void NormalKKVIteratorImpl<SKeyType>::TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id)
{
    codegen::CodegenCheckerPtr checker(new codegen::CodegenChecker);
    COLLECT_CONST_MEM(checker, mHasBuilding);
    COLLECT_CONST_MEM(checker, mHasSearchSKeyContext);
    COLLECT_CONST_MEM(checker, mHasMetricsCollector);
    COLLECT_TYPE_DEFINE(checker, BuildingKKVSegIter);
    COLLECT_TYPE_DEFINE(checker, BuiltKKVIter);
    checkers["NormalKKVIteratorImpl"] = checker;
    if (mBuildingSegIter) {
        mBuildingSegIter->TEST_collectCodegenResult(checkers);
    }
    if (mBuiltKKVIter) {
        mBuiltKKVIter->TEST_collectCodegenResult(checkers);
    }
}
}} // namespace indexlib::index

#endif //__INDEXLIB_NORMAL_KKV_ITERATOR_IMPL_H
