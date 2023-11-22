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
#include "indexlib/index/kkv/built_kkv_segment_reader.h"
#include "indexlib/index/kkv/kkv_built_segment_doc_iterator.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/kkv_index_options.h"
#include "indexlib/index/kkv/skey_search_context.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"

namespace indexlib { namespace index {

template <typename SKeyType>
class BuiltKKVIterator
{
private:
    typedef SKeySearchContext<SKeyType> SearchSKeyContext;
    typedef BuiltKKVSegmentReader<SKeyType> BuiltSegReaderTyped;
    DEFINE_SHARED_PTR(BuiltSegReaderTyped);
    typedef KKVBuiltSegmentDocIterator<SKeyType> BuiltKKVSegIter;
    using SKeySet = typename SKeyHashContainerTraits<SKeyType>::SKeySet;
    using PooledSKeySet = typename SKeyHashContainerTraits<SKeyType>::PooledSKeySet;

public:
    BuiltKKVIterator(autil::mem_pool::Pool* pool, KKVIndexOptions* indexOptions, PKeyType pKey,
                     SearchSKeyContext* skeyContext, PooledSKeySet& foundSKeys,
                     const std::vector<BuiltSegReaderTypedPtr>& builtSegReaders, uint64_t minimumTsInSecond,
                     uint64_t incTsInSecond, uint32_t currentTsInSecond, KVMetricsCollector* metricsCollector);

    ~BuiltKKVIterator();

public:
    bool IsValid() const { return mCursor >= 0; }
    bool HasPKeyDeleted() const { return mHasPKeyDeleted; }
    void MoveToNext();
    void GetCurrentValue(autil::StringView& value);
    uint64_t GetCurrentTimestamp() { return mCurrentDocTs; }
    SKeyType GetCurrentSkey() { return mCurrentSKey; }
    void MoveToValidPosition();

    void DisableLastSegmentOptimize()
    {
        if (mCurrentBuiltSegIter) {
            mCurrentBuiltSegIter->SetIsLastSegment(false);
        }
        mDisableLastSegOptimize = true;
    }
    void FillResultBuffer(KKVResultBuffer& resultBuffer);

    static void CollectAllCode(bool hasSkeyContext, bool isOnlySeg, bool hasMetricsCollector,
                               codegen::AllCodegenInfo& allCodegenInfos);
    void TEST_collectCodegenResult(codegen::CodegenObject::CodegenCheckers& checkers);

private:
    void SwitchIterator();
    bool IsLastSegment();

    bool IsObseleteSegment(int32_t idx)
    {
        assert(idx >= 0 && idx < (int32_t)mBuiltSegReaders.size());
        return mBuiltSegReaders[idx]->GetTimestampInSecond() < mMinimumTsInSecond;
    }

    template <typename Iter>
    bool HasPKeyDeletedByTime(Iter* iter, uint64_t tsThresholdInSecond)
    {
        if (!iter->HasPKeyDeleted()) {
            return false;
        }
        auto deleteTs = iter->GetPKeyDeletedTs();
        return deleteTs >= tsThresholdInSecond;
    }

private:
    PKeyType mPKey;
    BuiltKKVSegIter* mCurrentBuiltSegIter;
    SearchSKeyContext* mSearchSKeyContext;
    PooledSKeySet& mFoundSkeys;
    SKeyType mCurrentSKey;
    uint32_t mCurrentMinTsInSecond;
    uint32_t mCurrentIncTsThresholdInSecond;
    uint32_t mCurrentTsInSecond;
    uint32_t mCurrentDocTs;
    int32_t mCursor;
    bool mHasPKeyDeleted;
    bool mDisableLastSegOptimize;
    uint32_t mMinimumTsInSecond;
    const std::vector<BuiltSegReaderTypedPtr>& mBuiltSegReaders;
    KKVIndexOptions* mIndexOptions;
    KVMetricsCollector* mMetricsCollector;
    autil::mem_pool::Pool* mPool;

    // to codegen
    bool mHasSearchSkeyContext;

private:
    IE_LOG_DECLARE();
};

///////////////////////////////////////////
template <typename SKeyType>
inline BuiltKKVIterator<SKeyType>::BuiltKKVIterator(autil::mem_pool::Pool* pool, KKVIndexOptions* indexOptions,
                                                    PKeyType pKey, SearchSKeyContext* skeyContext,
                                                    PooledSKeySet& foundSKeys,
                                                    const std::vector<BuiltSegReaderTypedPtr>& builtSegReaders,
                                                    uint64_t minimumTsInSecond, uint64_t incTsInSecond,
                                                    uint32_t currentTsInSecond, KVMetricsCollector* metricsCollector)
    : mPKey(pKey)
    , mCurrentBuiltSegIter(NULL)
    , mSearchSKeyContext(skeyContext)
    , mFoundSkeys(foundSKeys)
    , mCurrentSKey(SKeyType())
    , mCurrentMinTsInSecond((uint32_t)std::max(minimumTsInSecond, incTsInSecond))
    , mCurrentIncTsThresholdInSecond((uint32_t)incTsInSecond)
    , mCurrentTsInSecond(currentTsInSecond)
    , mCurrentDocTs(INVALID_TIMESTAMP)
    , mCursor((int32_t)builtSegReaders.size() - 1)
    , mHasPKeyDeleted(false)
    , mDisableLastSegOptimize(false)
    , mMinimumTsInSecond((uint32_t)minimumTsInSecond)
    , mBuiltSegReaders(builtSegReaders)
    , mIndexOptions(indexOptions)
    , mMetricsCollector(metricsCollector)
    , mPool(pool)
{
    mHasSearchSkeyContext = mSearchSKeyContext;
    SwitchIterator();
}

template <typename SKeyType>
inline BuiltKKVIterator<SKeyType>::~BuiltKKVIterator()
{
    if (mCurrentBuiltSegIter) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mCurrentBuiltSegIter);
        mCurrentBuiltSegIter = NULL;
    }
}

template <typename SKeyType>
void BuiltKKVIterator<SKeyType>::FillResultBuffer(KKVResultBuffer& resultBuffer)
{
    while (true) {
        if (!IsValid()) {
            return;
        }
        mCurrentBuiltSegIter->FillResultBuffer(mSearchSKeyContext, mCurrentMinTsInSecond, mCurrentTsInSecond,
                                               mFoundSkeys, resultBuffer);
        if (resultBuffer.IsFull() || resultBuffer.ReachLimit()) {
            break;
        }
        MoveToNext();
        MoveToValidPosition();
    }
}

template <typename SKeyType>
inline void BuiltKKVIterator<SKeyType>::MoveToValidPosition()
{
    while (mCursor >= 0) {
        if (mCurrentBuiltSegIter->MoveToValidPosition(mSearchSKeyContext, mCurrentMinTsInSecond, mCurrentTsInSecond,
                                                      mFoundSkeys, mCurrentSKey, mCurrentDocTs)) {
            return;
        }
        if (HasPKeyDeletedByTime(mCurrentBuiltSegIter, mCurrentIncTsThresholdInSecond)) {
            mHasPKeyDeleted = true;
            mCursor = -1;
        } else if (mHasSearchSkeyContext && mSearchSKeyContext->GetRequiredSKeyCount() == mFoundSkeys.size()) {
            mCursor = -1;
        } else {
            mCursor--;
        }
        SwitchIterator();
    }
}

template <typename SKeyType>
inline void BuiltKKVIterator<SKeyType>::MoveToNext()
{
    if (mCurrentBuiltSegIter) {
        if (!mCurrentBuiltSegIter->MoveToNext() && !mCurrentBuiltSegIter->HasHitLastNode()) {
            assert(!future_lite::interface::USE_COROUTINES);
            future_lite::interface::syncAwait(mCurrentBuiltSegIter->SwitchChunk());
        }
    }
}

template <typename SKeyType>
inline void BuiltKKVIterator<SKeyType>::GetCurrentValue(autil::StringView& value)
{
    if (mCurrentBuiltSegIter) {
        return mCurrentBuiltSegIter->GetCurrentValue(value);
    }
    assert(false);
}

template <typename SKeyType>
inline void BuiltKKVIterator<SKeyType>::SwitchIterator()
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mCurrentBuiltSegIter);
    mCurrentBuiltSegIter = NULL;
    while (mCursor >= 0) {
        const BuiltSegReaderTypedPtr& reader = mBuiltSegReaders[mCursor];
        if (IsObseleteSegment(mCursor)) {
            mCursor--;
            continue;
        }

        mCurrentBuiltSegIter = future_lite::interface::syncAwait(
            reader->template Lookup<BuiltKKVSegIter>(mPKey, mPool, mIndexOptions, mMetricsCollector));
        if (mCurrentBuiltSegIter) {
            if (!reader->IsRealtimeSegment()) {
                mCurrentMinTsInSecond = mMinimumTsInSecond;
                mCurrentIncTsThresholdInSecond = 0;
            }
            mCurrentBuiltSegIter->SetIsLastSegment(IsLastSegment());
            return;
        }
        mCursor--;
    }
}

template <typename SKeyType>
inline bool BuiltKKVIterator<SKeyType>::IsLastSegment()
{
    if (mDisableLastSegOptimize) {
        return false;
    }

    if (HasPKeyDeletedByTime(mCurrentBuiltSegIter, mCurrentIncTsThresholdInSecond) || mCursor <= 0) {
        return true;
    }

    int32_t nextSegIdx = mCursor - 1;
    for (int32_t i = nextSegIdx; i >= 0; i--) {
        if (!IsObseleteSegment(i)) {
            return false;
        }
    }
    return true;
}

template <typename SKeyType>
inline void BuiltKKVIterator<SKeyType>::CollectAllCode(bool hasSkeyContext, bool isOnlySeg, bool hasMetricsCollector,
                                                       codegen::AllCodegenInfo& allCodegenInfos)
{
    codegen::CodegenInfo codegenInfo;
    STATIC_INIT_CODEGEN_INFO(BuiltKKVIterator, true, codegenInfo);
    BuiltKKVSegIter::CollectAllCode(hasSkeyContext, isOnlySeg, hasMetricsCollector, allCodegenInfos);
    codegenInfo.DefineConstMemberVar("mHasSearchSkeyContext", hasSkeyContext ? "true" : "false");
    STATIC_COLLECT_TYPE_REDEFINE(
        BuiltKKVSegIter, allCodegenInfos.GetTargetClassName("KKVBuiltSegmentDocIterator") + "<SKeyType>", codegenInfo);
    allCodegenInfos.AppendAndRename(codegenInfo);
}

template <typename SKeyType>
inline void BuiltKKVIterator<SKeyType>::TEST_collectCodegenResult(codegen::CodegenObject::CodegenCheckers& checkers)
{
    codegen::CodegenCheckerPtr checker(new codegen::CodegenChecker);
    COLLECT_CONST_MEM(checker, mHasSearchSkeyContext);
    COLLECT_TYPE_DEFINE(checker, BuiltKKVSegIter);
    checkers["BuiltKKVIterator"] = checker;
    if (mCurrentBuiltSegIter) {
        mCurrentBuiltSegIter->TEST_collectCodegenResult(checkers);
    }
}
}} // namespace indexlib::index
