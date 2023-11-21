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
#include "indexlib/index/kkv/kkv_docs.h"
#include "indexlib/index/kkv/kkv_iterator_impl_base.h"
#include "indexlib/index/kkv/search_cache_context.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename SKeyType>
class CachedKKVIteratorImpl final : public KKVIteratorImplBase
{
public:
    CachedKKVIteratorImpl(autil::mem_pool::Pool* pool, PKeyType pKey, KKVDocs docs, KKVIndexOptions* indexOptions,
                          KVMetricsCollector* metricsCollector)
        : KKVIteratorImplBase(pool, pKey, indexOptions, metricsCollector)
        , mDocs(std::move(docs))
        , mCursor(-1)
    {
        MoveToNext();
    }
    CachedKKVIteratorImpl(autil::mem_pool::Pool* pool)
        : KKVIteratorImplBase(pool, 0, nullptr, nullptr)
        , mDocs(mPool)
        , mCursor(0)
    {
    }

    CachedKKVIteratorImpl(const CachedKKVIteratorImpl&) = delete;
    CachedKKVIteratorImpl& operator=(const CachedKKVIteratorImpl&) = delete;

    CachedKKVIteratorImpl(CachedKKVIteratorImpl&& other)
        : KKVIteratorImplBase(std::move(other))
        , mDocs(std::move(other.mDocs))
        , mCursor(std::exchange(other.mCursor, 0))
    {
    }

public:
    bool IsValid() const { return static_cast<size_t>(mCursor) < mDocs.size(); }
    void FillResultBuffer(KKVResultBuffer& resultBuffer) override final
    {
        bool firstDuplicated = mDocs[0].duplicatedKey;
        resultBuffer.swap(mDocs);
        if (firstDuplicated) {
            resultBuffer.MoveToNext();
        }
        resultBuffer.SetEof();
    }
    void MoveToNext()
    {
        do {
            if (static_cast<size_t>(++mCursor) < mDocs.size() && !mDocs[mCursor].duplicatedKey &&
                !mDocs[mCursor].skeyDeleted) {
                break;
            }
        } while (static_cast<size_t>(mCursor) < mDocs.size());
    }

private:
    bool doCollectAllCode() override { return true; }

private:
    KKVDocs mDocs;
    int64_t mCursor;
};
}} // namespace indexlib::index
