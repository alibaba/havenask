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
#include <unordered_set>

#include "indexlib/codegen/codegen_object.h"
#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/kkv_index_options.h"
#include "indexlib/index/kkv/skey_search_context.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class KKVIteratorImplBase : public codegen::CodegenObject
{
public:
    enum IterStatus {
        NONE,
        BUILDING,
        BUILT,
        DONE,
    };

    inline KKVIteratorImplBase(autil::mem_pool::Pool* pool, PKeyType pKey, KKVIndexOptions* indexOptions,
                               KVMetricsCollector* metricsCollector)
        : mIndexOptions(indexOptions)
        , mPKey(pKey)
        , mOwnPool(pool == nullptr)
        , mPool((pool == nullptr) ? new autil::mem_pool::Pool : pool) // for psm kkv_searcher, lookup without pool
        , mMetricsCollector(metricsCollector)
    {
    }

    inline KKVIteratorImplBase(KKVIteratorImplBase&& other)
        : mIndexOptions(other.mIndexOptions)
        , mLiteralPKey(other.mLiteralPKey)
        , mPKey(other.mPKey)
        , mOwnPool(std::exchange(other.mOwnPool, false))
        , mPool(std::exchange(other.mPool, nullptr))
        , mMetricsCollector(std::exchange(other.mMetricsCollector, nullptr))
    {
    }

    virtual ~KKVIteratorImplBase()
    {
        if (unlikely(mOwnPool)) {
            DELETE_AND_SET_NULL(mPool);
        }
    }

public:
    bool IsSorted() const { return !mIndexOptions->sortParams.empty(); }
    PKeyType GetPKeyHash() const { return mPKey; }
    virtual void FillResultBuffer(KKVResultBuffer& resultBuffer) = 0;

protected:
    KKVIndexOptions* mIndexOptions;
    PKeyType mLiteralPKey;
    PKeyType mPKey;
    bool mOwnPool;
    autil::mem_pool::Pool* mPool;
    KVMetricsCollector* mMetricsCollector;
};
}} // namespace indexlib::index
