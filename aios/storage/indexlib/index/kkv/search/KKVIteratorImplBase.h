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

#include "indexlib/index/kkv/Types.h"
#include "indexlib/index/kkv/common/KKVMetricsCollector.h"
#include "indexlib/index/kkv/common/SKeySearchContext.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"

namespace indexlibv2::index {

class KKVIteratorImplBase
{
public:
    inline KKVIteratorImplBase(autil::mem_pool::Pool* pool, PKeyType pkey, config::KKVIndexConfig* indexConfig,
                               KKVMetricsCollector* metricsCollector)
        : _indexConfig(indexConfig)
        , _pkey(pkey)
        , _isValid(false)
        , _ownPool(pool == nullptr)
        , _pool((pool == nullptr) ? new autil::mem_pool::Pool : pool)
        , _metricsCollector(metricsCollector)
    {
    }

    inline KKVIteratorImplBase(KKVIteratorImplBase&& other)
        : _indexConfig(other._indexConfig)
        , _pkey(other._pkey)
        , _isValid(false)
        , _ownPool(std::exchange(other._ownPool, false))
        , _pool(std::exchange(other._pool, nullptr))
        , _metricsCollector(std::exchange(other._metricsCollector, nullptr))
    {
    }

    virtual ~KKVIteratorImplBase() {}

public:
    PKeyType GetPKeyHash() const { return _pkey; }
    bool IsValid() const { return _isValid; }
    virtual void BatchGet(KKVResultBuffer& resultBuffer) = 0;

protected:
    config::KKVIndexConfig* _indexConfig = nullptr;
    PKeyType _pkey;
    bool _isValid;
    // for psm kkv_searcher, lookup without pool
    bool _ownPool;
    autil::mem_pool::Pool* _pool = nullptr;
    KKVMetricsCollector* _metricsCollector = nullptr;
};

} // namespace indexlibv2::index
