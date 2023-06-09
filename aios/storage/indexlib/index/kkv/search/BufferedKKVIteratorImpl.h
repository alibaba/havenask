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

#include "indexlib/index/kkv/common/KKVDocs.h"
#include "indexlib/index/kkv/common/KKVResultBuffer.h"
#include "indexlib/index/kkv/common/SKeySearchContext.h"
#include "indexlib/index/kkv/search/KKVIteratorImplBase.h"

namespace indexlibv2::index {

template <typename SKeyType>
class BufferedKKVIteratorImpl final : public KKVIteratorImplBase
{
public:
    BufferedKKVIteratorImpl(autil::mem_pool::Pool* pool, PKeyType pkey, KKVDocs&& docs,
                            config::KKVIndexConfig* indexConfig, KVMetricsCollector* metricsCollector)
        : KKVIteratorImplBase(pool, pkey, indexConfig, metricsCollector)
        , _docs(std::move(docs))
    {
        _isValid = _docs.size() > 0;
    }

    BufferedKKVIteratorImpl(autil::mem_pool::Pool* pool) : KKVIteratorImplBase(pool, 0, nullptr, nullptr), _docs(_pool)
    {
    }

    BufferedKKVIteratorImpl(const BufferedKKVIteratorImpl&) = delete;
    BufferedKKVIteratorImpl& operator=(const BufferedKKVIteratorImpl&) = delete;

    BufferedKKVIteratorImpl(BufferedKKVIteratorImpl&& other)
        : KKVIteratorImplBase(std::move(other))
        , _docs(std::move(other._docs))
    {
    }

public:
    void BatchGet(KKVResultBuffer& resultBuffer) override final
    {
        bool firstDuplicated = _docs[0].duplicatedKey;
        resultBuffer.Swap(_docs);
        if (firstDuplicated) {
            resultBuffer.MoveToNext();
        }
        _isValid = false;
    }

private:
    KKVDocs _docs;
};

} // namespace indexlibv2::index
