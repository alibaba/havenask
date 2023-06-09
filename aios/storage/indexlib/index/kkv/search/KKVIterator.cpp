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
#include "indexlib/index/kkv/search/KKVIterator.h"

namespace indexlibv2::index {

KKVIterator::KKVIterator(autil::mem_pool::Pool* pool) 
    : _pool(pool), _resultBuffer(0, 0, pool), _isValid(false) {}

KKVIterator::KKVIterator(autil::mem_pool::Pool* pool, KKVIteratorImplBase* iter, config::KKVIndexConfig* kkvConfig,
                         PlainFormatEncoder* plainFormatEncoder, bool sorted, KKVMetricsCollector* metricsCollector,
                         uint32_t skeyCountLimit)
    : _pool(pool)
    , _iterator(iter)
    , _indexConfig(kkvConfig)
    , _plainFormatEncoder(plainFormatEncoder)
    , _metricsCollector(metricsCollector)
    , _resultBuffer(64, skeyCountLimit, pool)
    , _sorted(sorted)
{
    if (_iterator->IsValid()) {
        _iterator->BatchGet(_resultBuffer);
    }
    _isValid = _resultBuffer.IsValid();
}

KKVIterator::~KKVIterator()
{
    indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _iterator);
    _iterator = nullptr;
    _pool = nullptr;
}

} // namespace indexlibv2::index
