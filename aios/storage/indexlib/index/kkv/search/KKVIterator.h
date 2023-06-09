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

#include "autil/mem_pool/Pool.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/common/field_format/pack_attribute/PlainFormatEncoder.h"
#include "indexlib/index/kkv/common/KKVMetricsCollector.h"
#include "indexlib/index/kkv/common/KKVResultBuffer.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/search/KKVIteratorImplBase.h"

namespace indexlibv2::index {

class KKVIterator
{
public:
    KKVIterator(autil::mem_pool::Pool* pool); // for empty result
    KKVIterator(autil::mem_pool::Pool* pool, KKVIteratorImplBase* iter, config::KKVIndexConfig* kkvConfig,
                PlainFormatEncoder* plainFormatEncoder, bool sorted, KKVMetricsCollector* metricsCollector,
                uint32_t skeyCountLimit);

    ~KKVIterator();

public:
    bool IsValid() const { return _isValid; }
    void MoveToNext()
    {
        assert(_isValid);
        _resultBuffer.MoveToNext();
        _isValid = _resultBuffer.IsValid();
        if (_isValid) {
            return;
        }
        if (_resultBuffer.ReachLimit()) {
            return;
        }
        if (!_iterator->IsValid()) {
            return;
        }
        _resultBuffer.Clear();
        _iterator->BatchGet(_resultBuffer);
        _isValid = _resultBuffer.IsValid();
    }
    void GetCurrentValue(autil::StringView& value)
    {
        _resultBuffer.GetCurrentValue(value);
        if (_plainFormatEncoder && !_resultBuffer.IsCurrentDocInCache()) { // in cache doc already decoded
            bool ret = _plainFormatEncoder->Decode(value, _pool, value);
            assert(ret);
            (void)ret;
        }
    }
    uint64_t GetCurrentTimestamp() { return autil::TimeUtility::sec2us(_resultBuffer.GetCurrentTimestamp()); }
    uint64_t GetCurrentSkey() { return _resultBuffer.GetCurrentSkey(); }

    // if skey value is same with skey hash value, skey will not store in value
    bool IsOptimizeStoreSKey() const { return _indexConfig->OptimizedStoreSKey(); }
    const std::string& GetSKeyFieldName() const { return _indexConfig->GetSuffixFieldName(); }
    void Finish()
    {
        if (_metricsCollector) {
            _metricsCollector->EndQuery();
        }
    }
    bool IsSorted() const { return _sorted; }

private:
    autil::mem_pool::Pool* _pool = nullptr;
    KKVIteratorImplBase* _iterator = nullptr;
    config::KKVIndexConfig* _indexConfig = nullptr;
    PlainFormatEncoder* _plainFormatEncoder = nullptr;
    KKVMetricsCollector* _metricsCollector = nullptr;
    KKVResultBuffer _resultBuffer;
    bool _sorted = false;
    bool _isValid = false;
};

} // namespace indexlibv2::index
