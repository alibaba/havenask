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
#include "indexlib/index/kv/AdapterKVSegmentIterator.h"

#include "indexlib/index/common/field_format/pack_attribute/PackValueAdapter.h"
#include "indexlib/index/kv/config/ValueConfig.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AdapterKVSegmentIterator);

AdapterKVSegmentIterator::AdapterKVSegmentIterator(std::unique_ptr<IKVIterator> iterator,
                                                   const std::shared_ptr<PackValueAdapter>& adapter,
                                                   const std::shared_ptr<indexlibv2::config::ValueConfig>& currentValue,
                                                   const std::shared_ptr<indexlibv2::config::ValueConfig>& targetValue)
    : _iterator(std::move(iterator))
    , _adapter(adapter)
{
    assert(_iterator && adapter);
    _currentValueFixLength = (currentValue->GetFixedLength() > 0);
    _targetValueFixLength = (targetValue->GetFixedLength() > 0);
}

AdapterKVSegmentIterator::~AdapterKVSegmentIterator() {}

bool AdapterKVSegmentIterator::HasNext() const { return _iterator->HasNext(); }

Status AdapterKVSegmentIterator::Next(autil::mem_pool::Pool* pool, Record& record)
{
    auto s = _iterator->Next(pool, record);
    if (!s.IsOK() || record.deleted) {
        return s;
    }
    assert(record.value.size() > 0);

    size_t countLen = 0;
    if (!_currentValueFixLength) {
        autil::MultiValueFormatter::decodeCount(record.value.data(), countLen);
    }
    autil::StringView packValue = autil::StringView(record.value.data() + countLen, record.value.size() - countLen);
    auto convertValue = _adapter->ConvertIndexPackValue(packValue, pool);
    if (_targetValueFixLength) {
        record.value = convertValue;
        return s;
    }

    char encodeLenBuf[4];
    size_t dataSize = autil::MultiValueFormatter::encodeCount(convertValue.size(), encodeLenBuf, 4);
    char* buffer = (char*)pool->allocate(dataSize + convertValue.size());
    memcpy(buffer, encodeLenBuf, dataSize);
    memcpy(buffer + dataSize, convertValue.data(), convertValue.size());
    record.value = autil::StringView(buffer, dataSize + convertValue.size());
    return s;
}

Status AdapterKVSegmentIterator::Seek(offset_t offset) { return _iterator->Seek(offset); }

void AdapterKVSegmentIterator::Reset() { _iterator->Reset(); }

offset_t AdapterKVSegmentIterator::GetOffset() const { return _iterator->GetOffset(); }

const IKVIterator* AdapterKVSegmentIterator::GetKeyIterator() const { return _iterator->GetKeyIterator(); }

} // namespace indexlibv2::index
