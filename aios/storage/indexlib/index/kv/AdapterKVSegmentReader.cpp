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
#include "indexlib/index/kv/AdapterKVSegmentReader.h"

#include "indexlib/index/common/field_format/pack_attribute/PackValueAdapter.h"
#include "indexlib/index/kv/AdapterKVSegmentIterator.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AdapterKVSegmentReader);

AdapterKVSegmentReader::AdapterKVSegmentReader(const std::shared_ptr<IKVSegmentReader>& reader,
                                               const std::shared_ptr<PackValueAdapter>& adapter,
                                               const std::shared_ptr<indexlibv2::config::ValueConfig>& currentValue,
                                               const std::shared_ptr<indexlibv2::config::ValueConfig>& targetValue)
    : _reader(reader)
    , _adapter(adapter)
    , _currentValue(currentValue)
    , _targetValue(targetValue)
{
    assert(reader && adapter);
}

AdapterKVSegmentReader::~AdapterKVSegmentReader() {}

FL_LAZY(indexlib::util::Status)
AdapterKVSegmentReader::Get(keytype_t key, autil::StringView& value, uint64_t& ts, autil::mem_pool::Pool* pool,
                            KVMetricsCollector* collector, autil::TimeoutTerminator* timeoutTerminator) const
{
    auto status = FL_COAWAIT _reader->Get(key, value, ts, pool, collector, timeoutTerminator);
    if (status == indexlib::util::OK) {
        value = _adapter->ConvertIndexPackValue(value, pool);
    }
    FL_CORETURN status;
}

std::unique_ptr<IKVIterator> AdapterKVSegmentReader::CreateIterator()
{
    auto iterator = _reader->CreateIterator();
    if (iterator == nullptr) {
        return nullptr;
    }
    return std::make_unique<AdapterKVSegmentIterator>(std::move(iterator), _adapter, _currentValue, _targetValue);
}

} // namespace indexlibv2::index
