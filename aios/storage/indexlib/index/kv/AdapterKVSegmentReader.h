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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/index/kv/IKVSegmentReader.h"

namespace indexlibv2::config {
class ValueConfig;
}

namespace indexlibv2::index {
class PackValueAdapter;

class AdapterKVSegmentReader : public IKVSegmentReader
{
public:
    AdapterKVSegmentReader(const std::shared_ptr<IKVSegmentReader>& reader,
                           const std::shared_ptr<PackValueAdapter>& adapter,
                           const std::shared_ptr<indexlibv2::config::ValueConfig>& currentValue,
                           const std::shared_ptr<indexlibv2::config::ValueConfig>& targetValue);

    ~AdapterKVSegmentReader();

public:
    FL_LAZY(indexlib::util::Status)
    Get(keytype_t key, autil::StringView& value, uint64_t& ts, autil::mem_pool::Pool* pool,
        KVMetricsCollector* collector, autil::TimeoutTerminator* timeoutTerminator) const override;
    std::unique_ptr<IKVIterator> CreateIterator() override;
    size_t EvaluateCurrentMemUsed() override { return 0; }

private:
    std::shared_ptr<IKVSegmentReader> _reader;
    std::shared_ptr<PackValueAdapter> _adapter;
    std::shared_ptr<indexlibv2::config::ValueConfig> _currentValue;
    std::shared_ptr<indexlibv2::config::ValueConfig> _targetValue;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
