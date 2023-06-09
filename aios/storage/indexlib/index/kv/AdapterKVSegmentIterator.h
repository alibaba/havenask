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
#include "indexlib/index/kv/IKVIterator.h"

namespace indexlibv2::config {
class ValueConfig;
}

namespace indexlibv2::index {
class PackValueAdapter;

class AdapterKVSegmentIterator : public IKVIterator
{
public:
    AdapterKVSegmentIterator(std::unique_ptr<IKVIterator> iterator, const std::shared_ptr<PackValueAdapter>& adapter,
                             const std::shared_ptr<indexlibv2::config::ValueConfig>& currentValue,
                             const std::shared_ptr<indexlibv2::config::ValueConfig>& targetValue);

    ~AdapterKVSegmentIterator();

public:
    bool HasNext() const override;
    Status Next(autil::mem_pool::Pool* pool, Record& record) override;
    Status Seek(offset_t offset) override;
    void Reset() override;
    offset_t GetOffset() const override;
    const IKVIterator* GetKeyIterator() const override;

private:
    std::unique_ptr<IKVIterator> _iterator;
    std::shared_ptr<PackValueAdapter> _adapter;
    bool _currentValueFixLength = false;
    bool _targetValueFixLength = false;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
