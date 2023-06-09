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

#include "indexlib/index/kv/KVMemIndexerBase.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/KeyWriter.h"

namespace autil::mem_pool {
class PoolBase;
}

namespace indexlibv2::index {

class FixedLenKVMemIndexer final : public KVMemIndexerBase
{
public:
    explicit FixedLenKVMemIndexer(int64_t maxMemoryUse);
    ~FixedLenKVMemIndexer();

public:
    std::shared_ptr<IKVSegmentReader> CreateInMemoryReader() const override;

private:
    Status DoInit() override;
    bool IsFull() override;
    Status Add(uint64_t key, const autil::StringView& value, uint32_t timestamp) override;
    Status Delete(uint64_t key, uint32_t timestamp) override;
    Status DoDump(autil::mem_pool::PoolBase* pool,
                  const std::shared_ptr<indexlib::file_system::Directory>& directory) override;
    void UpdateMemoryUsage(MemoryUsage& memoryUsage) const override;
    void DoFillStatistics(SegmentStatistics& stat) const override;

private:
    KVTypeId _typeId;
    KeyWriter _keyWriter;
    int64_t _maxMemoryUse;
    std::shared_ptr<autil::mem_pool::PoolBase> _pool;
};

} // namespace indexlibv2::index
