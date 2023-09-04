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

#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/kv/ValueWriter.h"

namespace autil::mem_pool {
class PoolBase;
}

namespace indexlibv2::index {
template <typename T>
class ExpandableValueAccessor;
}

namespace indexlibv2::framework {
class IIndexMemoryReclaimer;
};

namespace indexlibv2::index {
template <typename T>
class ReclaimedValueCollector;

class InMemoryValueWriter final : public ValueWriter
{
public:
    explicit InMemoryValueWriter(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& indexConfig);
    ~InMemoryValueWriter();

public:
    Status Init(autil::mem_pool::PoolBase* pool, size_t maxMemoryUse, float lastValueCompressRatio,
                indexlibv2::framework::IIndexMemoryReclaimer* memReclaimer);
    Status Dump(const std::shared_ptr<indexlib::file_system::Directory>& directory) override;
    bool IsFull() const override;
    Status Write(const autil::StringView& data) override;
    const char* GetBaseAddress() const override;
    int64_t GetLength() const override;
    void FillStatistics(SegmentStatistics& stat) const override;
    void FillMemoryUsage(MemoryUsage& memUsage) const override;
    std::pair<Status, std::shared_ptr<indexlib::file_system::FileWriter>>
    CreateValueFileWriter(const std::shared_ptr<indexlib::file_system::Directory>& directory) const;

public:
    Status Write(const autil::StringView& value, uint64_t& valueOffset);
    Status ReclaimValue(uint64_t valueOffset);
    const std::shared_ptr<indexlibv2::index::ExpandableValueAccessor<uint64_t>>& GetValueAccessor() const;

private:
    Status Append(const autil::StringView& value, uint64_t& valueOffset);
    bool NeedCompress() const;

private:
    std::shared_ptr<indexlibv2::config::KVIndexConfig> _indexConfig;
    std::shared_ptr<indexlibv2::index::ExpandableValueAccessor<uint64_t>> _valueAccessor;
    std::shared_ptr<ReclaimedValueCollector<uint64_t>> _reclaimedValueCollector;
    float _lastValueCompressRatio = 1.0f;
};

} // namespace indexlibv2::index
