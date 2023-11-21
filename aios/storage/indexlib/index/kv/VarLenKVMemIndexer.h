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

#include "indexlib/config/SortDescription.h"
#include "indexlib/index/kv/KVFormatOptions.h"
#include "indexlib/index/kv/KVMemIndexerBase.h"
#include "indexlib/index/kv/KVSortDataCollector.h"
#include "indexlib/index/kv/KeyWriter.h"

namespace indexlib::file_system {
class FileWriter;
class Directory;
} // namespace indexlib::file_system

namespace autil::mem_pool {
class PoolBase;
}

namespace indexlibv2::framework {
class IIndexMemoryReclaimer;
};

namespace indexlibv2::index {
class InMemoryValueWriter;

class VarLenKVMemIndexer final : public KVMemIndexerBase
{
public:
    VarLenKVMemIndexer(bool tolerateDocError, int64_t maxMemoryUse,
                       float keyValueSizeRatio = DEFAULT_KEY_VALUE_MEM_RATIO,
                       float valueCompressRatio = DEFAULT_VALUE_COMPRESSION_RATIO,
                       indexlibv2::framework::IIndexMemoryReclaimer* memReclaimer = nullptr);
    VarLenKVMemIndexer(bool tolerateDocError, int64_t maxMemoryUse, float keyValueSizeRatio, float valueCompressRatio,
                       const config::SortDescriptions& sortDescriptions,
                       indexlibv2::framework::IIndexMemoryReclaimer* memReclaimer = nullptr);
    ~VarLenKVMemIndexer();

public:
    std::shared_ptr<IKVSegmentReader> CreateInMemoryReader() const override;
    const InMemoryValueWriter* TEST_GetValueWriter() const { return _valueWriter.get(); }

private:
    Status DoInit() override;
    bool IsFull() override;
    Status Add(uint64_t key, const autil::StringView& value, uint32_t timestamp) override;
    Status DoAdd(uint64_t key, const autil::StringView& value, uint32_t timestamp);
    Status Delete(uint64_t key, uint32_t timestamp) override;
    Status DoDump(autil::mem_pool::PoolBase* pool,
                  const std::shared_ptr<indexlib::file_system::Directory>& directory) override;
    Status NormalDump(const std::shared_ptr<indexlib::file_system::Directory>& directory);
    Status SortDump(const std::shared_ptr<indexlib::file_system::Directory>& directory);
    Status PrepareWriterForSortDump(std::unique_ptr<KeyWriter>& keyWriter,
                                    std::shared_ptr<indexlib::file_system::FileWriter>& valueFileWriter,
                                    const std::shared_ptr<indexlib::file_system::Directory>& directory);
    void SortByValue();
    Status DumpAfterSort(const std::unique_ptr<KeyWriter>& keyWriter,
                         const std::shared_ptr<indexlib::file_system::FileWriter>& valueFileWriter,
                         const std::shared_ptr<indexlib::file_system::Directory>& directory);

    void FillMemoryUsage(MemoryUsage& memoryUsage) const override;
    void DoFillStatistics(SegmentStatistics& stat) const override;
    std::pair<Status, std::unique_ptr<KeyWriter>> CreateKeyWriter(KVTypeId typeId, int32_t occupancyPct) const;
    bool TryFindCurrentValue(uint64_t key, offset_t& curValueOffset) const;

    offset_t DecodeOffset(const char* data) const;
    autil::StringView DecodeValue(const char* data) const;

private:
    int64_t _maxMemoryUse;
    float _keyValueSizeRatio;
    float _valueCompressRatio;
    config::SortDescriptions _sortDescriptions;

    std::shared_ptr<autil::mem_pool::PoolBase> _pool; // shared for in memory reader
    KVFormatOptions _formatOpts;
    std::unique_ptr<KeyWriter> _keyWriter;
    std::unique_ptr<InMemoryValueWriter> _valueWriter;
    std::shared_ptr<KVSortDataCollector> _sortDataCollector;
    indexlibv2::framework::IIndexMemoryReclaimer* _memReclaimer;

public:
    static constexpr float DEFAULT_KEY_VALUE_MEM_RATIO = 0.01f;
    static constexpr float MIN_KEY_VALUE_MEM_RATIO = 0.001f;
    static constexpr float MAX_KEY_VALUE_MEM_RATIO = 0.5f; // attention to no doc or full of delete doc
    static constexpr float DEFAULT_VALUE_COMPRESSION_RATIO = 1.0f;
};

} // namespace indexlibv2::index
