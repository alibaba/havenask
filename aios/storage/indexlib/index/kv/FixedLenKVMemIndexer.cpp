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
#include "indexlib/index/kv/FixedLenKVMemIndexer.h"

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/index/kv/FixedLenHashTableCreator.h"
#include "indexlib/index/kv/FixedLenKVMemoryReader.h"
#include "indexlib/index/kv/IKVSegmentReader.h"
#include "indexlib/index/kv/KVFormatOptions.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/SegmentStatistics.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, FixedLenKVMemIndexer);

FixedLenKVMemIndexer::FixedLenKVMemIndexer(bool tolerateDocError, int64_t maxMemoryUse)
    : KVMemIndexerBase(tolerateDocError)
    , _maxMemoryUse(maxMemoryUse)
{
}

FixedLenKVMemIndexer::~FixedLenKVMemIndexer() {}

std::shared_ptr<IKVSegmentReader> FixedLenKVMemIndexer::CreateInMemoryReader() const
{
    auto reader = std::make_shared<FixedLenKVMemoryReader>();
    reader->HoldDataPool(_pool);
    reader->SetKey(_keyWriter.GetHashTable(), _keyWriter.GetValueUnpacker(), _typeId);
    return reader;
}

Status FixedLenKVMemIndexer::DoInit()
{
    const auto& kvConfig = *_indexConfig;
    _typeId = MakeKVTypeId(kvConfig, nullptr);
    auto s = _keyWriter.Init(_typeId);
    if (!s.IsOK()) {
        return s;
    }

    _pool = std::make_unique<autil::mem_pool::UnsafePool>(1024 * 1024);
    const auto& hashParams = kvConfig.GetIndexPreference().GetHashDictParam();
    auto occupancyPct = hashParams.GetOccupancyPct();
    return _keyWriter.AllocateMemory(_pool.get(), _maxMemoryUse, occupancyPct);
}

bool FixedLenKVMemIndexer::IsFull() { return _keyWriter.IsFull(); }

Status FixedLenKVMemIndexer::Add(uint64_t key, const autil::StringView& value, uint32_t timestamp)
{
    return _keyWriter.Add(key, value, timestamp);
}

Status FixedLenKVMemIndexer::Delete(uint64_t key, uint32_t timestamp) { return _keyWriter.Delete(key, timestamp); }

Status FixedLenKVMemIndexer::DoDump(autil::mem_pool::PoolBase* pool,
                                    const std::shared_ptr<indexlib::file_system::Directory>& directory)
{
    auto s = _keyWriter.Dump(directory);
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "dump key for index %s failed, error: %s", _indexConfig->GetIndexName().c_str(),
                  s.ToString().c_str());
        return s;
    }

    KVFormatOptions kvOptions;
    kvOptions.SetShortOffset(false);
    s = kvOptions.Store(directory);
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "dump meta for %s failed, error: %s", _indexConfig->GetIndexName().c_str(),
                  s.ToString().c_str());
    }
    return s;
}

void FixedLenKVMemIndexer::FillMemoryUsage(MemoryUsage& memoryUsage) const { _keyWriter.FillMemoryUsage(memoryUsage); }

void FixedLenKVMemIndexer::DoFillStatistics(SegmentStatistics& stat) const
{
    _keyWriter.FillStatistics(stat);
    stat.totalMemoryUse = stat.keyMemoryUse;
    stat.keyValueSizeRatio = 1.0f;
}

} // namespace indexlibv2::index
