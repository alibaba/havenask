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
#include "indexlib/index/kv/VarLenKVMemIndexer.h"

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/mem_reclaimer/EpochBasedMemReclaimer.h"
#include "indexlib/index/kv/InMemoryValueWriter.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/SegmentStatistics.h"
#include "indexlib/index/kv/VarLenKVMemoryReader.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "indexlib/index/kv/config/ValueConfig.h"

using namespace std;

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, VarLenKVMemIndexer);

VarLenKVMemIndexer::VarLenKVMemIndexer(int64_t maxMemoryUse, float keyValueSizeRatio, float valueCompressRatio,
                                       indexlibv2::framework::IIndexMemoryReclaimer* memReclaimer)
    : _maxMemoryUse(maxMemoryUse)
    , _keyValueSizeRatio(keyValueSizeRatio)
    , _valueCompressRatio(valueCompressRatio)
    , _memReclaimer(memReclaimer)
{
}

VarLenKVMemIndexer::VarLenKVMemIndexer(int64_t maxMemoryUse, float keyValueSizeRatio, float valueCompressRatio,
                                       const config::SortDescriptions& sortDescriptions,
                                       indexlibv2::framework::IIndexMemoryReclaimer* memReclaimer)
    : _maxMemoryUse(maxMemoryUse)
    , _keyValueSizeRatio(keyValueSizeRatio)
    , _valueCompressRatio(valueCompressRatio)
    , _sortDescriptions(sortDescriptions)
    , _memReclaimer(memReclaimer)
{
}

VarLenKVMemIndexer::~VarLenKVMemIndexer() {}

std::shared_ptr<IKVSegmentReader> VarLenKVMemIndexer::CreateInMemoryReader() const
{
    auto reader = std::make_shared<VarLenKVMemoryReader>();
    reader->HoldDataPool(_pool);
    reader->SetKey(_keyWriter->GetHashTable(), _keyWriter->GetValueUnpacker(), _formatOpts.IsShortOffset());
    reader->SetValue(_valueWriter->GetValueAccessor(), _indexConfig->GetValueConfig()->GetFixedLength());
    reader->SetPlainFormatEncoder(_plainFormatEncoder);
    return reader;
}

Status VarLenKVMemIndexer::DoInit()
{
    _pool = std::make_shared<autil::mem_pool::UnsafePool>(1024 * 1024);

    // init sort collector
    if (!_sortDescriptions.empty()) {
        _sortDataCollector = std::make_shared<KVSortDataCollector>();
        if (!_sortDataCollector->Init(_indexConfig, _sortDescriptions)) {
            AUTIL_LOG(ERROR, "create sort data collector for index: %s failed, sort description %s",
                      _indexConfig->GetIndexName().c_str(),
                      autil::legacy::ToJsonString(_sortDescriptions, true).c_str());
            return Status::InvalidArgs("sort data collector init failed");
        }
    }

    // init key writer
    const auto& kvConfig = *_indexConfig;
    auto typeId = MakeKVTypeId(kvConfig, nullptr);
    _formatOpts.SetShortOffset(typeId.shortOffset);
    const auto& hashParams = kvConfig.GetIndexPreference().GetHashDictParam();
    auto [s, keyWriter] = CreateKeyWriter(typeId, hashParams.GetOccupancyPct());
    RETURN_STATUS_DIRECTLY_IF_ERROR(s);
    _keyWriter = std::move(keyWriter);

    // init value writer
    int64_t maxKeyMemoryUse = (int64_t)(_maxMemoryUse * _keyValueSizeRatio);
    int64_t maxValueMemoryUse = _maxMemoryUse - maxKeyMemoryUse;
    if (hashParams.HasEnableShortenOffset()) {
        maxValueMemoryUse =
            std::min(hashParams.GetMaxValueSizeForShortOffset(), static_cast<size_t>(maxValueMemoryUse));
    }
    auto valueWriter = std::make_unique<InMemoryValueWriter>(_indexConfig);
    RETURN_STATUS_DIRECTLY_IF_ERROR(valueWriter->Init(_pool.get(), maxValueMemoryUse, _memReclaimer));
    _valueWriter = std::move(valueWriter);

    AUTIL_LOG(INFO,
              "key buffer size: %lu, value buffer size: %ld, sort descriptions size: %ld, enable memory reclaim: %d",
              maxKeyMemoryUse, maxValueMemoryUse, _sortDescriptions.size(), (_memReclaimer != nullptr));
    return Status::OK();
}

bool VarLenKVMemIndexer::IsFull() { return _keyWriter->IsFull() || _valueWriter->IsFull(); }

Status VarLenKVMemIndexer::Add(uint64_t key, const autil::StringView& value, uint32_t timestamp)
{
    offset_t curValueOffset = 0ul;
    if (_memReclaimer != nullptr && TryFindCurrentValue(key, curValueOffset)) {
        RETURN_STATUS_DIRECTLY_IF_ERROR(DoAdd(key, value, timestamp));
        RETURN_STATUS_DIRECTLY_IF_ERROR(_valueWriter->ReclaimValue(curValueOffset));
        return Status::OK();
    }

    return DoAdd(key, value, timestamp);
}

Status VarLenKVMemIndexer::DoAdd(uint64_t key, const autil::StringView& value, uint32_t timestamp)
{
    offset_t valueOffset = 0ul;
    RETURN_STATUS_DIRECTLY_IF_ERROR(_valueWriter->Write(value, valueOffset));
    RETURN_STATUS_DIRECTLY_IF_ERROR(_keyWriter->AddSimple(key, valueOffset, timestamp));
    return Status::OK();
}

Status VarLenKVMemIndexer::Delete(uint64_t key, uint32_t timestamp)
{
    offset_t curValueOffset = 0ul;
    if (_memReclaimer != nullptr && TryFindCurrentValue(key, curValueOffset)) {
        RETURN_STATUS_DIRECTLY_IF_ERROR(_keyWriter->Delete(key, timestamp));
        RETURN_STATUS_DIRECTLY_IF_ERROR(_valueWriter->ReclaimValue(curValueOffset));
        return Status::OK();
    }

    RETURN_STATUS_DIRECTLY_IF_ERROR(_keyWriter->Delete(key, timestamp));
    return Status::OK();
}

Status VarLenKVMemIndexer::DoDump(autil::mem_pool::PoolBase* pool,
                                  const std::shared_ptr<indexlib::file_system::Directory>& directory)
{
    if (!_sortDataCollector) {
        RETURN_STATUS_DIRECTLY_IF_ERROR(NormalDump(directory));
    } else {
        RETURN_STATUS_DIRECTLY_IF_ERROR(SortDump(directory));
    }

    auto s = _formatOpts.Store(directory);
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "dump meta for %s failed, error: %s", _indexConfig->GetIndexName().c_str(),
                  s.ToString().c_str());
    }
    return s;
}

Status VarLenKVMemIndexer::NormalDump(const std::shared_ptr<indexlib::file_system::Directory>& directory)
{
    auto s = _keyWriter->Dump(directory);
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "dump key for index %s failed, error: %s", _indexConfig->GetIndexName().c_str(),
                  s.ToString().c_str());
        return s;
    }

    s = _valueWriter->Dump(directory);
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "dump value for %s failed, error: %s", _indexConfig->GetIndexName().c_str(),
                  s.ToString().c_str());
        return s;
    }

    return Status::OK();
}

Status VarLenKVMemIndexer::SortDump(const std::shared_ptr<indexlib::file_system::Directory>& directory)
{
    std::unique_ptr<KeyWriter> keyWriter;
    std::shared_ptr<indexlib::file_system::FileWriter> valueFileWriter;

    auto status = PrepareWriterForSortDump(keyWriter, valueFileWriter, directory);
    RETURN_IF_STATUS_ERROR(status, "prepare writer for sort dump failed");

    SortByValue();

    return DumpAfterSort(keyWriter, valueFileWriter, directory);
}

Status VarLenKVMemIndexer::PrepareWriterForSortDump(std::unique_ptr<KeyWriter>& keyWriter,
                                                    std::shared_ptr<indexlib::file_system::FileWriter>& valueFileWriter,
                                                    const std::shared_ptr<indexlib::file_system::Directory>& directory)
{
    auto typeId = MakeKVTypeId(*_indexConfig, nullptr);
    const auto& hashParams = _indexConfig->GetIndexPreference().GetHashDictParam();

    Status status;
    std::tie(status, keyWriter) = CreateKeyWriter(typeId, hashParams.GetOccupancyPct());
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create dump key writer for index %s failed, error: %s", _indexConfig->GetIndexName().c_str(),
                  status.ToString().c_str());
        return status;
    }

    std::tie(status, valueFileWriter) = _valueWriter->CreateValueFileWriter(directory);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create dump value writer for %s failed, error: %s", _indexConfig->GetIndexName().c_str(),
                  status.ToString().c_str());
        return status;
    }

    return status;
}

void VarLenKVMemIndexer::SortByValue()
{
    // 1. collect <key,offset>
    _sortDataCollector->Reserve(_keyWriter->GetHashTable()->Size());
    auto& recordVector = _sortDataCollector->GetRecords();
    auto func = [&recordVector](Record& record) { recordVector.push_back(record); };
    _keyWriter->CollectRecord(func);

    // 2. sort by offset
    auto cmpOffset = [this](const Record& lhs, const Record& rhs) {
        if (lhs.deleted && rhs.deleted) {
            return lhs.key < rhs.key;
        } else if (lhs.deleted) {
            return true;
        } else if (rhs.deleted) {
            return false;
        } else {
            offset_t loffset = DecodeOffset(lhs.value.data());
            offset_t roffset = DecodeOffset(rhs.value.data());
            return loffset == roffset ? lhs.key < rhs.key : loffset < roffset;
        }
    };
    std::sort(recordVector.begin(), recordVector.end(), std::move(cmpOffset));

    // 3. fill values
    auto valueAccessor = _valueWriter->GetValueAccessor();
    for (auto& record : recordVector) {
        if (record.deleted) {
            continue;
        }
        // TODO: maybe cache offset and decoded values
        offset_t offset = DecodeOffset(record.value.data());
        record.value = DecodeValue(valueAccessor->GetValue(offset));
    }

    // 4. sort
    _sortDataCollector->Sort();
}

offset_t VarLenKVMemIndexer::DecodeOffset(const char* data) const
{
    if (_formatOpts.IsShortOffset()) {
        return *reinterpret_cast<const short_offset_t*>(data);
    } else {
        return *reinterpret_cast<const offset_t*>(data);
    }
}

autil::StringView VarLenKVMemIndexer::DecodeValue(const char* data) const
{
    int32_t fixedValueLen = _indexConfig->GetValueConfig()->GetFixedLength();
    if (fixedValueLen > 0) {
        return autil::StringView(data, fixedValueLen);
    } else {
        size_t countLen = 0;
        auto count = autil::MultiValueFormatter::decodeCount(data, countLen);
        return autil::StringView(data, countLen + count);
    }
}

Status VarLenKVMemIndexer::DumpAfterSort(const std::unique_ptr<KeyWriter>& keyWriter,
                                         const std::shared_ptr<indexlib::file_system::FileWriter>& valueFileWriter,
                                         const std::shared_ptr<indexlib::file_system::Directory>& directory)
{
    Status status;
    const auto& records = _sortDataCollector->GetRecords();
    for (auto& record : records) {
        if (true == record.deleted) {
            status = keyWriter->Delete(record.key, record.timestamp);
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "delete key for index %s failed, error: %s", _indexConfig->GetIndexName().c_str(),
                          status.ToString().c_str());
                return status;
            }
        } else {
            auto valueOffset = valueFileWriter->GetLogicLength();
            auto result = valueFileWriter->Write(record.value.data(), record.value.size());
            if (!result.OK()) {
                AUTIL_LOG(ERROR, "add value for index %s failed, error: %s", _indexConfig->GetIndexName().c_str(),
                          result.Status().ToString().c_str());
                return result.Status();
            }
            status = keyWriter->AddSimple(record.key, valueOffset, record.timestamp);
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "add key for index %s failed, error: %s", _indexConfig->GetIndexName().c_str(),
                          status.ToString().c_str());
                return status;
            }
        }
    }

    status = keyWriter->Dump(directory);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "dump key for index %s failed, error: %s", _indexConfig->GetIndexName().c_str(),
                  status.ToString().c_str());
        return status;
    }
    status = valueFileWriter->Close().Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "value file for index %s close failed, error: %s", _indexConfig->GetIndexName().c_str(),
                  status.ToString().c_str());
        return status;
    }

    auto realLength = valueFileWriter->GetLength();
    auto logicLength = valueFileWriter->GetLogicLength();
    if (_indexConfig->GetIndexPreference().GetValueParam().EnableFileCompress()) {
        double ratio = 1.0 * realLength / logicLength;
        AUTIL_LOG(INFO, "value compression ratio for index %s is %f", _indexConfig->GetIndexName().c_str(), ratio);
    } else if (realLength != logicLength) {
        return Status::IOError("write kv value failed, expected=", logicLength, ", actual=", realLength);
    }

    return Status::OK();
}

void VarLenKVMemIndexer::UpdateMemoryUsage(MemoryUsage& memoryUsage) const
{
    _keyWriter->UpdateMemoryUsage(memoryUsage);

    MemoryUsage valueMemUsage;
    _valueWriter->UpdateMemoryUsage(valueMemUsage);
    if (NeedCompress()) {
        valueMemUsage.dumpedFileSize *= _valueCompressRatio;
        valueMemUsage.dumpMemory += valueMemUsage.dumpedFileSize;
    }
    valueMemUsage.buildMemory += _memBuffer.GetBufferSize();

    memoryUsage += valueMemUsage;

    if (_sortDataCollector) {
        MemoryUsage sortDataMemUsage;
        _sortDataCollector->UpdateMemoryUsage(sortDataMemUsage);
        memoryUsage += sortDataMemUsage;
    }
}

void VarLenKVMemIndexer::DoFillStatistics(SegmentStatistics& stat) const
{
    _keyWriter->FillStatistics(stat);
    _valueWriter->FillStatistics(stat);
    stat.totalMemoryUse = stat.keyMemoryUse + stat.valueMemoryUse;
    stat.keyValueSizeRatio = 1.0f * stat.keyMemoryUse / stat.totalMemoryUse;
}

bool VarLenKVMemIndexer::NeedCompress() const
{
    const auto& valueParam = _indexConfig->GetIndexPreference().GetValueParam();
    return valueParam.EnableFileCompress();
}

std::pair<Status, std::unique_ptr<KeyWriter>> VarLenKVMemIndexer::CreateKeyWriter(KVTypeId typeId,
                                                                                  int32_t occupancyPct) const
{
    auto keyWriter = std::make_unique<KeyWriter>();
    auto s = keyWriter->Init(typeId);
    if (!s.IsOK()) {
        return std::make_pair(s, nullptr);
    }

    int64_t maxKeyMemoryUse = (int64_t)(_maxMemoryUse * _keyValueSizeRatio);
    s = keyWriter->AllocateMemory(_pool.get(), maxKeyMemoryUse, occupancyPct);
    if (!s.IsOK()) {
        return std::make_pair(s, nullptr);
    }
    return std::make_pair(s, std::move(keyWriter));
}

bool VarLenKVMemIndexer::TryFindCurrentValue(uint64_t key, offset_t& curValueOffset) const
{
    autil::StringView valueOffsetStr;
    uint32_t timestamp = 0u;
    if (!_keyWriter->Find(key, valueOffsetStr, timestamp)) {
        return false;
    }

    if (_formatOpts.IsShortOffset()) {
        curValueOffset = *reinterpret_cast<const short_offset_t*>(valueOffsetStr.data());
    } else {
        curValueOffset = *reinterpret_cast<const offset_t*>(valueOffsetStr.data());
    }

    return true;
}

} // namespace indexlibv2::index
