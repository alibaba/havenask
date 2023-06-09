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
#include "indexlib/index/kv/KeyWriter.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/common/hash_table/HashTableBase.h"
#include "indexlib/index/common/hash_table/HashTableOptions.h"
#include "indexlib/index/kv/FixedLenHashTableCreator.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/MemoryUsage.h"
#include "indexlib/index/kv/SegmentStatistics.h"
#include "indexlib/index/kv/VarLenHashTableCollector.h"
#include "indexlib/index/kv/VarLenHashTableCreator.h"

namespace indexlibv2::index {

KeyWriter::KeyWriter() {}

KeyWriter::~KeyWriter() {}

Status KeyWriter::Init(const KVTypeId& typeId)
{
    std::unique_ptr<HashTableInfo> hashTableInfo;
    if (typeId.isVarLen) {
        hashTableInfo = VarLenHashTableCreator::CreateHashTableForWriter(typeId);
    } else {
        hashTableInfo = FixedLenHashTableCreator::CreateHashTableForWriter(typeId);
    }
    if (!hashTableInfo) {
        return Status::InternalError("create hash table failed for type: %s", typeId.ToString().c_str());
    }
    _typeId = typeId;
    _hashTable = hashTableInfo->StealHashTable<HashTableBase>();
    _valuePacker = std::move(hashTableInfo->valueUnpacker);
    _bucketCompressor = std::move(hashTableInfo->bucketCompressor);
    return Status::OK();
}

Status KeyWriter::AllocateMemory(autil::mem_pool::PoolBase* pool, size_t maxMemoryUse, int32_t occupancyPct)
{
    occupancyPct = _hashTable->GetRecommendedOccupancy(occupancyPct);
    size_t size = _hashTable->BuildMemoryToTableMemory(maxMemoryUse, occupancyPct);
    char* buffer = (char*)pool->allocate(size);
    if (buffer == nullptr) {
        return Status::NoMem("allocate ", size, " bytes failed");
    }
    HashTableOptions opts(occupancyPct);
    FillHashTableOptions(opts);
    if (!_hashTable->MountForWrite(buffer, size, occupancyPct)) {
        return Status::InternalError("mount hash table failed");
    }
    return Status::OK();
}

bool KeyWriter::KeyExist(uint64_t key) const
{
    autil::StringView data;
    return _hashTable->Find(key, data) != indexlib::util::NOT_FOUND;
}

Status KeyWriter::Add(uint64_t key, const autil::StringView& value, uint32_t timestamp)
{
    auto succ = _hashTable->Insert(key, _valuePacker->Pack(value, timestamp));
    if (likely(succ)) {
        return Status::OK();
    }
    return Status::NeedDump();
}

bool KeyWriter::Find(uint64_t key, autil::StringView& value, uint32_t& timestamp)
{
    autil::StringView packedVal;
    if (_hashTable->Find(key, packedVal) == indexlib::util::Status::OK) {
        _valuePacker->Unpack(packedVal, timestamp, value);
        return true;
    }
    return false;
}

Status KeyWriter::Delete(uint64_t key, uint32_t timestamp)
{
    bool succ = _hashTable->Delete(key, _valuePacker->Pack(autil::StringView::empty_instance(), timestamp));
    if (likely(succ)) {
        return Status::OK();
    }
    return Status::NeedDump();
}

Status KeyWriter::Dump(const std::shared_ptr<indexlib::file_system::Directory>& directory)
{
    return Dump(directory, false, false);
}

Status KeyWriter::Dump(const std::shared_ptr<indexlib::file_system::Directory>& directory, bool shrink, bool compress)
{
    auto impl = directory->GetIDirectory();
    auto [s, writer] = impl->CreateFileWriter(KV_KEY_FILE_NAME, indexlib::file_system::WriterOption()).StatusWith();
    if (!s.IsOK()) {
        return s;
    }

    if (shrink) {
        _hashTable->Shrink();
    }
    size_t fileSize = _hashTable->MemoryUse();
    if (compress) {
        size_t compressedSize = _hashTable->Compress(_bucketCompressor.get());
        if (compressedSize > fileSize) {
            return Status::InternalError("compress key file size: ", compressedSize, ", original size: ", fileSize);
        }
        fileSize = compressedSize;
    }
    auto result = writer->Write(_hashTable->Address(), fileSize);
    if (!result.OK()) {
        return result.Status();
    }
    if (result.Value() != fileSize) {
        return Status::IOError("write kv key failed, expected=", fileSize, ", actual=", result.Value());
    }
    return writer->Close().Status();
}

void KeyWriter::FillStatistics(SegmentStatistics& stat) const
{
    size_t hashMemUse = _hashTable->CapacityToTableMemory(_hashTable->Size(), _hashTable->GetOccupancyPct());
    stat.keyMemoryUse = hashMemUse;
    stat.keyCount = _hashTable->Size();
    stat.deletedKeyCount = _hashTable->GetDeleteCount();
    stat.occupancyPct = _hashTable->GetOccupancyPct();
}

void KeyWriter::UpdateMemoryUsage(MemoryUsage& memoryUsage) const
{
    memoryUsage.buildMemory = _hashTable->MemoryUse() + _hashTable->BuildAssistantMemoryUse();
    memoryUsage.dumpMemory = 0;
    memoryUsage.dumpedFileSize = _hashTable->MemoryUse();
}

void KeyWriter::CollectRecord(const CollectFuncType& func)
{
    // just support collect records in var len kv index
    assert(_typeId.isVarLen);
    VarLenHashTableCollector::CollectRecords(_typeId, _hashTable, func);
}

void KeyWriter::FillHashTableOptions(HashTableOptions& opts) const {}

} // namespace indexlibv2::index
