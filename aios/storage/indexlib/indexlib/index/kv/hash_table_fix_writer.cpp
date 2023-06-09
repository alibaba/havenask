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
#include "indexlib/index/kv/hash_table_fix_writer.h"

#include "indexlib/index/kv/hash_table_fix_creator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"

namespace indexlib { namespace index {

IE_LOG_SETUP(index, HashTableFixWriter);

void HashTableFixWriter::Init(const config::KVIndexConfigPtr& kvIndexConfig,
                              const file_system::DirectoryPtr& segmentDir, bool isOnline, uint64_t maxMemoryUse,
                              const std::shared_ptr<framework::SegmentGroupMetrics>& groupMetrics)
{
    mKVConfig = kvIndexConfig;
    mSegmentDir = segmentDir;
    mIsOnline = isOnline;
    file_system::DirectoryPtr indexDir = segmentDir->MakeDirectory(INDEX_DIR_NAME);
    mKVDir = indexDir->MakeDirectory(kvIndexConfig->GetIndexName());
    int32_t occupancyPct = mKVConfig->GetIndexPreference().GetHashDictParam().GetOccupancyPct();

    auto hashTableInfo = HashTableFixCreator::CreateHashTableForWriter(mKVConfig, KVFormatOptionsPtr(), mIsOnline);
    mHashTable.reset(static_cast<common::HashTableBase*>(hashTableInfo.hashTable.release()));
    mHashTableFileIterator.reset(
        static_cast<common::HashTableFileIterator*>(hashTableInfo.hashTableFileIterator.release()));
    mValueUnpacker = std::move(hashTableInfo.valueUnpacker);

    if (mKVConfig->GetIndexPreference().GetHashDictParam().GetHashType() == "cuckoo") {
        // if the config is (cuckoo, occupancy)
        // for dense table: set to min(80, occupancy)
        // for cuckoo table: set to occupancy
        occupancyPct = mHashTable->GetRecommendedOccupancy(occupancyPct);
    }
    size_t hashTableSize = mHashTable->BuildMemoryToTableMemory(maxMemoryUse, occupancyPct);
    mInMemFile = mKVDir->CreateFileWriter(KV_KEY_FILE_NAME, file_system::WriterOption::Mem(hashTableSize));
    mInMemFile->Truncate(hashTableSize).GetOrThrow(KV_KEY_FILE_NAME);
    mHashTable->MountForWrite(mInMemFile->GetBaseAddress(), mInMemFile->GetLength(), occupancyPct);
}

void HashTableFixWriter::Dump(const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* pool)
{
    assert(mInMemFile);
    if (!mIsOnline) {
        bool ret = mHashTable->Shrink();
        assert(ret);
        (void)ret;
        mInMemFile->Truncate(mHashTable->MemoryUse()).GetOrThrow();
    }
    mInMemFile->Close().GetOrThrow();
    KVFormatOptions kvOptions;
    kvOptions.SetShortOffset(false);
    directory->Store(KV_FORMAT_OPTION_FILE_NAME, kvOptions.ToString(), file_system::WriterOption());

    if (mAttributeWriter) {
        file_system::ReaderOption readerOption(file_system::FSOT_MEM);
        auto fileReader = mKVDir->CreateFileReader(KV_KEY_FILE_NAME, readerOption);
        mHashTableFileIterator->Init(fileReader);
        while (mHashTableFileIterator->IsValid()) {
            // add pkey whether it's deleted
            auto pkeyValue = (*mHashKeyValueMap)[mHashTableFileIterator->GetKey()];
            mAttributeWriter->AddField(mCurrentDocId, pkeyValue);
            mCurrentDocId++;
            mHashTableFileIterator->MoveToNext();
        }
        file_system::DirectoryPtr attributeDir = mSegmentDir->MakeDirectory(ATTRIBUTE_DIR_NAME);
        mAttributeWriter->Dump(attributeDir, pool);
    }
}

void HashTableFixWriter::FillSegmentMetrics(const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                            const std::string& groupName)
{
    FillSegmentMetrics(metrics, groupName, *mHashTable);
}

void HashTableFixWriter::FillSegmentMetrics(const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                            const std::string& groupName, const common::HashTableBase& hashTable)
{
    assert(metrics);
    size_t optimizeHashMem = hashTable.CapacityToTableMemory(hashTable.Size(), hashTable.GetOccupancyPct());
    metrics->Set<size_t>(groupName, KV_SEGMENT_MEM_USE, optimizeHashMem);
    metrics->Set<size_t>(groupName, KV_HASH_MEM_USE, optimizeHashMem);
    metrics->Set<int32_t>(groupName, KV_HASH_OCCUPANCY_PCT, hashTable.GetOccupancyPct());
    metrics->Set<size_t>(groupName, KV_KEY_COUNT, hashTable.Size());
    metrics->Set<size_t>(groupName, KV_KEY_COUNT, hashTable.Size());
    metrics->Set<uint64_t>(groupName, KV_KEY_DELETE_COUNT, hashTable.GetDeleteCount());
    metrics->Set<std::string>(groupName, KV_WRITER_TYPEID, typeid(HashTableFixWriter).name());
}

bool HashTableFixWriter::Add(const keytype_t& key, const autil::StringView& value, uint32_t timestamp, bool isDeleted,
                             regionid_t regionId)
{
    return Add(key, value, timestamp, isDeleted, regionId, *mHashTable, *mValueUnpacker);
}

bool HashTableFixWriter::Add(const keytype_t& key, const autil::StringView& value, uint32_t timestamp, bool isDeleted,
                             regionid_t regionId, common::HashTableBase& hashTable,
                             common::ValueUnpacker& valueUnpacker)
{
    if (regionId != DEFAULT_REGIONID) {
        IE_LOG(WARN, "kv fix writer only support regionId = 0");
        ERROR_COLLECTOR_LOG(WARN, "kv fix writer only support regionId = 0");
    }

    if (!isDeleted) {
        if (!hashTable.Insert(key, valueUnpacker.Pack(value, timestamp))) {
            IE_LOG(WARN, "insert key[%lu] into hash table failed", key);
            ERROR_COLLECTOR_LOG(WARN, "insert key[%lu] into hash table failed", key);
            return false;
        }
        return true;
    }
    if (!hashTable.Delete(key, valueUnpacker.Pack(value, timestamp))) {
        IE_LOG(WARN, "delete key[%lu] failed", key);
        ERROR_COLLECTOR_LOG(WARN, "delete key[%lu] failed", key);
        return false;
    }
    return true;
}
}} // namespace indexlib::index
