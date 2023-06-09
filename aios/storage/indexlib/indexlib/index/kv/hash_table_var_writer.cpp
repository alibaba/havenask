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
#include "indexlib/index/kv/hash_table_var_writer.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/SwapMmapFileWriter.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"

namespace indexlib { namespace index {

IE_LOG_SETUP(index, HashTableVarWriter);

double HashTableVarWriter::CalculateMemoryRatio(const std::shared_ptr<framework::SegmentGroupMetrics>& groupMetrics,
                                                uint64_t maxMemoryUse)
{
    double ratio = DEFAULT_KEY_VALUE_MEM_RATIO;
    if (groupMetrics) {
        double prevMemoryRatio = groupMetrics->Get<double>(KV_KEY_VALUE_MEM_RATIO);
        size_t prevHashMemUse = groupMetrics->Get<size_t>(KV_HASH_MEM_USE);
        size_t prevValueMemUse = groupMetrics->Get<size_t>(KV_VALUE_MEM_USE);
        size_t prevTotalMemUse = prevHashMemUse + prevValueMemUse;
        if (prevTotalMemUse > 0) {
            ratio = ((double)prevHashMemUse / prevTotalMemUse) * NEW_MEMORY_RATIO_WEIGHT +
                    prevMemoryRatio * (1 - NEW_MEMORY_RATIO_WEIGHT);
        }
        ratio = ratio < 0.0001 ? 0.0001 : ratio;
        ratio = ratio > 0.90 ? 0.90 : ratio;
        IE_LOG(INFO,
               "Allocate Memory by group Metrics, prevKeyMem[%luB], prevValueMem[%luB], "
               "prevRatio[%.6f], ratio[%.6f], totalMem[%luB]",
               prevHashMemUse, prevValueMemUse, prevMemoryRatio, ratio, maxMemoryUse);
    } else {
        IE_LOG(INFO, "Allocate Memory by default, ratio[%.6f], totalMem[%luB]", ratio, maxMemoryUse);
    }
    return ratio;
}

void HashTableVarWriter::Init(const config::KVIndexConfigPtr& kvIndexConfig,
                              const file_system::DirectoryPtr& segmentDir, bool isOnline, uint64_t maxMemoryUse,
                              const std::shared_ptr<framework::SegmentGroupMetrics>& groupMetrics)
{
    mKVConfig = kvIndexConfig;
    mIsOnline = isOnline;
    mSegmentDir = segmentDir;
    file_system::DirectoryPtr indexDir = segmentDir->MakeDirectory(INDEX_DIR_NAME);
    mKVDir = indexDir->MakeDirectory(kvIndexConfig->GetIndexName());
    mMemoryRatio = CalculateMemoryRatio(groupMetrics, maxMemoryUse);
    int32_t occupancyPct = mKVConfig->GetIndexPreference().GetHashDictParam().GetOccupancyPct();

    auto hashTableInfo = HashTableVarCreator::CreateHashTableForWriter(mKVConfig, KVFormatOptionsPtr(), mIsOnline);
    mHashTable.reset(static_cast<common::HashTableBase*>(hashTableInfo.hashTable.release()));
    mHashTableFileIterator.reset(
        static_cast<common::HashTableFileIterator*>(hashTableInfo.hashTableFileIterator.release()));
    mValueUnpacker = std::move(hashTableInfo.valueUnpacker);
    mOffsetFormatter.reset(HashTableVarCreator::CreateKVVarOffsetFormatter(mKVConfig, mIsOnline));
    if (mKVConfig->GetIndexPreference().GetHashDictParam().GetHashType() == "cuckoo") {
        // if the config is (cuckoo, occupancy)
        // for dense table: set to min(80, occupancy)
        // for cuckoo table: set to occupancy
        occupancyPct = mHashTable->GetRecommendedOccupancy(occupancyPct);
    }
    int64_t maxKeyMemoryUse = 0;
    int64_t maxValueMemoryUse = 0;
    if (mKVConfig->GetUseSwapMmapFile()) {
        // value use virtual memory, so key cost all the momery
        maxKeyMemoryUse = maxMemoryUse;
        maxValueMemoryUse = (int64_t)(maxMemoryUse / mMemoryRatio) - maxKeyMemoryUse;
        assert(maxValueMemoryUse > 0);
    } else {
        maxKeyMemoryUse = (int64_t)(maxMemoryUse * mMemoryRatio);
        maxValueMemoryUse = maxMemoryUse - maxKeyMemoryUse;
    }
    size_t hashTableSize = mHashTable->BuildMemoryToTableMemory(maxKeyMemoryUse, occupancyPct);
    mKeyFile = mKVDir->CreateFileWriter(KV_KEY_FILE_NAME, file_system::WriterOption::Mem(hashTableSize));
    mKeyFile->Truncate(hashTableSize).GetOrThrow(KV_KEY_FILE_NAME);
    mHashTable->MountForWrite(mKeyFile->GetBaseAddress(), mKeyFile->GetLength(), occupancyPct);
    mValueFile = CreateValueFile(maxValueMemoryUse);
    mValueWriter.Init((char*)mValueFile->GetBaseAddress(), (offset_t)maxValueMemoryUse);
    mValueMemoryUseThreshold = maxValueMemoryUse * MAX_VALUE_MEMORY_USE_RATIO;
    IE_LOG(INFO, "value file memory threshold [%lu]", mValueMemoryUseThreshold);

    if (mKVConfig->GetRegionCount() > 1) {
        mRegionInfo.reset(new MultiRegionInfo(mKVConfig->GetRegionCount()));
    }
}

file_system::FileWriterPtr HashTableVarWriter::CreateValueFile(int64_t& maxValueMemoryUse)
{
    assert(maxValueMemoryUse > 0);

    if (mKVConfig->GetIndexPreference().GetHashDictParam().HasEnableShortenOffset()) {
        maxValueMemoryUse =
            std::min(size_t(maxValueMemoryUse),
                     mKVConfig->GetIndexPreference().GetHashDictParam().GetMaxValueSizeForShortOffset());
        IE_LOG(INFO, "value file use shorten offset");
    }

    std::string valueFileName = GetTmpValueFileName();
    if (mKVConfig->GetUseSwapMmapFile()) {
        if (mKVConfig->GetMaxSwapMmapFileSize() > 0) {
            maxValueMemoryUse = std::min(mKVConfig->GetMaxSwapMmapFileSize() * 1024 * 1024, maxValueMemoryUse);
        }

        file_system::FileWriterPtr fileWriter =
            mKVDir->CreateFileWriter(valueFileName, file_system::WriterOption::SwapMmap((int64_t)maxValueMemoryUse));
        file_system::SwapMmapFileWriterPtr swapFileWriter =
            DYNAMIC_POINTER_CAST(file_system::SwapMmapFileWriter, fileWriter);
        swapFileWriter->Truncate(maxValueMemoryUse).GetOrThrow();
        return swapFileWriter;
    } else {
        file_system::FileWriterPtr inMemFile =
            mKVDir->CreateFileWriter(valueFileName, file_system::WriterOption::MemNoDump(maxValueMemoryUse));
        // in-memory-file length always = 0, not calculate by file system
        inMemFile->Truncate(0).GetOrThrow(valueFileName);
        return inMemFile;
    }
}

void HashTableVarWriter::Dump(const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* pool)
{
    assert(mKeyFile && mValueFile);

    IE_LOG(INFO,
           "Dump by KeyFull[%d] or ValueFull[%d], "
           "KeyCount[%lu], KeyCapacity[%lu], ValueMemUse[%luB], ValueThreshold[%luB]",
           IsKeyFull(), IsValueFull(), mHashTable->Size(), mHashTable->Capacity(), mValueWriter.GetUsedBytes(),
           mValueMemoryUseThreshold);

    // truncate key file length if offline
    if (!mIsOnline) {
        bool ret = mHashTable->Shrink();
        assert(ret);
        (void)ret;
        mKeyFile->Truncate(mHashTable->MemoryUse()).GetOrThrow();
    }
    mKeyFile->Close().GetOrThrow();

    file_system::FileWriterPtr fileWriter;
    file_system::WriterOption writerOption =
        mKVConfig->GetUseSwapMmapFile() ? file_system::WriterOption::Buffer() : file_system::WriterOption();
    if (NeedCompress()) {
        assert(util::PathUtil::GetFileName(mValueFile->GetLogicalPath()) == GetTmpValueFileName());
        IE_LOG(INFO, "Begin compress value file [%s]", mValueFile->DebugString().c_str());
        const config::KVIndexPreference::ValueParam& valueParam = mKVConfig->GetIndexPreference().GetValueParam();
        writerOption.compressorName = valueParam.GetFileCompressType();
        writerOption.compressBufferSize = valueParam.GetFileCompressBufferSize();
        writerOption.compressorParams = valueParam.GetFileCompressParameter();
    } else {
        IE_LOG(INFO, "Begin sync value file [%s]", mValueFile->DebugString().c_str());
    }
    fileWriter = directory->CreateFileWriter(KV_VALUE_FILE_NAME, writerOption);
    fileWriter->ReserveFile(mValueWriter.GetUsedBytes()).GetOrThrow();
    fileWriter->Write(mValueFile->GetBaseAddress(), mValueWriter.GetUsedBytes()).GetOrThrow();
    fileWriter->Close().GetOrThrow();

    bool isShortOffset = (mValueUnpacker->GetValueSize() == sizeof(uint32_t));
    KVFormatOptions kvOptions;
    kvOptions.SetShortOffset(isShortOffset);
    directory->Store(KV_FORMAT_OPTION_FILE_NAME, kvOptions.ToString(), file_system::WriterOption());

    if (mRegionInfo) {
        mRegionInfo->Store(directory);
    }

    IE_LOG(INFO, "End write value file [%s]", mValueFile->DebugString().c_str());
    if (!mIsOnline) {
        RemoveValueFile();
    }

    if (mAttributeWriter) {
        file_system::ReaderOption readerOption(file_system::FSOT_MEM);
        auto fileReader = mKVDir->CreateFileReader(KV_KEY_FILE_NAME, readerOption);
        mHashTableFileIterator->Init(fileReader);
        mHashTableFileIterator->SortByValue();
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

void HashTableVarWriter::FillSegmentMetrics(const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                            const std::string& groupName)
{
    return FillSegmentMetrics(metrics, groupName, *mHashTable, mValueWriter.GetUsedBytes(), mMemoryRatio);
}

void HashTableVarWriter::FillSegmentMetrics(const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                            const std::string& groupName, const common::HashTableBase& hashTable,
                                            size_t valueMemUse, double memoryRatio)
{
    assert(metrics);
    size_t hashMemUse = hashTable.CapacityToTableMemory(hashTable.Size(), hashTable.GetOccupancyPct());
    size_t segmentMemUse = hashMemUse + valueMemUse;
    metrics->Set<size_t>(groupName, KV_SEGMENT_MEM_USE, segmentMemUse);
    metrics->Set<size_t>(groupName, KV_HASH_MEM_USE, hashMemUse);
    metrics->Set<int32_t>(groupName, KV_HASH_OCCUPANCY_PCT, hashTable.GetOccupancyPct());
    metrics->Set<size_t>(groupName, KV_VALUE_MEM_USE, valueMemUse);
    metrics->Set<size_t>(groupName, KV_KEY_COUNT, hashTable.Size());
    metrics->Set<double>(groupName, KV_KEY_VALUE_MEM_RATIO, memoryRatio);
    metrics->Set<std::string>(groupName, KV_WRITER_TYPEID, typeid(HashTableVarWriter).name());
}

bool HashTableVarWriter::Add(const keytype_t& key, const autil::StringView& value, uint32_t timestamp, bool isDeleted,
                             regionid_t regionId)
{
    if (!Add(key, value, timestamp, isDeleted, regionId, *mHashTable, *mValueUnpacker, *mOffsetFormatter,
             mValueWriter)) {
        return false;
    }
    IncRegionInfoCount(regionId, 1);
    return true;
}

bool HashTableVarWriter::Add(const keytype_t& key, const autil::StringView& value, uint32_t timestamp, bool isDeleted,
                             regionid_t regionId, common::HashTableBase& hashTable,
                             common::ValueUnpacker& valueUnpacker, KVVarOffsetFormatterBase& offsetFormatter,
                             ValueWriter& valueWriter)
{
    if (unlikely(value.size() + valueWriter.GetUsedBytes() > valueWriter.GetReserveBytes())) {
        IE_LOG(WARN,
               "insert key[%lu] failed, value memory not enough, "
               "reserve[%lu], used[%lu], need[%lu]",
               key, valueWriter.GetReserveBytes(), valueWriter.GetUsedBytes(), value.size());
        ERROR_COLLECTOR_LOG(WARN,
                            "insert key[%lu] failed, value memory not enough, "
                            "reserve[%lu], used[%lu], need[%lu]",
                            key, valueWriter.GetReserveBytes(), valueWriter.GetUsedBytes(), value.size());
        return false;
    }

    offset_t offset = 0;
    if (!isDeleted) {
        offset = valueWriter.Append(value);
        // offset = KVVarOffsetFormatter<T>(offset, regionId).GetValue();
        offset = offsetFormatter.GetValue(offset, &regionId);
        // if(!hashTable.Insert(key, T(timestamp, offset)))
        autil::StringView offsetStr((char*)&offset, sizeof(offset));
        if (!hashTable.Insert(key, valueUnpacker.Pack(offsetStr, timestamp))) {
            IE_LOG(WARN, "insert key[%lu] into hash table failed", key);
            ERROR_COLLECTOR_LOG(WARN, "insert key[%lu] into hash table failed", key);
            return false;
        }
        return true;
    }
    // offset = KVVarOffsetFormatter<T>(0, regionId).GetValue();
    offset = offsetFormatter.GetValue(offset, &regionId);
    // if(!hashTable.Delete(key, T(timestamp, offset)))
    autil::StringView offsetStr((char*)&offset, sizeof(offset));
    if (!hashTable.Delete(key, valueUnpacker.Pack(offsetStr, timestamp))) {
        IE_LOG(WARN, "delete key[%lu] failed", key);
        ERROR_COLLECTOR_LOG(WARN, "delete key[%lu] failed", key);
        return false;
    }
    return true;
}

void HashTableVarWriter::RemoveValueFile()
{
    // remove value.tmp, avoid occupy memory when no one reader it
    if (!mValueFile) {
        return;
    }

    assert(mKVDir);
    mValueFile->Close().GetOrThrow();
    mValueFile.reset();
    try {
        mKVDir->RemoveFile(GetTmpValueFileName());
    } catch (const util::ExceptionBase& e) {
        IE_LOG(ERROR, "ERROR occurred when remove kv value file [%s] in dir [%s] : [%s]", GetTmpValueFileName().c_str(),
               mKVDir->DebugString().c_str(), e.what());
        ERROR_COLLECTOR_LOG(ERROR, "ERROR occurred when remove kv value file [%s] in dir [%s] : [%s]",
                            GetTmpValueFileName().c_str(), mKVDir->DebugString().c_str(), e.what());
    }
}

void HashTableVarWriter::IncRegionInfoCount(regionid_t regionId, uint32_t count)
{
    if (!mRegionInfo) {
        return;
    }
    mRegionInfo->Get(regionId)->AddItemCount(count);
}
}} // namespace indexlib::index
