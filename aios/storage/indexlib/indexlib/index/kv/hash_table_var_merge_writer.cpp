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
#include "indexlib/index/kv/hash_table_var_merge_writer.h"

#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/kv/hash_table_var_creator.h"
#include "indexlib/index/kv/hash_table_var_writer.h"
#include "indexlib/index/kv/kv_format_options.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer_factory.h"

namespace indexlib { namespace index {

IE_LOG_SETUP(index, HashTableVarMergeWriter);

uint64_t HashTableVarMergeWriter::EstimateMaxKeyCount(const framework::SegmentMetricsVec& segmentMetrics,
                                                      const index_base::SegmentTopologyInfo& targetTopoInfo)
{
    uint64_t maxKeyCount = 0;
    const std::string& groupName = index_base::SegmentMetricsUtil::GetColumnGroupName(targetTopoInfo.columnIdx);
    for (size_t i = 0; i < segmentMetrics.size(); ++i) {
        maxKeyCount += segmentMetrics[i]->Get<size_t>(groupName, KV_KEY_COUNT);
    }
    return maxKeyCount;
}

double HashTableVarMergeWriter::EstimateMemoryRatio(const framework::SegmentMetricsVec& segmentMetrics,
                                                    const index_base::SegmentTopologyInfo& targetTopoInfo)
{
    double totalMemoryRatio = 0.0;
    const std::string& groupName = index_base::SegmentMetricsUtil::GetColumnGroupName(targetTopoInfo.columnIdx);
    for (size_t i = 0; i < segmentMetrics.size(); ++i) {
        totalMemoryRatio += segmentMetrics[i]->Get<double>(groupName, KV_KEY_VALUE_MEM_RATIO);
    }
    return segmentMetrics.size() > 0 ? totalMemoryRatio / segmentMetrics.size() : 0.5;
}

void HashTableVarMergeWriter::Init(const file_system::DirectoryPtr& segmentDirectory,
                                   const file_system::DirectoryPtr& indexDirectory,
                                   const config::IndexPartitionSchemaPtr& schema,
                                   const config::KVIndexConfigPtr& kvIndexConfig, const KVFormatOptionsPtr& kvOptions,
                                   bool needStorePKeyValue, autil::mem_pool::PoolBase* pool,
                                   const framework::SegmentMetricsVec& segmentMetrics,
                                   const index_base::SegmentTopologyInfo& targetTopoInfo)
{
    mSchema = schema;
    mKVConfig = kvIndexConfig;
    mNeedStorePKeyValue = needStorePKeyValue;
    mAvgMemoryRatio = EstimateMemoryRatio(segmentMetrics, targetTopoInfo);

    uint64_t maxKeyCount = EstimateMaxKeyCount(segmentMetrics, targetTopoInfo);
    int32_t occupancyPct = mKVConfig->GetIndexPreference().GetHashDictParam().GetOccupancyPct();
    common::HashTableOptions options(occupancyPct);
    options.mayStretch = true;

    auto hashTableInfo = HashTableVarCreator::CreateHashTableForWriter(mKVConfig, kvOptions, false);
    mHashTable.reset(static_cast<common::HashTableBase*>(hashTableInfo.hashTable.release()));
    mValueUnpacker = std::move(hashTableInfo.valueUnpacker);
    mBucketCompressor = std::move(hashTableInfo.bucketCompressor);
    mOffsetFormatter.reset(HashTableVarCreator::CreateKVVarOffsetFormatter(mKVConfig, false));
    size_t hashTableSize = mHashTable->CapacityToTableMemory(maxKeyCount, options);
    void* baseAddr = pool->allocate(hashTableSize);
    mHashTable->MountForWrite(baseAddr, hashTableSize, options);
    assert(maxKeyCount <= mHashTable->Capacity());

    IE_LOG(INFO,
           "merger init with: maxKeyCount[%lu], occupancyPct[%d], "
           "hashTableSize[%lu], capacity[%lu], avgMemoryRatio[%f]",
           maxKeyCount, occupancyPct, hashTableSize, mHashTable->Capacity(), mAvgMemoryRatio);
    mSegmentDirectory = segmentDirectory;
    mIndexDirectory = indexDirectory;

    if (mIndexDirectory->IsExist(mKVConfig->GetIndexName())) {
        mIndexDirectory->RemoveDirectory(mKVConfig->GetIndexName());
    }
    mKVDir = mIndexDirectory->MakeDirectory(mKVConfig->GetIndexName());
    mKeyFileWriter = mKVDir->CreateFileWriter(KV_KEY_FILE_NAME);

    const config::KVIndexPreference::ValueParam& valueParam = kvIndexConfig->GetIndexPreference().GetValueParam();
    file_system::WriterOption writerOption;
    if (valueParam.EnableFileCompress()) {
        writerOption.compressorName = valueParam.GetFileCompressType();
        writerOption.compressBufferSize = valueParam.GetFileCompressBufferSize();
        writerOption.compressorParams = valueParam.GetFileCompressParameter();
    }
    mValueFileWriter = mKVDir->CreateFileWriter(KV_VALUE_FILE_NAME, writerOption);
    if (mKVConfig->GetRegionCount() > 1) {
        mRegionInfo.reset(new MultiRegionInfo(mKVConfig->GetRegionCount()));
    }

    if (mNeedStorePKeyValue) {
        config::IndexPartitionOptions partitionOptions;
        auto pkeyFieldName = mKVConfig->GetKeyFieldName();
        config::AttributeConfigPtr attrConfig = mSchema->GetRegionSchema(0)->CreateAttributeConfig(pkeyFieldName);
        mAttributeWriter.reset(
            index::AttributeWriterFactory::GetInstance()->CreateAttributeWriter(attrConfig, partitionOptions, nullptr));
        common::AttributeConvertorFactory factory;
        mPkeyConvertor.reset(factory.CreateAttrConvertor(attrConfig, tt_kv));
    }
}

void HashTableVarMergeWriter::OptimizeDump()
{
    int64_t begin = autil::TimeUtility::currentTime();
    bool ret = mHashTable->Shrink();
    assert(ret);
    (void)ret;
    IE_LOG(INFO, "shrink key file[%s], time interval[%ldus]", mKeyFileWriter->DebugString().c_str(),
           autil::TimeUtility::currentTime() - begin);

    begin = autil::TimeUtility::currentTime();
    mValueFileLength = GetValueLen();
    KVFormatOptionsPtr kvOptions = std::make_shared<KVFormatOptions>();
    kvOptions->SetShortOffset(false);
    bool enableShortOffset = mKVConfig->GetIndexPreference().GetHashDictParam().HasEnableShortenOffset();
    size_t maxValueSizeForShortOffset =
        mKVConfig->GetIndexPreference().GetHashDictParam().GetMaxValueSizeForShortOffset();
    size_t originalKeyFileSize = mHashTable->MemoryUse();
    if (enableShortOffset && (mValueFileLength <= maxValueSizeForShortOffset)) {
        size_t compressedSize = mHashTable->Compress(mBucketCompressor.get());
        if (compressedSize < originalKeyFileSize) {
            mKeyFileWriter->Write(mHashTable->Address(), compressedSize).GetOrThrow();
            kvOptions->SetShortOffset(true);
        } else {
            INDEXLIB_FATAL_ERROR(InconsistentState, "compressed size >= original size is unexpected");
        }
    } else {
        mKeyFileWriter->Write(mHashTable->Address(), originalKeyFileSize).GetOrThrow();
    }
    mKeyFileWriter->Close().GetOrThrow();
    IE_LOG(INFO, "flush key file[%s], shrinkSize[%lub], fileSize[%lub], time interval[%ldus]",
           mKeyFileWriter->DebugString().c_str(), mHashTable->MemoryUse(), mKeyFileWriter->GetLength(),
           autil::TimeUtility::currentTime() - begin);

    mValueFileWriter->Close().GetOrThrow();
    mKVDir->Store(KV_FORMAT_OPTION_FILE_NAME, kvOptions->ToString(), file_system::WriterOption());
    if (mRegionInfo) {
        mRegionInfo->Store(mKVDir);
    }

    if (!mHashKeyValueMap.empty()) {
        auto hashTableInfo = HashTableVarCreator::CreateHashTableForWriter(mKVConfig, kvOptions, false);
        mHashTableFileIterator.reset(
            static_cast<common::HashTableFileIterator*>(hashTableInfo.hashTableFileIterator.release()));

        file_system::ReaderOption readerOption(file_system::FSOT_MEM);
        auto fileReader = mKVDir->CreateFileReader(KV_KEY_FILE_NAME, readerOption);
        mHashTableFileIterator->Init(fileReader);
        mHashTableFileIterator->SortByValue();
        while (mHashTableFileIterator->IsValid()) {
            // add pkey whether it's deleted
            auto pkeyValue = mHashKeyValueMap[mHashTableFileIterator->GetKey()];
            mAttributeWriter->AddField(mCurrentDocId, pkeyValue);
            mCurrentDocId++;
            mHashTableFileIterator->MoveToNext();
        }
    }

    if (mNeedStorePKeyValue) {
        auto attributeDirectory = mSegmentDirectory->MakeDirectory(ATTRIBUTE_DIR_NAME);
        mAttributeWriter->Dump(attributeDirectory, &mPool);
    }
}

size_t HashTableVarMergeWriter::EstimateMemoryUse(const config::KVIndexConfigPtr& kvIndexConfig,
                                                  const KVFormatOptionsPtr& kvOptions,
                                                  const framework::SegmentMetricsVec& segmentMetrics,
                                                  const index_base::SegmentTopologyInfo& targetTopoInfo) const
{
    uint64_t maxKeyCount = EstimateMaxKeyCount(segmentMetrics, targetTopoInfo);
    int32_t occupancyPct = kvIndexConfig->GetIndexPreference().GetHashDictParam().GetOccupancyPct();
    common::HashTableOptions options(occupancyPct);
    options.mayStretch = true;

    auto hashTableInfo = HashTableVarCreator::CreateHashTableForWriter(kvIndexConfig, kvOptions, false);

    return static_cast<common::HashTableBase*>(hashTableInfo.hashTable.get())
        ->CapacityToBuildMemory(maxKeyCount, options);
}

void HashTableVarMergeWriter::FillSegmentMetrics(const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                                 const std::string& groupName)
{
    HashTableVarWriter::FillSegmentMetrics(metrics, groupName, *mHashTable, mValueFileLength, mAvgMemoryRatio);
}

bool HashTableVarMergeWriter::AddIfNotExist(const keytype_t& key, const autil::StringView& keyRawValue,
                                            const autil::StringView& value, uint32_t timestamp, bool isDeleted,
                                            regionid_t regionId)
{
    autil::StringView uselessStr;
    if (mHashTable->Find(key, uselessStr) != util::NOT_FOUND) {
        return true;
    }

    if (!Add(key, value, timestamp, isDeleted, regionId)) {
        IE_LOG(WARN, "add pkey hash [%lu] failed", key);
        return false;
    }

    if (mNeedStorePKeyValue) {
        bool hasFormatError = false;
        auto convertedValue = mPkeyConvertor->Encode(keyRawValue, &mPool, hasFormatError);
        if (hasFormatError) {
            IE_LOG(WARN, "pkey encode failed, pkey hash [%lu]", key);
            return false;
        }
        mHashKeyValueMap[key] = convertedValue;
    }

    IncRegionInfoCount(regionId, 1);

    return true;
}

bool HashTableVarMergeWriter::Add(const keytype_t& key, const autil::StringView& value, uint32_t timestamp,
                                  bool isDeleted, regionid_t regionId)
{
    offset_t offset = 0;
    if (!isDeleted) {
        offset = GetValueLen();
        if (value.size() != mValueFileWriter->Write(value.data(), value.size()).GetOrThrow()) {
            IE_LOG(ERROR, "write value for key[%lu] failed", key);
            return false;
        }
        offset = mOffsetFormatter->GetValue(offset, &regionId);
        autil::StringView offsetStr((char*)&offset, sizeof(offset));
        autil::StringView valueStr = mValueUnpacker->Pack(offsetStr, timestamp);
        if (unlikely(!mHashTable->Insert(key, valueStr))) {
            IE_LOG(ERROR, "insert key[%lu] failed, trigger rehash and retry", key);
            return mHashTable->Stretch() && mHashTable->Insert(key, valueStr);
        }
        return true;
    }
    offset = mOffsetFormatter->GetValue(0, &regionId);
    autil::StringView offsetStr((char*)&offset, sizeof(offset));
    autil::StringView valueStr = mValueUnpacker->Pack(offsetStr, timestamp);
    if (unlikely(!mHashTable->Delete(key, valueStr))) {
        IE_LOG(ERROR, "delete key[%lu] failed, trigger rehash and retry", key);
        return mHashTable->Stretch() && mHashTable->Delete(key, valueStr);
    }
    return true;
}

void HashTableVarMergeWriter::IncRegionInfoCount(regionid_t regionId, uint32_t count)
{
    if (!mRegionInfo) {
        return;
    }
    mRegionInfo->Get(regionId)->AddItemCount(count);
}

offset_t HashTableVarMergeWriter::GetValueLen() const
{
    assert(mValueFileWriter);
    return mValueFileWriter->GetLogicLength();
}
}} // namespace indexlib::index
