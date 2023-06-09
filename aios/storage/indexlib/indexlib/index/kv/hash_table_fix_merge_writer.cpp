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
#include "indexlib/index/kv/hash_table_fix_merge_writer.h"

#include "indexlib/common_define.h"
#include "indexlib/index/kv/hash_table_fix_creator.h"

namespace indexlib { namespace index {

IE_LOG_SETUP(index, HashTableFixMergeWriter);

uint64_t HashTableFixMergeWriter::EstimateMaxKeyCount(const framework::SegmentMetricsVec& segmentMetrics,
                                                      const index_base::SegmentTopologyInfo& targetTopoInfo)
{
    uint64_t maxKeyCount = 0;
    const std::string& groupName = index_base::SegmentMetricsUtil::GetColumnGroupName(targetTopoInfo.columnIdx);
    for (size_t i = 0; i < segmentMetrics.size(); ++i) {
        size_t keyCount = segmentMetrics[i]->Get<size_t>(groupName, KV_KEY_COUNT);
        maxKeyCount += keyCount;
    }
    return maxKeyCount;
}

void HashTableFixMergeWriter::Init(const file_system::DirectoryPtr& segmentDirectory,
                                   const file_system::DirectoryPtr& indexDirectory,
                                   const config::IndexPartitionSchemaPtr& mSchema,
                                   const config::KVIndexConfigPtr& kvIndexConfig, const KVFormatOptionsPtr& kvOptions,
                                   bool needStorePKeyValue, autil::mem_pool::PoolBase* pool,
                                   const framework::SegmentMetricsVec& segmentMetrics,
                                   const index_base::SegmentTopologyInfo& targetTopoInfo)
{
    mUseCompactBucket = kvOptions->UseCompactBucket();
    mKVConfig = kvIndexConfig;
    uint64_t maxKeyCount = EstimateMaxKeyCount(segmentMetrics, targetTopoInfo);
    int32_t occupancyPct = mKVConfig->GetIndexPreference().GetHashDictParam().GetOccupancyPct();
    Options options(occupancyPct);
    options.mayStretch = true;

    auto hashTableInfo = HashTableFixCreator::CreateHashTableForWriter(mKVConfig, kvOptions, false);
    mHashTable.reset(static_cast<common::HashTableBase*>(hashTableInfo.hashTable.release()));
    mValueUnpacker = std::move(hashTableInfo.valueUnpacker);

    size_t hashTableSize = mHashTable->CapacityToTableMemory(maxKeyCount, options);
    void* baseAddr = pool->allocate(hashTableSize);
    mHashTable->MountForWrite(baseAddr, hashTableSize, options);
    assert(maxKeyCount <= mHashTable->Capacity());

    IE_LOG(INFO,
           "merger init with: maxKeyCount[%lu], occupancyPct[%d], "
           "hashTableSize[%lu], capacity[%lu]",
           maxKeyCount, occupancyPct, hashTableSize, mHashTable->Capacity());

    if (indexDirectory->IsExist(mKVConfig->GetIndexName())) {
        indexDirectory->RemoveDirectory(mKVConfig->GetIndexName());
    }
    mKVDir = indexDirectory->MakeDirectory(mKVConfig->GetIndexName());
    mFileWriter = mKVDir->CreateFileWriter(KV_KEY_FILE_NAME);
}

void HashTableFixMergeWriter::FillSegmentMetrics(const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                                 const std::string& groupName)
{
    return HashTableFixWriter::FillSegmentMetrics(metrics, groupName, *mHashTable);
}

void HashTableFixMergeWriter::OptimizeDump()
{
    int64_t begin = autil::TimeUtility::currentTime();
    bool ret = mHashTable->Shrink();
    assert(ret);
    (void)ret;
    IE_LOG(INFO, "shrink key file[%s], time interval[%ldus]", mFileWriter->DebugString().c_str(),
           autil::TimeUtility::currentTime() - begin);

    begin = autil::TimeUtility::currentTime();
    mFileWriter->Write(mHashTable->Address(), mHashTable->MemoryUse()).GetOrThrow();
    mFileWriter->Close().GetOrThrow();

    KVFormatOptions kvOptions;
    kvOptions.SetShortOffset(false);
    kvOptions.SetUseCompactBucket(mUseCompactBucket);
    mKVDir->Store(KV_FORMAT_OPTION_FILE_NAME, kvOptions.ToString(), file_system::WriterOption());

    IE_LOG(INFO, "flush key file[%s], shrinkSize[%lub], fileSize[%lub], time interval[%ldus]",
           mFileWriter->DebugString().c_str(), mHashTable->MemoryUse(), mFileWriter->GetLength(),
           autil::TimeUtility::currentTime() - begin);
}

size_t HashTableFixMergeWriter::EstimateMemoryUse(const config::KVIndexConfigPtr& kvIndexConfig,
                                                  const KVFormatOptionsPtr& kvOptions,
                                                  const framework::SegmentMetricsVec& segmentMetrics,
                                                  const index_base::SegmentTopologyInfo& targetTopoInfo) const
{
    uint64_t maxKeyCount = EstimateMaxKeyCount(segmentMetrics, targetTopoInfo);
    int32_t occupancyPct = kvIndexConfig->GetIndexPreference().GetHashDictParam().GetOccupancyPct();
    Options options(occupancyPct);
    options.mayStretch = true;

    auto hashTableInfo = HashTableFixCreator::CreateHashTableForWriter(kvIndexConfig, kvOptions, false);
    return static_cast<common::HashTableBase*>(hashTableInfo.hashTable.get())
        ->CapacityToBuildMemory(maxKeyCount, options);
}

bool HashTableFixMergeWriter::AddIfNotExist(const keytype_t& key, const autil::StringView& keyRawValue,
                                            const autil::StringView& value, uint32_t timestamp, bool isDeleted,
                                            regionid_t regionId)
{
    autil::StringView uselessStr;
    if (mHashTable->Find(key, uselessStr) != util::NOT_FOUND) {
        return true;
    }

    if (likely(HashTableFixWriter::Add(key, value, timestamp, isDeleted, regionId, *mHashTable, *mValueUnpacker))) {
        return true;
    }

    IE_LOG(WARN, "Add key[%lu] failed, trigger stretch and retry", key);
    return mHashTable->Stretch() &&
           HashTableFixWriter::Add(key, value, timestamp, isDeleted, regionId, *mHashTable, *mValueUnpacker);
}
}} // namespace indexlib::index
