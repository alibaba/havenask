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

#include "indexlib/common_define.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/primary_key_hash_table.h"
#include "indexlib/index/normal/primarykey/primary_key_pair.h"
#include "indexlib/index/normal/primarykey/sorted_primary_key_formatter.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/PrimeNumberTable.h"

namespace indexlib { namespace index {

template <typename Key>
class HashPrimaryKeyFormatter : public PrimaryKeyFormatter<Key>
{
public:
    typedef index::PKPair<Key> PKPair;
    typedef util::HashMap<Key, docid_t> HashMapType;
    typedef std::shared_ptr<HashMapType> HashMapTypePtr;
    typedef std::shared_ptr<PrimaryKeyIterator<Key>> PrimaryKeyIteratorPtr;
    typedef std::shared_ptr<OrderedPrimaryKeyIterator<Key>> OrderedPrimaryKeyIteratorPtr;

public:
    HashPrimaryKeyFormatter() : mData(NULL) {}
    ~HashPrimaryKeyFormatter() {}

public:
    docid_t Find(char* data, Key key) __ALWAYS_INLINE;

    void Format(const OrderedPrimaryKeyIteratorPtr& pkIter,
                const file_system::FileWriterPtr& fileWriter) const override;
    void Format(const OrderedPrimaryKeyIteratorPtr& pkIter,
                SegmentOutputMapper<PKMergeOutputData>& outputMapper) const override;

    size_t CalculatePkSliceFileLen(size_t pkCount, size_t docCount) const override;
    void SerializeToFile(const HashMapTypePtr& hashMap, size_t docCount, autil::mem_pool::PoolBase* pool,
                         const file_system::FileWriterPtr& file) const override;
    void DeserializeToSliceFile(const PrimaryKeyIteratorPtr& pkIterator,
                                const file_system::FileWriterPtr& sliceFileWriter, size_t sliceFileLen) override;
    size_t EstimatePkCount(size_t fileLength, uint32_t docCount) const override;
    size_t EstimateDirectLoadSize(const PrimaryKeyLoadPlanPtr& plan) const override;

private:
    void SerializeToBuffer(char* buffer, size_t bufferSize, const HashMapTypePtr& hashMap) const;

    void DeserializeToBuffer(const PrimaryKeyIteratorPtr& pkIterator, char* buffer) const;
    void DeserializeToBuffer(const PrimaryKeyIteratorPtr& pkIterator,
                             SegmentOutputMapper<PKMergeOutputData>& outputMapper,
                             std::vector<PrimaryKeyHashTable<Key>>& pkHashTables) const;

private:
    // cache
    char* mData;
    PrimaryKeyHashTable<Key> mPkHashTable;

private:
    IE_LOG_DECLARE();
    friend class HashPrimaryKeyFormatterTest;
};

/////////////////////////////////////////////////////
template <typename Key>
void HashPrimaryKeyFormatter<Key>::Format(const OrderedPrimaryKeyIteratorPtr& pkIter,
                                          const file_system::FileWriterPtr& fileWriter) const
{
    assert(pkIter);
    assert(fileWriter);

    // TODO: use pool
    size_t bufLen = PrimaryKeyHashTable<Key>::CalculateMemorySize(pkIter->GetPkCount(), pkIter->GetDocCount());
    char* buffer = new char[bufLen];
    DeserializeToBuffer(pkIter, buffer);
    fileWriter->Write(buffer, bufLen).GetOrThrow();
    delete[] buffer;
}

template <typename Key>
void HashPrimaryKeyFormatter<Key>::Format(const OrderedPrimaryKeyIteratorPtr& pkIter,
                                          SegmentOutputMapper<PKMergeOutputData>& outputMapper) const
{
    assert(pkIter);

    const auto& reclaimMap = outputMapper.GetReclaimMap();

    const auto& outputs = outputMapper.GetOutputs();
    // TODO: use pool
    std::vector<std::vector<char>> buffers;
    buffers.reserve(outputs.size());
    std::vector<PrimaryKeyHashTable<Key>> pkHashTables(outputs.size());
    for (size_t i = 0; i < outputs.size(); ++i) {
        const auto& output = outputs[i];
        size_t docCount = reclaimMap->GetTargetSegmentDocCount(output.targetSegmentIndex);
        size_t bufLen = PrimaryKeyHashTable<Key>::CalculateMemorySize(docCount, docCount);
        buffers.push_back(std::vector<char>(bufLen));
        pkHashTables[i].Init(buffers.back().data(), docCount, docCount);
    }

    DeserializeToBuffer(pkIter, outputMapper, pkHashTables);
    for (size_t i = 0; i < outputs.size(); ++i) {
        const auto& output = outputs[i];
        output.fileWriter->Write(buffers[i].data(), buffers[i].size()).GetOrThrow();
    }
}

template <typename Key>
inline void HashPrimaryKeyFormatter<Key>::SerializeToBuffer(char* buffer, size_t docCount,
                                                            const HashMapTypePtr& hashMap) const
{
    assert(hashMap);
    assert(buffer);
    size_t pkCount = hashMap->Size();

    PrimaryKeyHashTable<Key> pkHashTable;
    pkHashTable.Init(buffer, pkCount, docCount);

    typename HashMapType::Iterator it = hashMap->CreateIterator();
    PKPair pkPair;
    while (it.HasNext()) {
        typename HashMapType::KeyValuePair p = it.Next();
        pkPair.key = p.first;
        pkPair.docid = p.second;
        pkHashTable.Insert(pkPair);
    }
}

template <typename Key>
void HashPrimaryKeyFormatter<Key>::DeserializeToBuffer(const PrimaryKeyIteratorPtr& pkIterator, char* buffer) const
{
    uint64_t pkCount = pkIterator->GetPkCount();
    uint64_t docCount = pkIterator->GetDocCount();
    PrimaryKeyHashTable<Key> pkHashTable;
    pkHashTable.Init(buffer, pkCount, docCount);
    PKPair pkPair;
    while (pkIterator->HasNext()) {
        pkIterator->Next(pkPair);
        pkHashTable.Insert(pkPair);
    }
}

template <typename Key>
void HashPrimaryKeyFormatter<Key>::DeserializeToBuffer(const PrimaryKeyIteratorPtr& pkIterator,
                                                       SegmentOutputMapper<PKMergeOutputData>& outputMapper,
                                                       std::vector<PrimaryKeyHashTable<Key>>& pkHashTables) const
{
    PKPair pkPair;
    const auto& reclaimMap = outputMapper.GetReclaimMap();
    while (pkIterator->HasNext()) {
        pkIterator->Next(pkPair);
        auto localInfo = reclaimMap->GetLocalId(pkPair.docid);
        auto output = outputMapper.GetOutputBySegIdx(localInfo.first);
        if (!output) {
            continue;
        }
        pkPair.docid = localInfo.second;
        pkHashTables[output->outputIdx].Insert(pkPair);
    }
}

template <typename Key>
void HashPrimaryKeyFormatter<Key>::SerializeToFile(const HashMapTypePtr& hashMap, size_t docCount,
                                                   autil::mem_pool::PoolBase* pool,
                                                   const file_system::FileWriterPtr& file) const
{
    assert(hashMap);
    assert(file);
    uint32_t itemCount = hashMap->Size();
    size_t bufLen = CalculatePkSliceFileLen(itemCount, docCount);
    char* buffer = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, char, bufLen);
    SerializeToBuffer(buffer, docCount, hashMap);
    file->ReserveFile(bufLen).GetOrThrow();
    file->Write((void*)buffer, bufLen).GetOrThrow();
    assert(file->GetLength() == bufLen);
    IE_POOL_COMPATIBLE_DELETE_VECTOR(pool, buffer, bufLen);
}

template <typename Key>
inline docid_t HashPrimaryKeyFormatter<Key>::Find(char* data, Key key)
{
    if (unlikely(mData != data)) {
        mPkHashTable.Init(data);
        MEMORY_BARRIER();
        mData = data;
    }
    return mPkHashTable.Find(key);
}

template <typename Key>
inline size_t HashPrimaryKeyFormatter<Key>::CalculatePkSliceFileLen(size_t pkCount, size_t docCount) const
{
    return PrimaryKeyHashTable<Key>::CalculateMemorySize(pkCount, docCount);
}

template <typename Key>
void HashPrimaryKeyFormatter<Key>::DeserializeToSliceFile(const PrimaryKeyIteratorPtr& pkIterator,
                                                          const file_system::FileWriterPtr& sliceFileWriter,
                                                          size_t sliceFileLen)
{
    IE_LOG(DEBUG, "begin load pk [%s] to hash...", sliceFileWriter->DebugString().c_str());
    // 1m : 900ms
    // 4k : 600ms ~ 700ms
    sliceFileWriter->Truncate(sliceFileLen).GetOrThrow();
    char* baseAddress = (char*)sliceFileWriter->GetBaseAddress();
    DeserializeToBuffer(pkIterator, baseAddress);
    IE_LOG(DEBUG, "end load pk [%s] to hash...", sliceFileWriter->DebugString().c_str());
}

template <typename Key>
size_t HashPrimaryKeyFormatter<Key>::EstimatePkCount(size_t fileLength, uint32_t docCount) const
{
    return PrimaryKeyHashTable<Key>::EstimatePkCount(fileLength, docCount);
}

template <typename Key>
size_t HashPrimaryKeyFormatter<Key>::EstimateDirectLoadSize(const PrimaryKeyLoadPlanPtr& plan) const
{
    file_system::DirectoryPtr pkDir = plan->GetTargetFileDirectory();
    assert(pkDir);
    return pkDir->EstimateFileMemoryUse(plan->GetTargetFileName(), file_system::FSOT_MEM_ACCESS);
}

IE_LOG_SETUP_TEMPLATE(index, HashPrimaryKeyFormatter);
}} // namespace indexlib::index
