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
#ifndef __INDEXLIB_BLOCK_PRIMARY_KEY_FORMATTER_H
#define __INDEXLIB_BLOCK_PRIMARY_KEY_FORMATTER_H

#include "autil/LongHashValue.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/common/block_array/BlockArrayReader.h"
#include "indexlib/index/common/block_array/BlockArrayWriter.h"
#include "indexlib/index/common/block_array/KeyValueItem.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/primary_key_pair.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/HashMap.h"
#include "indexlib/util/SimplePool.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib { namespace index {

template <typename Key>
class BlockPrimaryKeyFormatter : public PrimaryKeyFormatter<Key>
{
public:
    typedef index::PKPair<Key> PKPair;
    typedef util::HashMap<Key, docid_t> HashMapType;
    typedef std::shared_ptr<HashMapType> HashMapTypePtr;
    typedef std::shared_ptr<PrimaryKeyIterator<Key>> PrimaryKeyIteratorPtr;
    typedef std::shared_ptr<OrderedPrimaryKeyIterator<Key>> OrderedPrimaryKeyIteratorPtr;

    using BlockArrayWriterPtr = std::shared_ptr<indexlibv2::index::BlockArrayWriter<Key, docid_t>>;
    using KVItem = KeyValueItem<Key, docid_t>;
    BlockPrimaryKeyFormatter(int32_t pkDataBlockSize) : mPkDataBlockSize(pkDataBlockSize) {}
    ~BlockPrimaryKeyFormatter() {}

    BlockPrimaryKeyFormatter(const BlockPrimaryKeyFormatter&) = delete;
    BlockPrimaryKeyFormatter& operator=(const BlockPrimaryKeyFormatter&) = delete;
    BlockPrimaryKeyFormatter(BlockPrimaryKeyFormatter&&) = delete;
    BlockPrimaryKeyFormatter& operator=(BlockPrimaryKeyFormatter&&) = delete;

public:
    docid_t Find(Key key) __ALWAYS_INLINE;
    future_lite::coro::Lazy<index::Result<docid_t>> FindAsync(Key key, future_lite::Executor* executor) noexcept;

    // Load data from file, should called once before you call method @Find
    bool Load(const file_system::FileReaderPtr& fileReader, const file_system::DirectoryPtr& directory);

    void Format(const OrderedPrimaryKeyIteratorPtr& pkIter,
                const file_system::FileWriterPtr& fileWriter) const override;
    void Format(const OrderedPrimaryKeyIteratorPtr& pkIter,
                SegmentOutputMapper<PKMergeOutputData>& outputMapper) const override;
    void SerializeToFile(const HashMapTypePtr& hashMap, size_t docCount, autil::mem_pool::PoolBase* pool,
                         const file_system::FileWriterPtr& file) const override;
    size_t CalculatePkSliceFileLen(size_t pkCount, size_t docCount) const override;
    void DeserializeToSliceFile(const PrimaryKeyIteratorPtr& pkIterator,
                                const file_system::FileWriterPtr& sliceFileWriter, size_t sliceFileLen) override;
    size_t EstimatePkCount(size_t fileLength, uint32_t docCount) const override;
    size_t EstimateDirectLoadSize(const PrimaryKeyLoadPlanPtr& plan) const override;

private:
    void SerializeToBuffer(KVItem* buffer, const HashMapTypePtr& hashMap) const;
    bool InnerLoad(const file_system::FileReaderPtr& fileReader, const file_system::DirectoryPtr& directory,
                   bool loadMetaIndex);

private:
    int32_t mPkDataBlockSize;
    indexlibv2::index::BlockArrayReader<Key, docid_t> mReader;

    IE_LOG_DECLARE();
};
IE_LOG_SETUP_TEMPLATE(index, BlockPrimaryKeyFormatter);

template <typename Key>
void BlockPrimaryKeyFormatter<Key>::Format(const OrderedPrimaryKeyIteratorPtr& pkIter,
                                           const file_system::FileWriterPtr& fileWriter) const
{
    // no condition will run this function
    assert(false);
    assert(pkIter);
    assert(fileWriter);
    PKPair pkPair;
    util::SimplePool pool;
    indexlibv2::index::BlockArrayWriter<Key, docid_t> writer(&pool);
    writer.Init(mPkDataBlockSize, fileWriter);
    while (pkIter->HasNext()) {
        pkIter->Next(pkPair);
        auto status = writer.AddItem(pkPair.key, pkPair.docid);
        THROW_IF_STATUS_ERROR(status);
    }
    auto status = writer.Finish();
    THROW_IF_STATUS_ERROR(status);
}

template <typename Key>
void BlockPrimaryKeyFormatter<Key>::Format(const OrderedPrimaryKeyIteratorPtr& pkIter,
                                           SegmentOutputMapper<PKMergeOutputData>& outputMapper) const
{
    assert(pkIter);
    PKPair pkPair;
    const auto& reclaimMap = outputMapper.GetReclaimMap();
    const std::vector<PKMergeOutputData>& outputs = outputMapper.GetOutputs();
    util::SimplePool pool;
    std::vector<BlockArrayWriterPtr> writers;
    writers.reserve(outputs.size());
    for (size_t i = 0; i < outputs.size(); i++) {
        auto& output = outputs[i];
        assert(output.outputIdx == i);
        BlockArrayWriterPtr writer(new indexlibv2::index::BlockArrayWriter<Key, docid_t>(&pool));
        writer->Init(mPkDataBlockSize, output.fileWriter);
        writers.push_back(writer);
    }

    while (pkIter->HasNext()) {
        pkIter->Next(pkPair);
        auto localInfo = reclaimMap->GetLocalId(pkPair.docid);
        auto output = outputMapper.GetOutputBySegIdx(localInfo.first);
        if (!output) {
            continue;
        }
        pkPair.docid = localInfo.second;
        auto status = writers[output->outputIdx]->AddItem(pkPair.key, pkPair.docid);
        THROW_IF_STATUS_ERROR(status);
    }

    for (auto& writer : writers) {
        auto status = writer->Finish();
        THROW_IF_STATUS_ERROR(status);
    }
}

template <typename Key>
inline void BlockPrimaryKeyFormatter<Key>::SerializeToBuffer(KVItem* buffer, const HashMapTypePtr& hashMap) const
{
    assert(buffer);
    assert(hashMap);
    size_t count = hashMap->Size();
    typename HashMapType::Iterator it = hashMap->CreateIterator();
    size_t pkBufferIdx = 0;
    while (it.HasNext()) {
        typename HashMapType::KeyValuePair p = it.Next();
        buffer[pkBufferIdx].key = p.first;
        buffer[pkBufferIdx].value = p.second;
        pkBufferIdx++;
    }
    std::sort(buffer, buffer + count);
}

template <typename Key>
void BlockPrimaryKeyFormatter<Key>::SerializeToFile(const HashMapTypePtr& hashMap, size_t docCount,
                                                    autil::mem_pool::PoolBase* pool,
                                                    const file_system::FileWriterPtr& file) const
{
    assert(hashMap);
    assert(file);
    size_t count = hashMap->Size();
    KVItem* buffer = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, KVItem, count);
    SerializeToBuffer(buffer, hashMap);

    indexlibv2::index::BlockArrayWriter<Key, docid_t> blockArrayWriter(pool);
    blockArrayWriter.Init(mPkDataBlockSize, file);
    for (size_t i = 0; i < count; i++) {
        auto status = blockArrayWriter.AddItem(buffer[i].key, buffer[i].value);
        THROW_IF_STATUS_ERROR(status);
    }
    auto status = blockArrayWriter.Finish();
    if (!status.IsOK()) {
        IE_LOG(ERROR, "block array writter finish failed, file[%s]", file->DebugString().c_str());
        THROW_IF_STATUS_ERROR(status);
    }
    if (buffer) {
        IE_POOL_COMPATIBLE_DELETE_VECTOR(pool, buffer, count);
    }
}

template <typename Key>
bool BlockPrimaryKeyFormatter<Key>::Load(const file_system::FileReaderPtr& fileReader,
                                         const file_system::DirectoryPtr& directory)
{
    return InnerLoad(fileReader, directory, true /*loadMetaIndex*/);
}

template <typename Key>
bool BlockPrimaryKeyFormatter<Key>::InnerLoad(const file_system::FileReaderPtr& fileReader,
                                              const file_system::DirectoryPtr& directory, bool loadMetaIndex)
{
    return mReader.Init(fileReader, directory, fileReader->GetLength(), loadMetaIndex).second;
}

template <typename Key>
inline docid_t BlockPrimaryKeyFormatter<Key>::Find(Key key)
{
    auto retWithEc = future_lite::coro::syncAwait(FindAsync(key, nullptr));
    return retWithEc.ValueOrThrow();
}

template <typename Key>
inline future_lite::coro::Lazy<index::Result<docid_t>>
BlockPrimaryKeyFormatter<Key>::FindAsync(Key key, future_lite::Executor* executor) noexcept
{
    file_system::ReadOption readOption;
    readOption.executor = executor;
    readOption.useInternalExecutor = false;
    docid_t docid = INVALID_DOCID;
    auto ret = co_await mReader.FindAsync(key, readOption, &docid);
    if (!ret.Ok()) {
        co_return ret.GetErrorCode();
    }
    if (ret.Value()) {
        co_return docid;
    } else {
        co_return INVALID_DOCID;
    }
}

template <typename Key>
size_t BlockPrimaryKeyFormatter<Key>::CalculatePkSliceFileLen(size_t pkCount, size_t docCount) const
{
    assert(false);
    IE_LOG(ERROR, "Unsupport CalculatePkSliceFileLen for block array");
    return 0;
}

template <typename Key>
void BlockPrimaryKeyFormatter<Key>::DeserializeToSliceFile(const PrimaryKeyIteratorPtr& pkIterator,
                                                           const file_system::FileWriterPtr& sliceFileWriter,
                                                           size_t sliceFileLen)
{
    assert(false);
    IE_LOG(ERROR, "Unsupport DeserializeToSliceFile for block array");
}

template <typename Key>
size_t BlockPrimaryKeyFormatter<Key>::EstimatePkCount(size_t fileLength, uint32_t docCount) const
{
    return fileLength / sizeof(PKPair);
}

template <typename Key>
size_t BlockPrimaryKeyFormatter<Key>::EstimateDirectLoadSize(const PrimaryKeyLoadPlanPtr& plan) const
{
    file_system::DirectoryPtr directory = plan->GetTargetFileDirectory();
    size_t memLockSize = directory->EstimateFileMemoryUse(plan->GetTargetFileName(), file_system::FSOT_LOAD_CONFIG);
    if (memLockSize > 0) {
        return memLockSize;
    }

    bool disableSliceMemLock = autil::EnvUtil::getEnv<bool>("INDEXLIB_DISABLE_SLICE_MEM_LOCK", false);
    if (disableSliceMemLock &&
        directory->EstimateFileMemoryUse(plan->GetTargetFileName(), file_system::FSOT_MEM_ACCESS) == 0) {
        // mem-nolock && disable slice memlock
        return 0;
    }
    file_system::FileReaderPtr fileReader =
        directory->CreateFileReader(plan->GetTargetFileName(), file_system::FSOT_BUFFERED);
    indexlibv2::index::BlockArrayReader<Key, docid_t> reader;
    auto [status, ret] = reader.Init(fileReader, directory, fileReader->GetLength(), false);
    THROW_IF_STATUS_ERROR(status);
    if (!ret) {
        return 0;
    }
    return reader.EstimateMetaSize();
}

}} // namespace indexlib::index

#endif //__INDEXLIB_BLOCK_PRIMARY_KEY_FORMATTER_H
