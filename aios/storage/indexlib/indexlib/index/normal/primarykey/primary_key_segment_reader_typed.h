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
#ifndef __INDEXLIB_PRIMARY_KEY_SEGMENT_READER_TYPED_H
#define __INDEXLIB_PRIMARY_KEY_SEGMENT_READER_TYPED_H

#include <memory>

#include "autil/BloomFilter.h"
#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/NormalFileReader.h"
#include "indexlib/file_system/file/ResourceFile.h"
#include "indexlib/index/inverted_index/IndexSegmentReader.h"
#include "indexlib/index/normal/primarykey/block_primary_key_pair_segment_iterator.h"
#include "indexlib/index/normal/primarykey/primary_key_loader.h"
#include "indexlib/index/normal/primarykey/primary_key_segment_formatter.h"
#include "indexlib/index/normal/primarykey/sorted_primary_key_pair_segment_iterator.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace index {

template <typename Key>
class PrimaryKeySegmentReaderTyped : public index::IndexSegmentReader
{
public:
    typedef typename PrimaryKeyFormatter<Key>::PKPair PKPair;
    typedef typename std::shared_ptr<PrimaryKeyPairSegmentIterator<Key>> PkPairIteratorPtr;

public:
    PrimaryKeySegmentReaderTyped() : mData(NULL), mItemCount(0), mBloomFilter(NULL) {}

    ~PrimaryKeySegmentReaderTyped() {}

public:
    void Init(const config::PrimaryKeyIndexConfigPtr& indexConfig, const PrimaryKeyLoadPlanPtr& plan);

    future_lite::coro::Lazy<docid_t> LookupAsync(const Key& hashKey,
                                                 future_lite::Executor* executor) const __ALWAYS_INLINE;

    PkPairIteratorPtr CreateIterator() const
    {
        file_system::FileReaderPtr fileReader(new file_system::NormalFileReader(mFileReader->GetFileNode()));
        return CreateIterator(fileReader);
    }

private:
    PkPairIteratorPtr CreateIterator(const file_system::FileReaderPtr& reader) const
    {
        PkPairIteratorPtr iterator;
        switch (mIndexConfig->GetPKLoadStrategyParam()->GetPrimaryKeyLoadMode()) {
        case config::PrimaryKeyLoadStrategyParam::SORTED_VECTOR:
            iterator.reset(new SortedPrimaryKeyPairSegmentIterator<Key>());
            break;
        case config::PrimaryKeyLoadStrategyParam::BLOCK_VECTOR:
            iterator.reset(new BlockPrimaryKeyPairSegmentIterator<Key>());
            break;
        case config::PrimaryKeyLoadStrategyParam::HASH_TABLE:
            INDEXLIB_FATAL_ERROR(UnSupported, "Not support create hash iterator");
            return PkPairIteratorPtr();
        default:
            INDEXLIB_FATAL_ERROR(UnSupported, "Not support unknown load mode[%d] for iterator",
                                 mIndexConfig->GetPKLoadStrategyParam()->GetPrimaryKeyLoadMode());
            return PkPairIteratorPtr();
        }
        iterator->Init(reader);
        return iterator;
    }

    file_system::FileReaderPtr GetFileReaderForLoadBloomFilter(const file_system::DirectoryPtr& directory)
    {
        if (mFileReader->GetOpenType() == file_system::FSOT_CACHE) {
            std::string relativePath;
            if (util::PathUtil::GetRelativePath(directory->GetLogicalPath(), mFileReader->GetLogicalPath(),
                                                relativePath)) {
                return directory->CreateFileReader(relativePath,
                                                   file_system::ReaderOption::NoCache(file_system::FSOT_BUFFERED));
            }
        }
        return file_system::FileReaderPtr(new file_system::NormalFileReader(mFileReader->GetFileNode()));
    }

private:
    char* mData;
    uint32_t mItemCount;
    file_system::FileReaderPtr mFileReader;
    config::PrimaryKeyIndexConfigPtr mIndexConfig;
    PrimaryKeySegmentFormatter<Key> mPkFormatter;
    file_system::ResourceFilePtr mResourceFile;
    autil::BloomFilter* mBloomFilter;

private:
    IE_LOG_DECLARE();
};

typedef PrimaryKeySegmentReaderTyped<uint64_t> UInt64PrimaryKeySegmentReader;
DEFINE_SHARED_PTR(UInt64PrimaryKeySegmentReader);

typedef PrimaryKeySegmentReaderTyped<autil::uint128_t> UInt128PrimaryKeySegmentReader;
DEFINE_SHARED_PTR(UInt128PrimaryKeySegmentReader);

////////////////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, PrimaryKeySegmentReaderTyped);

template <typename Key>
inline void PrimaryKeySegmentReaderTyped<Key>::Init(const config::PrimaryKeyIndexConfigPtr& indexConfig,
                                                    const PrimaryKeyLoadPlanPtr& plan)
{
    // TODO : this code is ulgy, such as @mItemCount only for sort array, @mDirectory only for block array
    mIndexConfig = indexConfig;
    mFileReader = PrimaryKeyLoader<Key>::Load(plan, indexConfig);

    // use mDirectory to create file reader or writer
    file_system::DirectoryPtr directory = plan->GetTargetFileDirectory();
    mData = (char*)mFileReader->GetBaseAddress();
    mPkFormatter.Init(indexConfig, mFileReader, directory);

    // TODO: pkhash & pkblock not right
    mItemCount = mFileReader->GetLogicLength() / sizeof(PKPair);

    uint32_t multipleNum = 0;
    uint32_t hashFuncNum = 0;
    if (!mFileReader->IsMemLock() &&
        mIndexConfig->GetPKLoadStrategyParam()->GetPrimaryKeyLoadMode() !=
            config::PrimaryKeyLoadStrategyParam::HASH_TABLE &&
        mIndexConfig->GetBloomFilterParamForPkReader(multipleNum, hashFuncNum)) {
        assert(directory);
        std::string bloomFilterFileName = "pk_bloom_filter";
        mResourceFile = directory->GetResourceFile(bloomFilterFileName);
        if (!mResourceFile || mResourceFile->Empty()) {
            file_system::ResourceFilePtr writeResource = directory->CreateResourceFile(bloomFilterFileName);
            size_t bitSize = mItemCount != 0 ? (size_t)mItemCount * multipleNum : multipleNum;
            if (bitSize > std::numeric_limits<uint32_t>::max()) {
                bitSize = std::numeric_limits<uint32_t>::max();
            }
            std::unique_ptr<autil::BloomFilter> bloomFilter(new autil::BloomFilter((uint32_t)bitSize, hashFuncNum));
            IE_LOG(INFO, "Begin create bloomFilter File [%s], bitSize [%lu], multipleNum [%u], hashFuncNum [%u]",
                   writeResource->GetLogicalPath().c_str(), bitSize, multipleNum, hashFuncNum);
            file_system::FileReaderPtr fileReader = GetFileReaderForLoadBloomFilter(directory);
            PkPairIteratorPtr pkPairIter = CreateIterator(fileReader);
            PKPair pkPair;
            while (pkPairIter->HasNext()) {
                pkPairIter->Next(pkPair);
                bloomFilter->Insert(pkPair.key);
            }
            writeResource->UpdateMemoryUse(bitSize / 8);
            writeResource->Reset(bloomFilter.release());
            mResourceFile = writeResource;
            IE_LOG(INFO, "Finish create bloomFilter File [%s], bitSize [%lu], multipleNum [%u], hashFuncNum [%u]",
                   writeResource->GetLogicalPath().c_str(), bitSize, multipleNum, hashFuncNum);
        }
        mBloomFilter = mResourceFile->GetResource<autil::BloomFilter>();
    } else {
        mBloomFilter = nullptr;
    }
}

template <typename Key>
inline future_lite::coro::Lazy<docid_t>
PrimaryKeySegmentReaderTyped<Key>::LookupAsync(const Key& hashKey, future_lite::Executor* executor) const
{
    if (mBloomFilter && !mBloomFilter->Contains(hashKey)) {
        co_return INVALID_DOCID;
    }
    co_return co_await mPkFormatter.FindAsync(mData, mItemCount, hashKey, executor);
}
}} // namespace indexlib::index

#endif //__INDEXLIB_PRIMARY_KEY_SEGMENT_READER_TYPED_H
