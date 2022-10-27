#ifndef __INDEXLIB_HASH_DICTIONARY_WRITER_H
#define __INDEXLIB_HASH_DICTIONARY_WRITER_H

#include <autil/mem_pool/pool_allocator.h>
#include "indexlib/util/simple_pool.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_writer.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/util_define.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/util/prime_number_table.h"
#include "indexlib/util/slice_array/byte_aligned_slice_array.h"
#include <tr1/memory>

IE_NAMESPACE_BEGIN(index);

template <typename DictKeyType>
class HashDictionaryWriter : public DictionaryWriter
{
public:
    static const uint32_t INITIAL_BLOCK_COUNT = 32 * 1024;
    static constexpr double MAX_REHASH_DATA_RATIO = 1.2f;
    static constexpr double MIN_REHASH_DATA_RATIO = 0.2f;        
    typedef typename autil::mem_pool::pool_allocator<uint32_t> AllocatorType;
    using HashItem = HashDictionaryItemTyped<DictKeyType>;

public:
    HashDictionaryWriter(autil::mem_pool::PoolBase* pool);
    ~HashDictionaryWriter();

public:
    // will use memory buffer to rehash, then write to mFile
    void Open(const file_system::DirectoryPtr& directory,
              const std::string& fileName) override;

    // will directly write to mFile
    void Open(const file_system::DirectoryPtr& directory,
              const std::string& fileName, size_t itemCount) override;
    
    void Close() override;
    void AddItem(dictkey_t key, dictvalue_t offset) override;

    uint32_t GetItemCount() const { return mItemCount; }

    static size_t GetInitialMemUse() 
    { return INITIAL_BLOCK_COUNT * sizeof(DictKeyType); }

private:
    void CheckKeySequence(DictKeyType key);
    void Reserve(size_t itemCount, size_t blockNum);
    void ResetBlock(size_t blockNum);
    bool NeedRehash() const;
    void Rehash();

private:
    uint32_t mItemCount;
    DictKeyType mLastKey;
    std::vector<uint32_t, AllocatorType> mBlockIndex;
    file_system::FileWriterPtr mFile;
    autil::mem_pool::PoolBase* mPool;
    util::ByteAlignedSliceArray* mDataSlice;
    static const size_t SLICE_LEN = 512 * 1024;

private:
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, HashDictionaryWriter);

template <typename DictKeyType>
HashDictionaryWriter<DictKeyType>::HashDictionaryWriter(
        autil::mem_pool::PoolBase* pool)
    : mItemCount(0)
    , mLastKey(0)
    , mBlockIndex(AllocatorType(pool))
    , mPool(pool)
    , mDataSlice(NULL)
{
    ResetBlock(util::PrimeNumberTable::FindPrimeNumber(INITIAL_BLOCK_COUNT));
}

template <typename DictKeyType>
HashDictionaryWriter<DictKeyType>::~HashDictionaryWriter() 
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mDataSlice);
}

template <typename DictKeyType>
void HashDictionaryWriter<DictKeyType>::Open(
        const file_system::DirectoryPtr& directory, const std::string& fileName)
{
    mFile = directory->CreateFileWriter(fileName);
    mDataSlice = IE_POOL_COMPATIBLE_NEW_CLASS(mPool, util::ByteAlignedSliceArray,
            SLICE_LEN, 10, NULL, mPool);
}

template <typename DictKeyType>
void HashDictionaryWriter<DictKeyType>::Open(
        const file_system::DirectoryPtr& directory,
        const std::string& fileName, size_t itemCount)
{
    size_t blockNum = util::PrimeNumberTable::FindPrimeNumber(itemCount);
    ResetBlock(blockNum);
    mFile = directory->CreateFileWriter(fileName);
    Reserve(itemCount, blockNum);
}

template <typename DictKeyType>
void HashDictionaryWriter<DictKeyType>::AddItem(dictkey_t key, dictvalue_t offset)
{
    CheckKeySequence((DictKeyType)key);
    
    HashItem item;
    item.key = key;
    uint32_t blockNum = (uint32_t)mBlockIndex.size();
    uint32_t blockIdx = key % blockNum;
    item.next = mBlockIndex[blockIdx];
    item.value = offset;
    
    if (mDataSlice)
    {
        mDataSlice->SetTypedValue(mItemCount * sizeof(HashItem), item);
    }
    else
    {
        assert(mFile != NULL);
        mFile->Write((void*)(&item), sizeof(HashItem));
    }
    mLastKey = key;
    mBlockIndex[blockIdx] = mItemCount;
    ++mItemCount;
}

template <typename DictKeyType>
void HashDictionaryWriter<DictKeyType>::Reserve(size_t itemCount, size_t blockNum)
{
    if (!mFile) { return; }

    size_t reserveSize = itemCount * sizeof(HashItem);
    size_t blockSize = blockNum * sizeof(uint32_t);
    reserveSize += blockSize;
    reserveSize += (sizeof(uint32_t) * 2);  // blocknum & magic tail
    mFile->ReserveFileNode(reserveSize);
}

template <typename DictKeyType>
void HashDictionaryWriter<DictKeyType>::ResetBlock(size_t blockNum)
{
    mBlockIndex.resize(blockNum);
    size_t batchSize = blockNum / 8;
    size_t remainSize = blockNum % 8;
    uint32_t* cursor = (uint32_t*)mBlockIndex.data();
    for (size_t i = 0; i < batchSize; i++)
    {
        cursor[0] = std::numeric_limits<uint32_t>::max();
        cursor[1] = std::numeric_limits<uint32_t>::max();
        cursor[2] = std::numeric_limits<uint32_t>::max();
        cursor[3] = std::numeric_limits<uint32_t>::max();
        cursor[4] = std::numeric_limits<uint32_t>::max();
        cursor[5] = std::numeric_limits<uint32_t>::max();
        cursor[6] = std::numeric_limits<uint32_t>::max();
        cursor[7] = std::numeric_limits<uint32_t>::max();
        cursor += 8;
    }
    for (size_t i = 0; i < remainSize; i++)
    {
        *cursor = std::numeric_limits<uint32_t>::max();
        ++cursor;
    }
}

template <typename DictKeyType>
void HashDictionaryWriter<DictKeyType>::Close()
{
    if (mFile == NULL)
    {
        return;
    }

    if (mDataSlice)
    {
        if (NeedRehash())
        {
            Rehash();
        }
        
        for (size_t i = 0; i < mDataSlice->GetSliceNum(); ++i)
        {
            mFile->Write(mDataSlice->GetSlice(i), mDataSlice->GetSliceDataLen(i));
        }
        IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mDataSlice);
        mDataSlice = NULL;
    }
    
    void* data = mBlockIndex.data();
    uint32_t blockNum = (uint32_t)mBlockIndex.size();
    uint32_t magicNum = DICTIONARY_MAGIC_NUMBER;
    uint32_t blockSize = blockNum * sizeof(uint32_t);
    mFile->Write(data, blockSize);
    mFile->Write((void*)(&blockNum), sizeof(uint32_t));
    mFile->Write((void*)(&magicNum), sizeof(uint32_t));
    if (NeedRehash())
    {
        IE_LOG(INFO, "need rehash for file [%s], itemCount [%u], bucketNum [%u]",
               mFile->GetPath().c_str(), mItemCount, blockNum);
    }
    mFile->Close();
    mFile.reset();
}

template <typename DictKeyType>
inline void HashDictionaryWriter<DictKeyType>::CheckKeySequence(DictKeyType key)
{
    if (mItemCount == 0 || key > mLastKey)
    {
        return;
    }
    
    INDEXLIB_FATAL_ERROR(InconsistentState, "should add key by sequence:"
                         "lastKey [%lu], currentKey [%lu], currentItemCount [%u]",
                         (uint64_t)mLastKey, (uint64_t)key, mItemCount);
}

template <typename DictKeyType>
inline bool HashDictionaryWriter<DictKeyType>::NeedRehash() const
{
    double capacityRatio = (double)mItemCount / (double)mBlockIndex.size();
    return capacityRatio > MAX_REHASH_DATA_RATIO ||
        capacityRatio < MIN_REHASH_DATA_RATIO;
}

template <typename DictKeyType>
inline void HashDictionaryWriter<DictKeyType>::Rehash()
{
    IE_LOG(INFO, "rehash for file [%s], old blockNum [%lu] itemCount [%u]",
           mFile->GetPath().c_str(), mBlockIndex.size(), mItemCount);
    assert(mDataSlice);
    size_t blockNum = util::PrimeNumberTable::FindPrimeNumber(mItemCount);
    ResetBlock(blockNum);
    HashItem item;
    for (uint32_t i = 0; i < mItemCount; i++)
    {
        mDataSlice->GetTypedValue(i * sizeof(HashItem), item);
        uint32_t blockIdx = item.key % blockNum;
        item.next = mBlockIndex[blockIdx];
        mDataSlice->SetTypedValue(i * sizeof(HashItem), item);
        mBlockIndex[blockIdx] = i;
    }
    IE_LOG(INFO, "rehash for file [%s], new blockNum [%lu] itemCount [%u]",
           mFile->GetPath().c_str(), mBlockIndex.size(), mItemCount);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_HASH_DICTIONARY_WRITER_H
