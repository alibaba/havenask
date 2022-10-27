#ifndef __INDEXLIB_TIERED_DICTIONARY_WRITER_H
#define __INDEXLIB_TIERED_DICTIONARY_WRITER_H

#include <autil/mem_pool/pool_allocator.h>
#include "indexlib/util/simple_pool.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/misc/exception.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_writer.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/util_define.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include <tr1/memory>

IE_NAMESPACE_BEGIN(index);

template <typename DictKeyType>
class TieredDictionaryWriter : public DictionaryWriter
{
public:
    static const uint32_t INITIAL_BLOCK_COUNT = 32 * 1024;
    typedef typename autil::mem_pool::pool_allocator<DictKeyType> AllocatorType;

public:
    TieredDictionaryWriter(autil::mem_pool::PoolBase* pool);
    ~TieredDictionaryWriter();

public:
    // void Open(const std::string& path) override;
    void Open(const file_system::DirectoryPtr& directory,
              const std::string& fileName) override;
    
    void Open(const file_system::DirectoryPtr& directory,
              const std::string& fileName, size_t itemCount) override;
    
    void Close() override;
    void AddItem(dictkey_t key, dictvalue_t offset) override;

    uint32_t GetItemCount() const { return mItemCount; }

    static size_t GetInitialMemUse() 
    { return INITIAL_BLOCK_COUNT * sizeof(DictKeyType); }

private:
    void DoAddItem(DictKeyType key, dictvalue_t offset);
    void CheckKeySequence(DictKeyType key);
    void Reserve(size_t itemCount);

private:
    uint32_t mItemCount;
    uint32_t mItemCountInBlock;
    DictKeyType mLastKey;
    std::vector<DictKeyType, AllocatorType> mBlockIndex;
    file_system::FileWriterPtr mFile;
    
// private:
//     static const uint32_t BUFFER_SIZE = 1024 * 1024 * 2;
private:
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, TieredDictionaryWriter);

template <typename DictKeyType>
TieredDictionaryWriter<DictKeyType>::TieredDictionaryWriter(
        autil::mem_pool::PoolBase* pool)
    : mItemCount(0)
    , mItemCountInBlock(0)
    , mLastKey(0)
    , mBlockIndex(AllocatorType(pool))
{
    mBlockIndex.reserve(INITIAL_BLOCK_COUNT);
}

template <typename DictKeyType>
TieredDictionaryWriter<DictKeyType>::~TieredDictionaryWriter() 
{
}

template <typename DictKeyType>
void TieredDictionaryWriter<DictKeyType>::Open(
        const file_system::DirectoryPtr& directory, const std::string& fileName)
{
    mFile = directory->CreateFileWriter(fileName);
}


template <typename DictKeyType>
void TieredDictionaryWriter<DictKeyType>::Open(
        const file_system::DirectoryPtr& directory,
        const std::string& fileName, size_t itemCount)
{
    Open(directory, fileName);
    Reserve(itemCount);
}

// template <typename DictKeyType>
// void TieredDictionaryWriter<DictKeyType>::Open(const std::string& path)
// {
//     file_system::FSWriterParam writerParam;
//     writerParam.atomicDump = false;
//     writerParam.copyOnDump = false;
//     mFile.reset(new file_system::BufferedFileWriter(writerParam, BUFFER_SIZE));
//     mFile->Open(path);
// }

template <typename DictKeyType>
void TieredDictionaryWriter<DictKeyType>::AddItem(dictkey_t key, dictvalue_t offset)
{
    CheckKeySequence((DictKeyType)key);
    DoAddItem((DictKeyType)key, offset);
}

template <typename DictKeyType>
void TieredDictionaryWriter<DictKeyType>::Reserve(size_t itemCount)
{
    if (!mFile) { return; }

    size_t reserveSize = itemCount * (sizeof(DictKeyType) + sizeof(dictvalue_t));
    
    size_t blockNum = (itemCount + ITEM_COUNT_PER_BLOCK - 1) / ITEM_COUNT_PER_BLOCK;
    size_t blockSize = blockNum * sizeof(DictKeyType);
    reserveSize += blockSize;
    reserveSize += (sizeof(uint32_t) * 2);  // blocknum & magic tail
    
    mFile->ReserveFileNode(reserveSize);
}

template <typename DictKeyType>
void TieredDictionaryWriter<DictKeyType>::DoAddItem(DictKeyType key, dictvalue_t offset)
{
    assert(mFile != NULL);
    mFile->Write((void*)(&key), sizeof(DictKeyType));
    mFile->Write((void*)(&offset), sizeof(dictvalue_t));

    mItemCountInBlock++;
    if (mItemCountInBlock == ITEM_COUNT_PER_BLOCK)
    {
        mBlockIndex.push_back(key);
        mItemCountInBlock = 0;
    }
    mLastKey = key;
    mItemCount++;
}

template <typename DictKeyType>
void TieredDictionaryWriter<DictKeyType>::Close()
{
    if (mFile == NULL)
    {
        return;
    }
    if (mItemCountInBlock != 0)
    {
        mBlockIndex.push_back(mLastKey);
    }
    
    void* data = &(mBlockIndex[0]);
    uint32_t blockNum = mBlockIndex.size();
    uint32_t magicNum = DICTIONARY_MAGIC_NUMBER;

    uint32_t blockSize = blockNum * sizeof(DictKeyType);
    mFile->Write(data, blockSize);
    mFile->Write((void*)(&blockNum), sizeof(uint32_t));
    mFile->Write((void*)(&magicNum), sizeof(uint32_t));

    mFile->Close();
    mFile.reset();
}

template <typename DictKeyType>
inline void TieredDictionaryWriter<DictKeyType>::CheckKeySequence(DictKeyType key)
{
    if (mItemCount == 0 || key > mLastKey)
    {
        return;
    }
    
    INDEXLIB_FATAL_ERROR(InconsistentState, "should add key by sequence:"
                         "lastKey [%lu], currentKey [%lu], currentItemCount [%u]",
                         (uint64_t)mLastKey, (uint64_t)key, mItemCount);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TIERED_DICTIONARY_WRITER_H
