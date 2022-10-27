#ifndef __INDEXLIB_BLOCK_TIERED_DICTIONARY_READER_H
#define __INDEXLIB_BLOCK_TIERED_DICTIONARY_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_reader.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/util_define.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/common_disk_tiered_dictionary_iterator.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/slice_file.h"
#include "indexlib/file_system/slice_file_reader.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/block_file_node.h"

IE_NAMESPACE_BEGIN(index);

template <typename KeyType>
class BlockTieredDictionaryReaderTyped : public DictionaryReader
{
public:
    typedef std::tr1::shared_ptr<DictionaryIterator> DictionaryIteratorPtr;
    typedef std::tr1::shared_ptr<CommonDiskTieredDictionaryIteratorTyped<KeyType>> OnDiskDictionaryIteratorPtr;

public:
    BlockTieredDictionaryReaderTyped();
    ~BlockTieredDictionaryReaderTyped();
    
public:
    void Open(const file_system::DirectoryPtr& directory,
              const file_system::FileReaderPtr& fileReader,
              uint32_t blockCount, size_t blockIndexOffset);
    
public:
    /* override */ void Open(const file_system::DirectoryPtr& directory, 
                             const std::string& fileName);

    /* override */ bool Lookup(dictkey_t key, dictvalue_t& value);
    future_lite::Future<DictionaryReader::LookupResult>
    LookupAsync(dictkey_t key, file_system::ReadOption option) override
    {
        return DoLookupAsync(key, option);
    }

    /* override */ DictionaryIteratorPtr CreateIterator() const;

private:
    typedef DictionaryItemTyped<KeyType> DictionaryItem;
    class DictionaryCompare
    {
    public:
        bool operator () (const DictionaryItem& left, const KeyType& rightKey)
        {
            return left.first < rightKey;
        }

        bool operator () (const KeyType& leftKey, const DictionaryItem& right)
        {
            return leftKey < right.first;
        }
    };

private:
    future_lite::Future<LookupResult> DoLookupAsync(KeyType key, file_system::ReadOption option);
    file_system::FileReaderPtr CreateBlockFileReader();

    bool LocateBlockIndex(KeyType* begin, KeyType* end, KeyType key,
                          size_t& itemBlockOffset, size_t& itemBlockSize);

    bool LocateItem(DictionaryItem* blockStart, DictionaryItem* blockEnd,
                    KeyType key, dictvalue_t& value);
    
private:
    file_system::DirectoryPtr mDirectory;
    file_system::FileReaderPtr mFileReader;
    file_system::FileReaderPtr mBlockIndexReader;
    file_system::BlockFileNodePtr mBlockFileNode;
    KeyType* mBlockIndex;
    uint32_t mBlockCount;
    size_t mBlockIndexOffset;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, BlockTieredDictionaryReaderTyped);

template <typename KeyType>
BlockTieredDictionaryReaderTyped<KeyType>::BlockTieredDictionaryReaderTyped()
    : mBlockIndex(NULL)
    , mBlockCount(0)
    , mBlockIndexOffset(0)
{
}

template <typename KeyType>
BlockTieredDictionaryReaderTyped<KeyType>::~BlockTieredDictionaryReaderTyped() 
{
}

template <typename KeyType>
void BlockTieredDictionaryReaderTyped<KeyType>::Open(
        const file_system::DirectoryPtr& directory,
        const file_system::FileReaderPtr& fileReader,
        uint32_t blockCount, size_t blockIndexOffset)
{
    assert(fileReader);
    mDirectory = directory;
    mFileReader = fileReader;
    mBlockCount = blockCount;
    mBlockIndexOffset = blockIndexOffset;

    mBlockFileNode = DYNAMIC_POINTER_CAST(file_system::BlockFileNode,
            fileReader->GetFileNode());
    if (!mBlockFileNode)
    {
        INDEXLIB_FATAL_ERROR(
                UnSupported, "open [%s] with wrong open type [%d], should be FSOT_CACHE",
                fileReader->GetPath().c_str(), fileReader->GetOpenType());
    }
    
    mBlockIndexReader = CreateBlockFileReader();
    if (mBlockIndexReader)
    {
        mBlockIndex = (KeyType*)mBlockIndexReader->GetBaseAddress();
    }
    else
    {
        mBlockIndex = NULL;
    }
}

template <typename KeyType>
void BlockTieredDictionaryReaderTyped<KeyType>::Open(
        const file_system::DirectoryPtr& directory,
        const std::string& fileName)
{
    file_system::FileReaderPtr fileReader =
        directory->CreateFileReader(fileName, file_system::FSOT_CACHE);
    uint32_t blockCount = 0;
    size_t dictDataLen = 0;
    ReadBlockMeta<KeyType>(fileReader, sizeof(KeyType), blockCount, dictDataLen);
    assert(fileReader->GetBaseAddress() == NULL);
    Open(directory, fileReader, blockCount, dictDataLen);
}

template <typename KeyType>
bool BlockTieredDictionaryReaderTyped<KeyType>::Lookup(dictkey_t key, dictvalue_t& value)
{
    file_system::ReadOption option;
    option.advice = storage::IO_ADVICE_LOW_LATENCY;
    std::pair<bool, dictvalue_t> result = DoLookupAsync((KeyType)key, option).get();
    if (result.first)
    {
        value = result.second;
    }
    return result.first;
}

template <typename KeyType>
inline DictionaryIteratorPtr BlockTieredDictionaryReaderTyped<KeyType>::CreateIterator() const
{
    std::string fileName;
    util::PathUtil::GetRelativePath(mDirectory->GetPath(),
                                    mFileReader->GetPath(), fileName);
    file_system::FileReaderPtr fileReader = mDirectory->CreateFileReader(
            fileName, file_system::FSOT_BUFFERED);
    assert(fileReader);

    file_system::BufferedFileReaderPtr bufferFileReader = DYNAMIC_POINTER_CAST(
            file_system::BufferedFileReader, fileReader);
    assert(bufferFileReader);
    OnDiskDictionaryIteratorPtr iterator(
            new CommonDiskTieredDictionaryIteratorTyped<KeyType>(
                    bufferFileReader, mBlockIndexOffset));
    return iterator;
}

template <typename KeyType>
inline file_system::FileReaderPtr
BlockTieredDictionaryReaderTyped<KeyType>::CreateBlockFileReader()
{
    assert(mDirectory);
    assert(mFileReader);

    if (mBlockCount == 0)
    {
        return file_system::FileReaderPtr();
    }
    std::string fileName;
    util::PathUtil::GetRelativePath(mDirectory->GetPath(),
                                    mFileReader->GetPath(), fileName);
    std::string sliceFileName = fileName + ".block_index";
    
    auto sliceFile = mDirectory->GetSliceFile(sliceFileName);
    if (sliceFile)
    {
        return sliceFile->CreateSliceFileReader();
    }

    size_t sliceLen = sizeof(KeyType) * mBlockCount;
    sliceFile = mDirectory->CreateSliceFile(sliceFileName, sliceLen, 1);
    sliceFile->ReserveSliceFile(sliceLen);
    file_system::SliceFileReaderPtr sliceReader = sliceFile->CreateSliceFileReader();
    if (sliceLen != mFileReader->Read(
                    sliceReader->GetBaseAddress(), sliceLen, mBlockIndexOffset))
    {
        INDEXLIB_FATAL_ERROR(FileIO, "read block index data fail in file [%s]",
                             mFileReader->GetPath().c_str());
    }
    return sliceReader;
}

template <typename KeyType>
inline bool BlockTieredDictionaryReaderTyped<KeyType>::LocateBlockIndex(
        KeyType* begin, KeyType* end, KeyType key,
        size_t& itemBlockOffset, size_t& itemBlockSize)
{
    if (unlikely(begin == NULL))
    {
        return false;
    }
    KeyType* data = std::lower_bound(begin, end, key);
    if (data >= end)
    {
        return false;
    }
    uint32_t subBlockIdx = data - begin;
    size_t blockUnitSize = ITEM_COUNT_PER_BLOCK * sizeof(DictionaryItem);
    itemBlockOffset = subBlockIdx * blockUnitSize;
    itemBlockSize = (subBlockIdx == mBlockCount - 1) ?
                    mBlockIndexOffset - itemBlockOffset : blockUnitSize;
    return true;
}

template <typename KeyType>
inline bool BlockTieredDictionaryReaderTyped<KeyType>::LocateItem(
        DictionaryItem* blockStart, DictionaryItem* blockEnd,
        KeyType key, dictvalue_t& value)
{
    DictionaryCompare comp;
    DictionaryItem* dictItem = std::lower_bound(blockStart, blockEnd, key, comp);
    if (dictItem == blockEnd || dictItem->first != key)
    {
        return false;
    }
    value = dictItem->second;
    return true;
}

template <typename KeyType>
inline future_lite::Future<DictionaryReader::LookupResult>
BlockTieredDictionaryReaderTyped<KeyType>::DoLookupAsync(KeyType key, file_system::ReadOption option)
{
    size_t itemBlockOffset = 0;
    size_t itemBlockSize = 0;
    if (!LocateBlockIndex(mBlockIndex, mBlockIndex + mBlockCount, key,
                          itemBlockOffset, itemBlockSize))
    {
        return future_lite::makeReadyFuture(std::make_pair(false, dictvalue_t()));
    }

    file_system::BlockFileAccessor* accessor = mBlockFileNode->GetAccessor();
    assert(accessor);
    size_t blockCount = accessor->GetBlockCount(itemBlockOffset, itemBlockSize);
    if (blockCount == 1)
    {
        // optimize: no copy from cache

        return accessor->GetBlockAsync(itemBlockOffset, option)
            .thenTry([this, itemBlockOffset, itemBlockSize, accessor, key](
                         future_lite::Try<util::BlockHandle>&& t) mutable {
                try
                {
                    auto& blockHandle = t.value();
                    uint8_t* data = blockHandle.GetData();
                    size_t inBlockOffset = accessor->GetInBlockOffset(itemBlockOffset);
                    DictionaryItem* blockStart = (DictionaryItem*)(data + inBlockOffset);
                    DictionaryItem* blockEnd = (DictionaryItem*)((char*)blockStart + itemBlockSize);
                    dictvalue_t value = 0;
                    auto ret = LocateItem(blockStart, blockEnd, key, value);
                    return std::make_pair(ret, value);
                }
                catch (std::exception& e)
                {
                    IE_LOG(ERROR, "caught exception [%s]", e.what());
                    INDEXLIB_FATAL_ERROR(IndexCollapsed,
                        "read data from [%s] fail, offset[%lu], len[%lu]",
                        mFileReader->GetPath().c_str(), itemBlockOffset, itemBlockSize);
                }
            });
    }
    // cross over 2 cache blocks, will copy data

    constexpr size_t bufferSize = 2048;
    std::vector<char> dataBuf(bufferSize);

    static_assert(bufferSize >= (sizeof(DictionaryItem) * ITEM_COUNT_PER_BLOCK),
                  "data buffer is not big enough");

    auto buf = dataBuf.data();
    return mFileReader->ReadAsync(buf, itemBlockSize, itemBlockOffset, option)
        .thenTry([this, itemBlockOffset, itemBlockSize, buffer = std::move(dataBuf), key]
                 (future_lite::Try<size_t>&& t) mutable {
            if (t.hasError() || t.value() != itemBlockSize)
            {
                INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "read data from [%s] fail, offset[%lu], len[%lu]",
                             mFileReader->GetPath().c_str(), itemBlockOffset, itemBlockSize);
            }
            DictionaryItem* blockStart = (DictionaryItem*)buffer.data();
            DictionaryItem* blockEnd = (DictionaryItem*)(buffer.data() + itemBlockSize);
            dictvalue_t value = 0;
            auto ret = LocateItem(blockStart, blockEnd, key, value);
            return std::make_pair(ret, value);
        });
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BLOCK_TIERED_DICTIONARY_READER_H
