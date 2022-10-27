#ifndef __INDEXLIB_BLOCK_HASH_DICTIONARY_READER_H
#define __INDEXLIB_BLOCK_HASH_DICTIONARY_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_reader.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/util_define.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/common_disk_hash_dictionary_iterator.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/slice_file.h"
#include "indexlib/file_system/slice_file_reader.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/block_file_node.h"

IE_NAMESPACE_BEGIN(index);

template <typename KeyType>
class BlockHashDictionaryReaderTyped : public DictionaryReader
{
public:
    typedef std::tr1::shared_ptr<DictionaryIterator> DictionaryIteratorPtr;
    typedef std::tr1::shared_ptr<CommonDiskHashDictionaryIteratorTyped<KeyType>> OnDiskDictionaryIteratorPtr;
    using HashItem = HashDictionaryItemTyped<KeyType>;

public:
    BlockHashDictionaryReaderTyped();
    ~BlockHashDictionaryReaderTyped();
    
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
    future_lite::Future<LookupResult> DoLookupAsync(KeyType key, file_system::ReadOption option);

private:
    file_system::DirectoryPtr mDirectory;
    file_system::FileReaderPtr mFileReader;
    file_system::BlockFileNodePtr mBlockFileNode;
    uint32_t mBlockCount;
    size_t mBlockIndexOffset;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, BlockHashDictionaryReaderTyped);

template <typename KeyType>
BlockHashDictionaryReaderTyped<KeyType>::BlockHashDictionaryReaderTyped()
    : mBlockCount(0)
    , mBlockIndexOffset(0)
{
}

template <typename KeyType>
BlockHashDictionaryReaderTyped<KeyType>::~BlockHashDictionaryReaderTyped() 
{
}

template <typename KeyType>
void BlockHashDictionaryReaderTyped<KeyType>::Open(
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
}

template <typename KeyType>
void BlockHashDictionaryReaderTyped<KeyType>::Open(
        const file_system::DirectoryPtr& directory,
        const std::string& fileName)
{
    file_system::FileReaderPtr fileReader =
        directory->CreateFileReader(fileName, file_system::FSOT_CACHE);
    uint32_t blockCount = 0;
    size_t dictDataLen = 0;
    ReadBlockMeta<KeyType>(fileReader, sizeof(uint32_t), blockCount, dictDataLen);
    assert(fileReader->GetBaseAddress() == NULL);
    Open(directory, fileReader, blockCount, dictDataLen);
}

template <typename KeyType>
bool BlockHashDictionaryReaderTyped<KeyType>::Lookup(dictkey_t key, dictvalue_t& value)
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
inline DictionaryIteratorPtr BlockHashDictionaryReaderTyped<KeyType>::CreateIterator() const
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
            new CommonDiskHashDictionaryIteratorTyped<KeyType>(
                    bufferFileReader, mBlockIndexOffset));
    return iterator;
}

template <typename KeyType>
inline future_lite::Future<DictionaryReader::LookupResult>
BlockHashDictionaryReaderTyped<KeyType>::DoLookupAsync(KeyType key, file_system::ReadOption option)
{
    // TODO: support async lookup
    file_system::BlockFileAccessor* accessor = mBlockFileNode->GetAccessor();
    assert(accessor);
    uint32_t blockIdx = key % mBlockCount;
    uint32_t cursor = 0;
    size_t cursorOffset = blockIdx * sizeof(uint32_t) + mBlockIndexOffset;
    if (accessor->Read(&cursor, sizeof(uint32_t), cursorOffset, option) != sizeof(uint32_t))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "read block fail from file [%s], offset [%lu]",
                             accessor->GetFilePath(), cursorOffset);
    }

    while (cursor != std::numeric_limits<uint32_t>::max())
    {
        HashItem item;
        if (accessor->Read(&item, sizeof(item), cursor * sizeof(HashItem), option) != sizeof(item))
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed,
                    "read HashItem fail from file [%s], offset [%lu]",
                    accessor->GetFilePath(), cursor * sizeof(HashItem));
        }
        
        if (item.key == (KeyType)key)
        {
            return future_lite::makeReadyFuture(std::make_pair(true, dictvalue_t(item.value)));
        }
        // key sorted in file
        if (item.key < (KeyType)key)
        {
            return future_lite::makeReadyFuture(std::make_pair(false, dictvalue_t()));
        }
        cursor = item.next;
    }
    return future_lite::makeReadyFuture(std::make_pair(false, dictvalue_t()));    
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BLOCK_HASH_DICTIONARY_READER_H
