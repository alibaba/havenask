#ifndef __INDEXLIB_INTEGRATE_HASH_DICTIONARY_READER_H
#define __INDEXLIB_INTEGRATE_HASH_DICTIONARY_READER_H

#include <tr1/memory>
#include "fslib/fslib.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_reader.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/hash_dictionary_iterator.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/directory.h"

IE_NAMESPACE_BEGIN(index);

template <typename KeyType>
class IntegrateHashDictionaryReaderTyped : public DictionaryReader
{
public:
    typedef std::tr1::shared_ptr<DictionaryIterator> DictionaryIteratorPtr;
    typedef std::tr1::shared_ptr<HashDictionaryIteratorTyped<KeyType> >  HashDictionaryIteratorPtr;
    using HashItem = HashDictionaryItemTyped<KeyType>;
        
public:
    IntegrateHashDictionaryReaderTyped();
    ~IntegrateHashDictionaryReaderTyped();

public:
    void Open(const file_system::FileReaderPtr& fileReader,
              uint32_t blockCount, size_t blockDataOffset);
    
public:
    void Open(const file_system::DirectoryPtr& directory, 
              const std::string& fileName) override;

    bool Lookup(dictkey_t key, dictvalue_t& value) override;

    DictionaryIteratorPtr CreateIterator() const override
    {
        uint32_t dictItemCount =
            ((uint8_t*)mBlockIndex - (uint8_t*)mData) / sizeof(HashItem);
        return HashDictionaryIteratorPtr(
                new HashDictionaryIteratorTyped<KeyType>(
                    mData, dictItemCount));
    }

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

protected:
    file_system::FileReaderPtr mFileReader;
    HashItem* mData;

private:
    uint32_t* mBlockIndex;
    uint32_t mBlockCount;

private:
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, IntegrateHashDictionaryReaderTyped);

template <typename KeyType>
IntegrateHashDictionaryReaderTyped<KeyType>::IntegrateHashDictionaryReaderTyped()
    : mData(NULL)
    , mBlockIndex(NULL)
    , mBlockCount(0)
{
}

template <typename KeyType>
IntegrateHashDictionaryReaderTyped<KeyType>::~IntegrateHashDictionaryReaderTyped() 
{
}

template <typename KeyType>
void IntegrateHashDictionaryReaderTyped<KeyType>::Open(
        const file_system::FileReaderPtr& fileReader,
        uint32_t blockCount, size_t blockDataOffset)
{
    assert(fileReader);
    mFileReader = fileReader;
    mData = (HashItem*)mFileReader->GetBaseAddress();
    mBlockCount = blockCount;
    mBlockIndex = (uint32_t*)((uint8_t*)mData + blockDataOffset);
}

template <typename KeyType>
void IntegrateHashDictionaryReaderTyped<KeyType>::Open(
        const file_system::DirectoryPtr& directory,
        const std::string& fileName)
{
    file_system::FileReaderPtr fileReader =
        directory->CreateIntegratedFileReader(fileName);
    
    uint32_t blockCount = 0;
    size_t dictDataLen = 0;
    ReadBlockMeta<KeyType>(fileReader, sizeof(uint32_t), blockCount, dictDataLen);
    Open(fileReader, blockCount, dictDataLen);
}

template <typename KeyType>
bool IntegrateHashDictionaryReaderTyped<KeyType>::Lookup(dictkey_t key, dictvalue_t& value)
{
    uint32_t blockIdx = key % mBlockCount;
    uint32_t cursor = mBlockIndex[blockIdx];
    while (cursor != std::numeric_limits<uint32_t>::max())
    {
        const HashItem& item = mData[cursor];
        if (item.key == (KeyType)key)
        {
            value = item.value;
            return true;
        }

        // key sorted in file
        if (item.key < (KeyType)key) 
        {
            return false;
        }
        cursor = item.next;
    }
    return false;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INTEGRATE_HASH_DICTIONARY_READER_H
