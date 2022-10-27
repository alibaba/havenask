#ifndef __INDEXLIB_INTEGRATE_TIERED_DICTIONARY_READER_H
#define __INDEXLIB_INTEGRATE_TIERED_DICTIONARY_READER_H

#include <tr1/memory>
#include "fslib/fslib.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

#include "indexlib/misc/exception.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_reader.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_iterator.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/directory.h"

IE_NAMESPACE_BEGIN(index);

template <typename KeyType>
class IntegrateTieredDictionaryReaderTyped : public DictionaryReader
{
public:
    typedef std::tr1::shared_ptr<DictionaryIterator> DictionaryIteratorPtr;
    typedef std::tr1::shared_ptr<TieredDictionaryIteratorTyped<KeyType> >  TieredDictionaryIteratorPtr;

public:
    IntegrateTieredDictionaryReaderTyped();
    ~IntegrateTieredDictionaryReaderTyped();

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
            ((uint8_t*)mBlockIndex - (uint8_t*)mData) / sizeof(DictionaryItem);
        return TieredDictionaryIteratorPtr(
                new TieredDictionaryIteratorTyped<KeyType>(
                    mBlockIndex, mBlockCount,
                    (DictionaryItem*)mData, dictItemCount));
    }

private:
    bool DoLookup(KeyType key, dictvalue_t& value);

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
    void* mData;

private:
    KeyType* mBlockIndex;
    uint32_t mBlockCount;

private:
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, IntegrateTieredDictionaryReaderTyped);

template <typename KeyType>
IntegrateTieredDictionaryReaderTyped<KeyType>::IntegrateTieredDictionaryReaderTyped()
    : mData(NULL)
    , mBlockIndex(NULL)
    , mBlockCount(0)
{
}

template <typename KeyType>
IntegrateTieredDictionaryReaderTyped<KeyType>::~IntegrateTieredDictionaryReaderTyped() 
{
}

template <typename KeyType>
void IntegrateTieredDictionaryReaderTyped<KeyType>::Open(
        const file_system::FileReaderPtr& fileReader,
        uint32_t blockCount, size_t blockDataOffset)
{
    assert(fileReader);
    mFileReader = fileReader;
    mData = (uint8_t*)mFileReader->GetBaseAddress();
    mBlockCount = blockCount;
    mBlockIndex = (KeyType*)((uint8_t*)mData + blockDataOffset);
    mFileReader->Lock(blockDataOffset, mBlockCount * sizeof(KeyType));
}

template <typename KeyType>
void IntegrateTieredDictionaryReaderTyped<KeyType>::Open(
        const file_system::DirectoryPtr& directory,
        const std::string& fileName)
{
    file_system::FileReaderPtr fileReader =
        directory->CreateIntegratedFileReader(fileName);
    
    uint32_t blockCount = 0;
    size_t dictDataLen = 0;
    ReadBlockMeta<KeyType>(fileReader, sizeof(KeyType), blockCount, dictDataLen);
    Open(fileReader, blockCount, dictDataLen);
}

template <typename KeyType>
bool IntegrateTieredDictionaryReaderTyped<KeyType>::Lookup(dictkey_t key, dictvalue_t& value)
{
    return DoLookup((KeyType)key, value);
}

template <typename KeyType>
inline bool IntegrateTieredDictionaryReaderTyped<KeyType>::DoLookup(KeyType key, dictvalue_t& value)
{
    KeyType* data = std::lower_bound(mBlockIndex, mBlockIndex + mBlockCount, key);
    if (data < mBlockIndex + mBlockCount)
    {
        uint32_t subBlockNumber = data - mBlockIndex;
        DictionaryItem* blockStart = (DictionaryItem*)mData + 
                                     subBlockNumber * ITEM_COUNT_PER_BLOCK;
        DictionaryItem* blockEnd = blockStart + ITEM_COUNT_PER_BLOCK;
        if (subBlockNumber == mBlockCount - 1)
        {
            blockEnd = (DictionaryItem*)(mBlockIndex);
        }
        DictionaryCompare comp;
        DictionaryItem* dictItem = std::lower_bound(blockStart, blockEnd, key, comp);
        if (dictItem == blockEnd || dictItem->first != key)
        {
            return false;
        }
        
        value = dictItem->second;
        return true;
    }
    return false;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INTEGRATE_TIERED_DICTIONARY_READER_H
