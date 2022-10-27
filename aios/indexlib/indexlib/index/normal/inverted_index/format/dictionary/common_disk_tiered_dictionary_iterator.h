#ifndef __INDEXLIB_COMMON_DISK_TIERED_DICTIONARY_ITERATOR_H
#define __INDEXLIB_COMMON_DISK_TIERED_DICTIONARY_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_iterator.h"
#include "indexlib/file_system/buffered_file_reader.h"

IE_NAMESPACE_BEGIN(index);

template <typename KeyType>
class CommonDiskTieredDictionaryIteratorTyped : public DictionaryIterator
{
public:
    CommonDiskTieredDictionaryIteratorTyped(
            file_system::BufferedFileReaderPtr bufferedFileReader,
            size_t dictDataLength);

    ~CommonDiskTieredDictionaryIteratorTyped() {}

public:
    bool HasNext() const override;
    void Next(dictkey_t& key, dictvalue_t& value) override;

private:
    void DoNext(KeyType& key, dictvalue_t& value);

private:
    file_system::BufferedFileReaderPtr mBufferedFileReader;
    size_t mDictDataLength;

private:
    IE_LOG_DECLARE();
};

typedef CommonDiskTieredDictionaryIteratorTyped<dictkey_t> CommonDiskTieredDictionaryIterator;
typedef std::tr1::shared_ptr<CommonDiskTieredDictionaryIterator> CommonDiskTieredDictionaryIteratorPtr;

///////////////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, CommonDiskTieredDictionaryIteratorTyped);

template <typename KeyType>
CommonDiskTieredDictionaryIteratorTyped<KeyType>::CommonDiskTieredDictionaryIteratorTyped(
        file_system::BufferedFileReaderPtr bufferedFileReader,
        size_t dictDataLength)
    : mBufferedFileReader(bufferedFileReader)
    , mDictDataLength(dictDataLength)
{
    mBufferedFileReader->Seek(0);
}

template <typename KeyType>
bool CommonDiskTieredDictionaryIteratorTyped<KeyType>::HasNext() const
{
    return ((size_t)mBufferedFileReader->Tell() + 1 < mDictDataLength);
}

template <typename KeyType>
void CommonDiskTieredDictionaryIteratorTyped<KeyType>::Next(dictkey_t& key, dictvalue_t& value)
{
    KeyType typedKey;
    DoNext(typedKey, value);
    key = (dictkey_t)typedKey;
}

template <typename KeyType>
void CommonDiskTieredDictionaryIteratorTyped<KeyType>::DoNext(KeyType& key, dictvalue_t& value)
{
    size_t rKeyLen = mBufferedFileReader->Read((void*)&key, sizeof(KeyType));
    size_t rValueLen = mBufferedFileReader->Read((void*)&value, sizeof(dictvalue_t));
    if (rKeyLen < sizeof(KeyType) || rValueLen < sizeof(dictvalue_t))
    {
        INDEXLIB_FATAL_ERROR(FileIO, 
                             "read DictItem from file[%s] FAILED!",
                             mBufferedFileReader->GetPath().c_str());
    }
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_COMMON_DISK_TIERED_DICTIONARY_ITERATOR_H
