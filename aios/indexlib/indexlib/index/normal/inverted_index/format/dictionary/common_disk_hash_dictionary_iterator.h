#ifndef __INDEXLIB_COMMON_DISK_HASH_DICTIONARY_ITERATOR_H
#define __INDEXLIB_COMMON_DISK_HASH_DICTIONARY_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_iterator.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/util_define.h"
#include "indexlib/file_system/buffered_file_reader.h"

IE_NAMESPACE_BEGIN(index);

template <typename KeyType>
class CommonDiskHashDictionaryIteratorTyped : public DictionaryIterator
{
public:
    CommonDiskHashDictionaryIteratorTyped(
            file_system::BufferedFileReaderPtr bufferedFileReader,
            size_t dictDataLength);

    ~CommonDiskHashDictionaryIteratorTyped() {}

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

typedef CommonDiskHashDictionaryIteratorTyped<dictkey_t> CommonDiskHashDictionaryIterator;
typedef std::tr1::shared_ptr<CommonDiskHashDictionaryIterator> CommonDiskHashDictionaryIteratorPtr;

///////////////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, CommonDiskHashDictionaryIteratorTyped);

template <typename KeyType>
CommonDiskHashDictionaryIteratorTyped<KeyType>::CommonDiskHashDictionaryIteratorTyped(
        file_system::BufferedFileReaderPtr bufferedFileReader,
        size_t dictDataLength)
    : mBufferedFileReader(bufferedFileReader)
    , mDictDataLength(dictDataLength)
{
    mBufferedFileReader->Seek(0);
}

template <typename KeyType>
bool CommonDiskHashDictionaryIteratorTyped<KeyType>::HasNext() const
{
    return ((size_t)mBufferedFileReader->Tell() + 1 < mDictDataLength);
}

template <typename KeyType>
void CommonDiskHashDictionaryIteratorTyped<KeyType>::Next(dictkey_t& key, dictvalue_t& value)
{
    KeyType typedKey;
    DoNext(typedKey, value);
    key = (dictkey_t)typedKey;
}

template <typename KeyType>
void CommonDiskHashDictionaryIteratorTyped<KeyType>::DoNext(KeyType& key, dictvalue_t& value)
{
    HashDictionaryItemTyped<KeyType> item;
    size_t len = mBufferedFileReader->Read((void*)&item, sizeof(item));
    if (len < sizeof(item))
    {
        INDEXLIB_FATAL_ERROR(FileIO, 
                             "read DictItem from file[%s] FAILED!",
                             mBufferedFileReader->GetPath().c_str());
    }

    key = item.key;
    value = item.value;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_COMMON_DISK_HASH_DICTIONARY_ITERATOR_H
