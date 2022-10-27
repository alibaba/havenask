#ifndef __INDEXLIB_COMMON_DISK_HASH_DICTIONARY_READER_H
#define __INDEXLIB_COMMON_DISK_HASH_DICTIONARY_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/hash_dictionary_reader.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/common_disk_hash_dictionary_iterator.h"

IE_NAMESPACE_BEGIN(index);

template <typename KeyType>
class CommonDiskHashDictionaryReaderTyped : public  DictionaryReader
{
public:
    typedef std::tr1::shared_ptr<DictionaryIterator> DictionaryIteratorPtr;
    typedef std::tr1::shared_ptr<CommonDiskHashDictionaryIteratorTyped<KeyType> > CommonDiskHashDictionaryIteratorPtr; 

public:
    CommonDiskHashDictionaryReaderTyped() : mDictDataLength(0) {}
    ~CommonDiskHashDictionaryReaderTyped() {}

public:
    void Open(const file_system::DirectoryPtr& directory, 
              const std::string& fileName) override;

    bool Lookup(dictkey_t key, dictvalue_t& value) override;

    DictionaryIteratorPtr CreateIterator() const override
    {
        assert(mBufferedFileReader);
        CommonDiskHashDictionaryIteratorPtr iterator(
                new CommonDiskHashDictionaryIteratorTyped<KeyType>(
                        mBufferedFileReader, mDictDataLength));
        return iterator;
    }

private:
    file_system::BufferedFileReaderPtr mBufferedFileReader;
    size_t mDictDataLength;

private:
    IE_LOG_DECLARE();
};

typedef CommonDiskHashDictionaryReaderTyped<dictkey_t> CommonDiskHashDictionaryReader;
typedef std::tr1::shared_ptr<CommonDiskHashDictionaryReader> CommonDiskHashDictionaryReaderPtr;

//////////////////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, CommonDiskHashDictionaryReaderTyped);

template <typename KeyType>
void CommonDiskHashDictionaryReaderTyped<KeyType>::Open(
        const file_system::DirectoryPtr& directory, 
        const std::string& fileName)
{
    file_system::FileReaderPtr fileReader = 
        directory->CreateFileReader(fileName, file_system::FSOT_BUFFERED);
    
    uint32_t blockCount = 0;
    ReadBlockMeta<KeyType>(fileReader, sizeof(uint32_t), blockCount, mDictDataLength);
    assert(mDictDataLength >= 0);

    mBufferedFileReader = DYNAMIC_POINTER_CAST(
            file_system::BufferedFileReader, fileReader);
    assert(mBufferedFileReader);
    mBufferedFileReader->Seek(0);
}

template <typename KeyType>
bool CommonDiskHashDictionaryReaderTyped<KeyType>::Lookup(dictkey_t key, dictvalue_t& value)
{
    INDEXLIB_FATAL_ERROR(UnSupported, 
                         "Lookup not supported for CommonDiskHashDictionaryReader");
    return false;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_COMMON_DISK_HASH_DICTIONARY_READER_H
