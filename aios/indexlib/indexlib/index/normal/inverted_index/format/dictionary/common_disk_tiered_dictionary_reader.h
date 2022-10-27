#ifndef __INDEXLIB_COMMON_DISK_TIERED_DICTIONARY_READER_H
#define __INDEXLIB_COMMON_DISK_TIERED_DICTIONARY_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_reader.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/common_disk_tiered_dictionary_iterator.h"

IE_NAMESPACE_BEGIN(index);

template <typename KeyType>
class CommonDiskTieredDictionaryReaderTyped : public  DictionaryReader
{
public:
    typedef std::tr1::shared_ptr<DictionaryIterator> DictionaryIteratorPtr;
    typedef std::tr1::shared_ptr<CommonDiskTieredDictionaryIteratorTyped<KeyType> > CommonDiskTieredDictionaryIteratorPtr; 

public:
    CommonDiskTieredDictionaryReaderTyped() : mDictDataLength(0) {}
    ~CommonDiskTieredDictionaryReaderTyped() {}

public:
    void Open(const file_system::DirectoryPtr& directory, 
                             const std::string& fileName) override;

    bool Lookup(dictkey_t key, dictvalue_t& value) override;

    DictionaryIteratorPtr CreateIterator() const override
    {
        assert(mBufferedFileReader);
        CommonDiskTieredDictionaryIteratorPtr iterator(
                new CommonDiskTieredDictionaryIteratorTyped<KeyType>(
                        mBufferedFileReader, mDictDataLength));
        return iterator;
    }

private:
    file_system::BufferedFileReaderPtr mBufferedFileReader;
    size_t mDictDataLength;

private:
    IE_LOG_DECLARE();
};

typedef CommonDiskTieredDictionaryReaderTyped<dictkey_t> CommonDiskTieredDictionaryReader;
typedef std::tr1::shared_ptr<CommonDiskTieredDictionaryReader> CommonDiskTieredDictionaryReaderPtr;

//////////////////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, CommonDiskTieredDictionaryReaderTyped);

template <typename KeyType>
void CommonDiskTieredDictionaryReaderTyped<KeyType>::Open(
        const file_system::DirectoryPtr& directory, 
        const std::string& fileName)
{
    file_system::FileReaderPtr fileReader = 
        directory->CreateFileReader(fileName, file_system::FSOT_BUFFERED);
    
    uint32_t blockCount = 0;
    ReadBlockMeta<KeyType>(fileReader, sizeof(KeyType), blockCount, mDictDataLength);
    assert(mDictDataLength >= 0);

    mBufferedFileReader = DYNAMIC_POINTER_CAST(
            file_system::BufferedFileReader, fileReader);
    assert(mBufferedFileReader);
    mBufferedFileReader->Seek(0);
}

template <typename KeyType>
bool CommonDiskTieredDictionaryReaderTyped<KeyType>::Lookup(dictkey_t key, dictvalue_t& value)
{
    INDEXLIB_FATAL_ERROR(UnSupported, 
                         "Lookup not supported for CommonDiskTieredDictionaryReader");
    return false;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_COMMON_DISK_TIERED_DICTIONARY_READER_H
