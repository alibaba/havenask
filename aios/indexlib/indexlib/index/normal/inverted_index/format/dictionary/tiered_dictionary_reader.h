#ifndef __INDEXLIB_TIERED_DICTIONARY_READER_H
#define __INDEXLIB_TIERED_DICTIONARY_READER_H

#include <tr1/memory>
#include "fslib/fslib.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

#include "indexlib/index/normal/inverted_index/format/dictionary/integrate_tiered_dictionary_reader.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/block_tiered_dictionary_reader.h"

IE_NAMESPACE_BEGIN(index);

template <typename KeyType>
class TieredDictionaryReaderTyped : public DictionaryReader
{
public:
    typedef IntegrateTieredDictionaryReaderTyped<KeyType> IntegrateDictReader;
    typedef std::tr1::shared_ptr<IntegrateDictReader> IntegrateDictReaderPtr;

    typedef BlockTieredDictionaryReaderTyped<KeyType> BlockDictReader;
    typedef std::tr1::shared_ptr<BlockDictReader> BlockDictReaderPtr;

    typedef std::tr1::shared_ptr<DictionaryIterator> DictionaryIteratorPtr;
    typedef std::tr1::shared_ptr<TieredDictionaryIteratorTyped<KeyType>>  TieredDictionaryIteratorPtr;

public:
    TieredDictionaryReaderTyped() {}
    ~TieredDictionaryReaderTyped() {}

public:
    void Open(const file_system::DirectoryPtr& directory, 
              const std::string& fileName) override;

    bool Lookup(dictkey_t key, dictvalue_t& value) override;
    future_lite::Future<DictionaryReader::LookupResult> LookupAsync(
        dictkey_t key, file_system::ReadOption option) override;

    DictionaryIteratorPtr CreateIterator() const override;
    static size_t GetPatialLockSize(size_t fileLength) {
        return fileLength / (ITEM_COUNT_PER_BLOCK * 2);
    }
private:
    DictionaryReaderPtr mInnerDictionaryReader;
    
private:
    IE_LOG_DECLARE();
};

typedef TieredDictionaryReaderTyped<dictkey_t> TieredDictionaryReader;
typedef std::tr1::shared_ptr<TieredDictionaryReader> TieredDictionaryReaderPtr;

//////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, TieredDictionaryReaderTyped);

template <typename KeyType>
void TieredDictionaryReaderTyped<KeyType>::Open(
        const file_system::DirectoryPtr& directory,
        const std::string& fileName)
{
    file_system::FileReaderPtr fileReader =
        directory->CreateFileReader(fileName, file_system::FSOT_LOAD_CONFIG);
    uint32_t blockCount = 0;
    size_t dictDataLen = 0;
    ReadBlockMeta<KeyType>(fileReader, sizeof(KeyType), blockCount, dictDataLen);
    if (fileReader->GetBaseAddress())
    {
        IntegrateDictReaderPtr reader(new IntegrateDictReader);
        reader->Open(fileReader, blockCount, dictDataLen);
        mInnerDictionaryReader = reader;
        return;
    }
    BlockDictReaderPtr reader(new BlockDictReader);
    reader->Open(directory, fileReader, blockCount, dictDataLen);
    mInnerDictionaryReader = reader;
}

template <typename KeyType>
bool TieredDictionaryReaderTyped<KeyType>::Lookup(dictkey_t key, dictvalue_t& value)
{
    return mInnerDictionaryReader->Lookup(key, value);
}

template <typename KeyType>
future_lite::Future<DictionaryReader::LookupResult>
TieredDictionaryReaderTyped<KeyType>::LookupAsync(dictkey_t key, file_system::ReadOption option)
{
    return mInnerDictionaryReader->LookupAsync(key, option);
}

template <typename KeyType>
typename TieredDictionaryReaderTyped<KeyType>::DictionaryIteratorPtr
TieredDictionaryReaderTyped<KeyType>::CreateIterator() const
{
    return mInnerDictionaryReader->CreateIterator();
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TIERED_DICTIONARY_READER_H
