#ifndef __INDEXLIB_DICTIONARY_READER_H
#define __INDEXLIB_DICTIONARY_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_iterator.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/util_define.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/file_reader.h"
#include <future_lite/Future.h>
#include <utility>

IE_NAMESPACE_BEGIN(index);

class DictionaryReader
{
public:
    typedef std::tr1::shared_ptr<DictionaryIterator> DictionaryIteratorPtr;

    using LookupResult = std::pair<bool, dictvalue_t>;

public:
    DictionaryReader();
    virtual ~DictionaryReader();

public:
    /* 
     * Open the dictionary file with fileName
     */
    virtual void Open(const file_system::DirectoryPtr& directory, 
                      const std::string& fileName) = 0;
    /*
     * lookup a key in the dictionary
     * return true if lookup successfully, and return the responding value
     * return false if lookup failed
     */
    virtual bool Lookup(dictkey_t key, dictvalue_t& value) = 0;
    virtual future_lite::Future<LookupResult> LookupAsync(dictkey_t key, file_system::ReadOption option)
    {
        try
        {
            dictvalue_t value;
            auto ret = Lookup(key, value);
            return future_lite::makeReadyFuture(std::make_pair(ret, value));
        }
        catch (...)
        {
            return future_lite::makeReadyFuture<LookupResult>(std::current_exception());
        }
    }

    virtual DictionaryIteratorPtr CreateIterator() const = 0;

protected:
    template <typename KeyType>
    void ReadBlockMeta(const file_system::FileReaderPtr& fileReader,
                       size_t blockUnitSize,
                       uint32_t& blockCount, size_t& dictDataLen)
    {
        assert(fileReader);
        size_t fileLength = (size_t)fileReader->GetLength();
        if (fileLength < 2 * sizeof(uint32_t))
        {
            INDEXLIB_FATAL_ERROR(FileIO, "Bad dictionary file, fileLength[%zd]"
                    " less than 2*sizeof(uint32_t), filePath is: %s",
                    fileLength, fileReader->GetPath().c_str());
        }

        uint32_t tempBuffer[2] = {0};
        size_t cursor = fileLength - sizeof(tempBuffer);
        size_t ret = fileReader->Read((void*)tempBuffer, sizeof(tempBuffer), cursor);
        if (ret != sizeof(tempBuffer) || tempBuffer[1] != DICTIONARY_MAGIC_NUMBER)
        {
            INDEXLIB_FATAL_ERROR(FileIO, "Bad dictionary file, magic number"
                    " doesn't match and the file name is: %s",
                    fileReader->GetPath().c_str());
        }
        blockCount = tempBuffer[0];
        dictDataLen = fileLength - blockUnitSize * blockCount - sizeof(tempBuffer);
    }
private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<DictionaryReader> DictionaryReaderPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DICTIONARY_READER_H
