#ifndef __INDEXLIB_DICTIONARY_WRITER_H
#define __INDEXLIB_DICTIONARY_WRITER_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/file_system/directory.h"
#include <tr1/memory>

IE_NAMESPACE_BEGIN(index);

class DictionaryWriter
{
public:
    DictionaryWriter() {}
    virtual ~DictionaryWriter() {}

public:
    // virtual void Open(const std::string& path) = 0;
    virtual void Open(const file_system::DirectoryPtr& directory,
                      const std::string& fileName) = 0;

    virtual void Open(const file_system::DirectoryPtr& directory,
                      const std::string& fileName, size_t itemCount) = 0;
    
    virtual void AddItem(dictkey_t key, dictvalue_t offset) = 0;
    virtual void Close() = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DictionaryWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DICTIONARY_WRITER_H
