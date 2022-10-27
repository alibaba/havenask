#ifndef __INDEXLIB_HASH_TABLE_VALUE_FILE_H
#define __INDEXLIB_HASH_TABLE_VALUE_FILE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/kv/base_value_file.h"

IE_NAMESPACE_BEGIN(index);

template<typename T>
class HashTableValueFile : public BaseValueFile
{
public:
    typedef typename std::tr1::shared_ptr<T> ValueFilePtr;
    HashTableValueFile(const ValueFilePtr& valueFile) {
        mValueFile = valueFile;
    }
    ~HashTableValueFile() {}
public:
    void *GetBaseAddress() override
    {
        return mValueFile->GetBaseAddress();
    }
    
    const std::string& GetPath() const override
    {
        return mValueFile->GetPath();
    }
    
    void MakeAutoClean() override
    {
        mValueFile->SetDirty(false);
    }
    
private:
    ValueFilePtr mValueFile;
private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_HASH_TABLE_VALUE_FILE_H
