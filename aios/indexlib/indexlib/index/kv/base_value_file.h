#ifndef __INDEXLIB_BASE_VALUE_FILE_H
#define __INDEXLIB_BASE_VALUE_FILE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index);

class BaseValueFile
{
public:
    BaseValueFile() {}
    virtual ~BaseValueFile() {}
public:
    virtual void *GetBaseAddress() = 0;
    virtual const std::string& GetPath() const = 0;
    virtual void MakeAutoClean() = 0;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BaseValueFile);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BASE_VALUE_FILE_H
