#ifndef __INDEXLIB_IO_EXCEPTION_CONTROLLER_H
#define __INDEXLIB_IO_EXCEPTION_CONTROLLER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(misc);

class IoExceptionController
{
public:
    IoExceptionController();
    ~IoExceptionController();

public:
    static bool HasFileIOException() { return mHasFileIOException; }
    static void SetFileIOExceptionStatus(bool flag) { mHasFileIOException = flag; } 
    
private:
    static volatile bool mHasFileIOException;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IoExceptionController);

IE_NAMESPACE_END(misc);

#endif //__INDEXLIB_IO_EXCEPTION_CONTROLLER_H
