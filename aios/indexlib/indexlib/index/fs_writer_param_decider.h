#ifndef __INDEXLIB_FS_WRITER_PARAM_DECIDER_H
#define __INDEXLIB_FS_WRITER_PARAM_DECIDER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/file_system_define.h"

IE_NAMESPACE_BEGIN(index);

class FSWriterParamDecider
{
public:
    FSWriterParamDecider() {}
    virtual ~FSWriterParamDecider() {}
    
public:
    virtual file_system::FSWriterParam MakeParam(
        const std::string& fileName) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FSWriterParamDecider);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_FS_WRITER_PARAM_DECIDER_H
