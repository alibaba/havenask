#ifndef __INDEXLIB_DEFAULT_FS_WRITER_PARAM_DECIDER_H
#define __INDEXLIB_DEFAULT_FS_WRITER_PARAM_DECIDER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/fs_writer_param_decider.h"
#include "indexlib/file_system/file_system_define.h"

IE_NAMESPACE_BEGIN(index);

class DefaultFSWriterParamDecider : public FSWriterParamDecider
{
public:
    DefaultFSWriterParamDecider() {}
    ~DefaultFSWriterParamDecider() {}
    
public:
    file_system::FSWriterParam MakeParam(
        const std::string& fileName) override
    {
        return file_system::FSWriterParam(false, false);
    }
        
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DefaultFSWriterParamDecider);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DEFAULT_FS_WRITER_PARAM_DECIDER_H
