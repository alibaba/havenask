#ifndef __INDEXLIB_NORMAL_COMPRESS_FILE_READER_H
#define __INDEXLIB_NORMAL_COMPRESS_FILE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/compress_file_reader.h"

IE_NAMESPACE_BEGIN(file_system);

class NormalCompressFileReader : public CompressFileReader
{
public:
    NormalCompressFileReader(autil::mem_pool::Pool* pool = NULL)
        : CompressFileReader(pool)
    {}
       
    ~NormalCompressFileReader() {}
    
public:
    NormalCompressFileReader* CreateSessionReader(
            autil::mem_pool::Pool* pool) override;
    
private:
    void LoadBuffer(size_t offset, ReadOption option) override;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NormalCompressFileReader);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_NORMAL_COMPRESS_FILE_READER_H
