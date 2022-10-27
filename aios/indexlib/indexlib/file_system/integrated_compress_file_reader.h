#ifndef __INDEXLIB_INTEGRATED_COMPRESS_FILE_READER_H
#define __INDEXLIB_INTEGRATED_COMPRESS_FILE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/compress_file_reader.h"

IE_NAMESPACE_BEGIN(file_system);

class IntegratedCompressFileReader : public CompressFileReader
{
public:
    IntegratedCompressFileReader(autil::mem_pool::Pool* pool = NULL)
        : CompressFileReader(pool)
        , mDataAddr(NULL)
    {}
    
    ~IntegratedCompressFileReader() {}
    
public:
    bool Init(const FileReaderPtr& fileReader,
              const CompressFileInfo& compressInfo,
              Directory* directory) override;

    IntegratedCompressFileReader* CreateSessionReader(
            autil::mem_pool::Pool* pool) override;

    void Close() override
    {
        CompressFileReader::Close();
        mDataAddr = NULL;
    };

private:
    void LoadBuffer(size_t offset, ReadOption option) override;

private:
    char* mDataAddr;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IntegratedCompressFileReader);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_INTEGRATED_COMPRESS_FILE_READER_H
