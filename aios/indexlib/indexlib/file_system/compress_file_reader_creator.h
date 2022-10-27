#ifndef __INDEXLIB_COMPRESS_FILE_READER_CREATOR_H
#define __INDEXLIB_COMPRESS_FILE_READER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/compress_file_reader.h"
#include "indexlib/file_system/compress_file_info.h"

IE_NAMESPACE_BEGIN(file_system);

class CompressFileReaderCreator
{
public:
    CompressFileReaderCreator();
    ~CompressFileReaderCreator();
    
public:
    static CompressFileReaderPtr Create(const FileReaderPtr& fileReader,
            const CompressFileInfo& compressInfo, Directory* directory);

private:
    static bool NeedCacheDecompressFile(const FileReaderPtr& fileReader,
            const CompressFileInfo& compressInfo);
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CompressFileReaderCreator);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_COMPRESS_FILE_READER_CREATOR_H
