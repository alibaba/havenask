#ifndef __INDEXLIB_COMPRESS_FILE_INFO_H
#define __INDEXLIB_COMPRESS_FILE_INFO_H

#include <tr1/memory>
#include "autil/legacy/jsonizable.h"
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(file_system);

class CompressFileInfo : public autil::legacy::Jsonizable
{
public:
    CompressFileInfo()
        : blockCount(0)
        , blockSize(0)
        , compressFileLen(0)
        , deCompressFileLen(0)
    {}

    ~CompressFileInfo() {}
    
public:
    void FromString(const std::string& str)
    {
        FromJsonString(*this, str);
    }
    
    std::string ToString() const
    {
        return ToJsonString(*this);
    }
    
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
    {
        json.Jsonize("compressor",  compressorName, compressorName);
        json.Jsonize("block_count",  blockCount, blockCount);
        json.Jsonize("block_size",  blockSize, blockSize);
        json.Jsonize("compress_file_length",  compressFileLen, compressFileLen);
        json.Jsonize("decompress_file_length",  deCompressFileLen, deCompressFileLen);
    }
    
public:
    std::string compressorName;
    size_t blockCount;
    size_t blockSize;
    size_t compressFileLen;
    size_t deCompressFileLen;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CompressFileInfo);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_COMPRESS_FILE_INFO_H
