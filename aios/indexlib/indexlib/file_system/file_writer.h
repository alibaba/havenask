#ifndef __INDEXLIB_FS_FILE_WRITER_H
#define __INDEXLIB_FS_FILE_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_system_define.h"

IE_NAMESPACE_BEGIN(file_system);

class FileWriter
{
public:
    FileWriter(const FSWriterParam& param = FSWriterParam())
        : mWriterParam(param) {}
    
    virtual ~FileWriter() {}

public:
    virtual void Open(const std::string& path) = 0;
    virtual size_t Write(const void* buffer, size_t length) = 0;
    virtual size_t GetLength() const = 0;
    virtual const std::string& GetPath() const = 0;
    virtual void Close() = 0;
    virtual void ReserveFileNode(size_t reserveSize) {}

public:
    FSWriterParam GetWriterParam() const { return mWriterParam; }
    void WriteVUInt32(uint32_t value)
    {
        char buf[5];
        int cursor = 0;
        while (value > 0x7F)
        {
            buf[cursor++] = 0x80 | (value & 0x7F);
            value >>= 7;
        }
        buf[cursor++] = value & 0x7F;
        Write((const void*)buf, cursor);
    }
    
protected:
    FSWriterParam mWriterParam;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FileWriter);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FS_FILE_WRITER_H
