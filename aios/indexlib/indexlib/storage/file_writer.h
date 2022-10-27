#ifndef __INDEXLIB_FILE_WRITER_H
#define __INDEXLIB_FILE_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include <unistd.h>

IE_NAMESPACE_BEGIN(storage);

class FileWriter
{
public:
    FileWriter() {}
    virtual ~FileWriter() {}
public:
    virtual void Open(const std::string& fileName) = 0;

    virtual ssize_t Write(const void* src, size_t lenToWrite) = 0;

    virtual int64_t Tell() const = 0;
    virtual void Close() = 0;

    void WriteVUInt32(uint32_t value);

private:
    IE_LOG_DECLARE();
};

inline void FileWriter::WriteVUInt32(uint32_t value)
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

DEFINE_SHARED_PTR(FileWriter);

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_FILE_WRITER_H
