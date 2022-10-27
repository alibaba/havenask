#ifndef __INDEXLIB_FS_FILE_READER_H
#define __INDEXLIB_FS_FILE_READER_H

#include <tr1/memory>
#include <future_lite/Future.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/file_system/read_option.h"

DECLARE_REFERENCE_CLASS(util, ByteSliceList);
DECLARE_REFERENCE_CLASS(file_system, FileNode);

IE_NAMESPACE_BEGIN(file_system);

class FileReader
{
public:
    FileReader();
    virtual ~FileReader();

public:
    virtual void Open() = 0;
    virtual size_t Read(void* buffer, size_t length, size_t offset, ReadOption option = ReadOption()) = 0;
    virtual future_lite::Future<size_t> ReadAsync(
        void* buffer, size_t length, size_t offset, ReadOption option)
    {
        return future_lite::makeReadyFuture(Read(buffer, length, offset, option));
    }
    virtual size_t Read(void* buffer, size_t length, ReadOption option = ReadOption()) = 0;
    virtual util::ByteSliceList* Read(size_t length, size_t offset, ReadOption option = ReadOption()) = 0;

    virtual future_lite::Future<uint32_t> ReadUInt32Async(size_t offset, ReadOption option)
    {
        uint32_t buffer;
        auto readSize = Read(static_cast<void*>(&buffer), sizeof(buffer), offset, option);
        assert(readSize == sizeof(buffer));
        (void)readSize;
        return future_lite::makeReadyFuture<uint32_t>(buffer);
    }

    virtual void* GetBaseAddress() const = 0;
    virtual size_t GetLength() const = 0;
    virtual const std::string& GetPath() const = 0;
    virtual void Close() = 0;
    virtual FSOpenType GetOpenType() = 0;
    virtual void Lock(size_t offset, size_t length) { }
    virtual size_t Prefetch(size_t length, size_t offset, ReadOption option = ReadOption()) { return 0; }

public:
    inline uint32_t ReadVUInt32(ReadOption option = ReadOption())
    {
        uint8_t byte;
        Read((void*)&byte, sizeof(byte), option);
        uint32_t value = byte & 0x7F;
        int shift = 7;
    
        while(byte & 0x80)
        {
            Read((void*)&byte, sizeof(byte), option);
            value |= ((byte & 0x7F) << shift);
            shift += 7;
        }
        return value;
    }

    virtual future_lite::Future<uint32_t> ReadVUInt32Async(ReadOption option)
    {
        return future_lite::makeReadyFuture(ReadVUInt32());
    }

    inline uint32_t ReadVUInt32(size_t offset, ReadOption option = ReadOption())
    {
        uint8_t byte;
        Read((void*)&byte, sizeof(byte), offset++, option);
        uint32_t value = byte & 0x7F;
        int shift = 7;
    
        while(byte & 0x80)
        {
            Read((void*)&byte, sizeof(byte), offset++, option);
            value |= ((byte & 0x7F) << shift);
            shift += 7;
        }
        return value;
    }

    virtual future_lite::Future<uint32_t> ReadVUInt32Async(size_t offset, ReadOption option)
    {
        return future_lite::makeReadyFuture(ReadVUInt32(offset));
    }

public:
    //for test
    virtual FileNodePtr GetFileNode() const = 0;

private:
    friend class LocalStorageTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FileReader);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FS_FILE_READER_H
