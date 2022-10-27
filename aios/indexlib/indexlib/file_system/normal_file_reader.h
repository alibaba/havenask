#ifndef __INDEXLIB_NORMAL_FILE_READER_H
#define __INDEXLIB_NORMAL_FILE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/file_node.h"

DECLARE_REFERENCE_CLASS(file_system, FileNode);
IE_NAMESPACE_BEGIN(file_system);

class NormalFileReader : public FileReader
{
public:
    NormalFileReader(const FileNodePtr& fileNode);
    ~NormalFileReader();

public:
    void Open() override;
    size_t Read(void* buffer, size_t length, size_t offset, ReadOption option = ReadOption()) override;
    future_lite::Future<size_t> ReadAsync(
            void* buffer, size_t length, size_t offset, ReadOption option) override;
    size_t Read(void* buffer, size_t length, ReadOption option = ReadOption()) override;
    util::ByteSliceList* Read(size_t length, size_t offset, ReadOption option = ReadOption()) override;

    void* GetBaseAddress() const override;
    size_t GetLength() const override;
    const std::string& GetPath() const override;
    FSOpenType GetOpenType() override;
    void Close() override;
    FileNodePtr GetFileNode() const override
    { return mFileNode; }
    void Lock(size_t offset, size_t length) override;
    size_t Prefetch(size_t length, size_t offset, ReadOption option) override;

    future_lite::Future<uint32_t> ReadVUInt32Async(
        size_t offset, ReadOption option) override;

    future_lite::Future<uint32_t> ReadUInt32Async(size_t offset, ReadOption option) override
    {
        return mFileNode->ReadUInt32Async(offset, option);
    }

private:
    FileNodePtr mFileNode;
    uint64_t mCursor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NormalFileReader);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_NORMAL_FILE_READER_H
