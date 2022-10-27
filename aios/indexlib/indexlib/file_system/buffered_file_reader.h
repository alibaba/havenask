#ifndef __INDEXLIB_FS_BUFFERED_FILE_READER_H
#define __INDEXLIB_FS_BUFFERED_FILE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/file_system/file_reader.h"

DECLARE_REFERENCE_CLASS(file_system, FileNode);
DECLARE_REFERENCE_CLASS(storage, FileBuffer);
DECLARE_REFERENCE_CLASS(util, ThreadPool);

IE_NAMESPACE_BEGIN(file_system);

class BufferedFileReader : public FileReader
{
public:
    BufferedFileReader(const FileNodePtr& fileNode, 
                       uint32_t bufferSize = FSReaderParam::DEFAULT_BUFFER_SIZE);
    ~BufferedFileReader();

public:
    // Independent from the file_system
    BufferedFileReader(uint32_t bufferSize = FSReaderParam::DEFAULT_BUFFER_SIZE,
                       bool asyncRead = false);
    void Open(const std::string& filePath);

public:
    void Open() override { }
    // The Read with offset should modify mOffset
    // The result of Read without offset will be influenced
    // They can not be used interchangeably
    size_t Read(void* buffer, size_t length, size_t offset, ReadOption option = ReadOption()) override;
    size_t Read(void* buffer, size_t length, ReadOption option = ReadOption()) override;
    util::ByteSliceList* Read(size_t length, size_t offset, ReadOption option = ReadOption()) override;
    void* GetBaseAddress() const override;
    size_t GetLength() const override;
    const std::string& GetPath() const override;
    FSOpenType GetOpenType() override;
    void Close() override;
    FileNodePtr GetFileNode() const override
    { return mFileNode; }

public:
    void Seek(int64_t offset)
    {
        if (offset > mFileLength)
        {
            INDEXLIB_FATAL_ERROR(OutOfRange, "seek file[%s] out of range, "
                    "offset: [%lu], file length: [%lu]",
                    GetPath().c_str(), offset, mFileLength);
        }
        mOffset = offset;
    }

    int64_t Tell() const { return mOffset; }

    void ResetBufferParam(size_t bufferSize, bool asyncRead);

private:
    virtual void LoadBuffer(int64_t blockNum, ReadOption option);

    void SyncLoadBuffer(int64_t blockNum, ReadOption option);
    void AsyncLoadBuffer(int64_t blockNum, ReadOption option);
    void PrefetchBlock(int64_t blockNum, ReadOption option);
    void IOCtlPrefetch(size_t blockSize);

public:
    // public for test
    virtual size_t DoRead(void* buffer, size_t length, size_t offset, ReadOption option);

private:
    FileNodePtr mFileNode;
    int64_t mFileLength;
    int64_t mOffset;
    int64_t mCurBlock;
    storage::FileBuffer *mBuffer;
    storage::FileBuffer *mSwitchBuffer;
    util::ThreadPoolPtr mThreadPool;

private:
    IE_LOG_DECLARE();
    friend class MockBufferedFileReader;
};

DEFINE_SHARED_PTR(BufferedFileReader);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FS_BUFFERED_FILE_READER_H
