#ifndef __INDEXLIB_BUFFERED_FILE_WRAPPER_H
#define __INDEXLIB_BUFFERED_FILE_WRAPPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/storage/file_wrapper.h"

IE_NAMESPACE_BEGIN(storage);

class BufferedFileWrapper : public FileWrapper
{
public:
    BufferedFileWrapper(fslib::fs::File *file, fslib::Flag mode, 
                        int64_t fileLength, uint32_t bufferSize);
    virtual ~BufferedFileWrapper();
public:
    size_t Read(void* buffer, size_t length) override;
    size_t PRead(void* buffer, size_t length, off_t offset) override;
    void Write(const void* buffer, size_t length) override;
    void Seek(int64_t offset, fslib::SeekFlag flag) override;
    int64_t Tell() override { return mOffset; }
    void Flush() override;
    void Close() override;
public:
    void SetOffset(int64_t offset) 
    { 
        assert(offset <= mFileLength);
        mOffset = offset; 
    }
    int64_t GetOffset() const { return mOffset; }
    int64_t GetFileLength() const { return mFileLength; }
private:
    virtual void LoadBuffer(int64_t blockNum);
    virtual void DumpBuffer();
protected:
    fslib::Flag mMode;
    int64_t mFileLength;
    int64_t mOffset;
    int64_t mCurBlock;
    FileBuffer *mBuffer;
private:
    friend class MockBufferedFileWrapper;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BufferedFileWrapper);

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_BUFFERED_FILE_WRAPPER_H
