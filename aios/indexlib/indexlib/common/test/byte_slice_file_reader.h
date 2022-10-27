#ifndef __INDEXLIB_BYTE_SLICE_FILE_READER_H
#define __INDEXLIB_BYTE_SLICE_FILE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/buffered_file_reader.h"

IE_NAMESPACE_BEGIN(common);

class ByteSliceFileReader : public ByteSliceReader
{
private:
    using ByteSliceReader::Open;
public:
    const static size_t DEFAULT_BUFFER_SIZE = 128 * 1024; //128k

public:
    ByteSliceFileReader();
    ~ByteSliceFileReader();

public:
    void Open(file_system::BufferedFileReader* fileReader, size_t fileLength);
    void ReOpen(file_system::BufferedFileReader* fileReader, size_t fileLength);

    void SetBufferSize(size_t bufSize);

public:
    void Close() override;
    uint32_t GetSize() const override;

protected:
    util::ByteSlice* ReadNextSlice(util::ByteSlice* byteSlice) override;
    util::ByteSlice* PeekNextSlice(util::ByteSlice* byteSlice) override;

private:
    util::ByteSlice* SwitchToPeekSlice();

    inline void ReadSlice(util::ByteSlice* slice);

private:
    file_system::BufferedFileReader* mFileReader;
    size_t mSliceListSize;
    size_t mReadCursor;

    size_t mBufferSize;

    util::ByteSlice* mByteSlice;
    util::ByteSlice* mPeekSlice;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ByteSliceFileReader);

/////////////////////////////////////////////
inline void ByteSliceFileReader::ReadSlice(util::ByteSlice* slice)
{
    ssize_t readLen = (mSliceListSize - mReadCursor) < mBufferSize ?
                     (mSliceListSize - mReadCursor) : mBufferSize;
    if (mFileReader->Read((char*)slice->data, readLen) != readLen)
    {
        INDEXLIB_FATAL_ERROR(FileIO, "File IO error: [%s]",
                             storage::FileSystemWrapper::GetErrorString(
                                     mFileReader->GetLastError()).c_str());
    }
    slice->size = readLen;
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_BYTE_SLICE_FILE_READER_H
