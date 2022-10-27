#include "indexlib/common/test/byte_slice_file_reader.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, ByteSliceFileReader);

ByteSliceFileReader::ByteSliceFileReader() 
    : mFileReader(NULL)
    , mSliceListSize(0)
    , mReadCursor(0)
    , mBufferSize(DEFAULT_BUFFER_SIZE)
    , mByteSlice(NULL)
    , mPeekSlice(NULL)
{
}

ByteSliceFileReader::~ByteSliceFileReader() 
{
    Close();
}

void ByteSliceFileReader::Open(BufferedFileReader* fileReader,
                               size_t sliceListSize)
{
    mFileReader = fileReader;
    mSliceListSize = sliceListSize;
    mReadCursor = 0;
    if (mByteSlice)
    {
        ByteSlice::DestroyObject(mByteSlice, NULL);
    }
    mByteSlice = ByteSlice::CreateObject(mBufferSize, NULL);

    ByteSliceReader::Open(ReadNextSlice(NULL));
}

void ByteSliceFileReader::ReOpen(BufferedFileReader* fileReader, 
                                 size_t sliceListSize)
{
    mFileReader = fileReader;
    mSliceListSize = sliceListSize;
    mReadCursor = 0;
    if (!mByteSlice)
    {
        mByteSlice = ByteSlice::CreateObject(mBufferSize, NULL);
    }

    ByteSliceReader::Open(ReadNextSlice(NULL));
}

void ByteSliceFileReader::Close()
{
    if (mByteSlice)
    {
        ByteSlice::DestroyObject(mByteSlice, NULL);
        mByteSlice = NULL;
    }
    if (mPeekSlice)
    {
        ByteSlice::DestroyObject(mPeekSlice, NULL);
        mPeekSlice = NULL;
    }
}

uint32_t ByteSliceFileReader::GetSize() const
{
    return (uint32_t)mSliceListSize;
}

ByteSlice* ByteSliceFileReader::ReadNextSlice(ByteSlice* byteSlice)
{
    if (mReadCursor >= mSliceListSize)
    {
        return NULL;
    }

    if (mPeekSlice)
    {
        return SwitchToPeekSlice();
    }

    ReadSlice(mByteSlice);
    mReadCursor += mByteSlice->size;
    return mByteSlice;
}

ByteSlice* ByteSliceFileReader::PeekNextSlice(ByteSlice* byteSlice)
{
    if (mPeekSlice)
    {
        return mPeekSlice;
    }
    if (mReadCursor >= mSliceListSize)
    {
        return NULL;
    }

    mPeekSlice = ByteSlice::CreateObject(mBufferSize, NULL);
    ReadSlice(mPeekSlice);
    return mPeekSlice;
}

ByteSlice* ByteSliceFileReader::SwitchToPeekSlice()
{
    ByteSlice* tmpSlice = mByteSlice;
    mByteSlice = mPeekSlice;
    ByteSlice::DestroyObject(tmpSlice, NULL);
    mPeekSlice = NULL;

    mReadCursor += mByteSlice->size;
    return mByteSlice; 
}

void ByteSliceFileReader::SetBufferSize(size_t bufSize)
{
    mBufferSize = bufSize;
    if (mByteSlice)
    {
        ByteSlice::DestroyObject(mByteSlice, NULL);
    }
    mByteSlice = ByteSlice::CreateObject(mBufferSize, NULL);
}

IE_NAMESPACE_END(common);
