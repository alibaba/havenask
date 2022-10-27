#include "indexlib/common/in_mem_file_writer.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"

using namespace std;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, InMemFileWriter);

InMemFileWriter::InMemFileWriter() 
    : mBuf(NULL)
    , mLength(0)
    , mCursor(0)
{
}

InMemFileWriter::~InMemFileWriter() 
{
    if (mBuf != NULL)
    {
        delete [] mBuf;
        mLength = 0;
        mCursor = 0;
    }
}

void InMemFileWriter::Init(uint64_t length)
{
    mLength = length;
    mCursor = 0;
    if (mBuf != NULL)
    {
        delete [] mBuf;
    }
    mBuf = new uint8_t[mLength];
}

size_t InMemFileWriter::CalculateSize(size_t needLength)
{
    size_t size = mLength;
    while (size < needLength)
    {
        size *= 2;
    }
    return size;
}

size_t InMemFileWriter::Write(const void* buffer, size_t length) 
{ 
    assert(buffer != NULL);
    if (length + mCursor > mLength)
    {
        Extend(CalculateSize(length + mCursor));
    }
    memcpy(mBuf + mCursor, buffer, length);
    mCursor += length;
    return length;
}

void InMemFileWriter::Extend(size_t extendSize)
{
    assert(extendSize > mLength);
    uint8_t* dstBuffer = new uint8_t[extendSize];
    memcpy(dstBuffer, mBuf, mCursor);
    uint8_t* tmpBuffer = mBuf;
    mBuf = dstBuffer;
    mLength = extendSize;
    delete [] tmpBuffer;
}

ByteSliceListPtr InMemFileWriter::GetByteSliceList(bool isVIntHeader) const 
{
    ByteSlice* slice = ByteSlice::CreateObject(0);
    if (isVIntHeader)
    {
        char* cursor = (char*)mBuf;
        uint32_t len = VByteCompressor::ReadVUInt32(cursor);
        (void)len;
        slice->size = mLength - (cursor - (char*)mBuf);
        slice->data = (uint8_t*)cursor;
    }
    else
    {
        slice->size = mLength - sizeof(uint32_t);
        slice->dataSize = slice->size;        
        slice->data = mBuf + sizeof(uint32_t);
    }
    ByteSliceList* sliceList = new ByteSliceList(slice);
    return ByteSliceListPtr(sliceList);
}

ByteSliceListPtr InMemFileWriter::CopyToByteSliceList(bool isVIntHeader)
{
    ByteSlice* slice = NULL;
    if (isVIntHeader)
    {
        char* cursor = (char*)mBuf;
        uint32_t len = VByteCompressor::ReadVUInt32(cursor);
        (void)len;
        uint32_t headerLen = cursor - (char*)mBuf;
        slice = ByteSlice::CreateObject(mLength - headerLen);
        memcpy(slice->data, cursor, mLength - headerLen);
    }
    else
    {
        slice = ByteSlice::CreateObject(mLength - sizeof(uint32_t));
        memcpy(slice->data, mBuf + sizeof(uint32_t), mLength - sizeof(uint32_t));
    }

    mLength = 0;
    mCursor = 0;
    ByteSliceList* sliceList = new ByteSliceList(slice);
    return ByteSliceListPtr(sliceList);
}

const string& InMemFileWriter::GetPath() const 
{
    assert(false);
    static const string emptyString;
    return emptyString;
}

IE_NAMESPACE_END(common);

