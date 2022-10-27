#include "indexlib/file_system/slice_file_reader.h"

using namespace std;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, SliceFileReader);

SliceFileReader::SliceFileReader(const SliceFileNodePtr& sliceFileNode)
    : mSliceFileNode(sliceFileNode) 
    , mOffset(0)
{
}

SliceFileReader::~SliceFileReader() 
{
    Close();
}

size_t SliceFileReader::Read(void* buffer, size_t length, size_t offset, ReadOption option)
{
    return mSliceFileNode->Read(buffer, length, offset);
}

size_t SliceFileReader::Read(void* buffer, size_t length, ReadOption option)
{
    size_t readLen = mSliceFileNode->Read(buffer, length, mOffset);
    mOffset += readLen;
    return readLen;
}

ByteSliceList* SliceFileReader::Read(size_t length, size_t offset, ReadOption option)
{ 
    INDEXLIB_FATAL_ERROR(UnSupported, "Not support Read with ByteSliceList");
    return NULL;
}

void* SliceFileReader::GetBaseAddress() const
{
    return mSliceFileNode->GetBaseAddress();
}

size_t SliceFileReader::GetLength() const
{

    return mSliceFileNode->GetLength();
}

const string& SliceFileReader::GetPath() const
{
    return mSliceFileNode->GetPath();
}

FSOpenType SliceFileReader::GetOpenType()
{ 
    return mSliceFileNode->GetOpenType();
}

void SliceFileReader::Close()
{
}

IE_NAMESPACE_END(file_system);

