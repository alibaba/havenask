#include "indexlib/file_system/normal_file_reader.h"
#include "indexlib/file_system/file_node.h"

using namespace std;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, NormalFileReader);

NormalFileReader::NormalFileReader(const FileNodePtr& fileNode)
    : mFileNode(fileNode) 
    , mCursor(0)
{
    assert(fileNode);
}

NormalFileReader::~NormalFileReader() 
{
    Close();
}

void NormalFileReader::Open()
{
    mFileNode->Populate();
}

size_t NormalFileReader::Read(void* buffer, size_t length, size_t offset, ReadOption option)
{
    return mFileNode->Read(buffer, length, offset, option);
}

future_lite::Future<size_t> NormalFileReader::ReadAsync(
        void* buffer, size_t length, size_t offset, ReadOption option)
{
    return mFileNode->ReadAsync(buffer, length, offset, option);
}

size_t NormalFileReader::Read(void* buffer, size_t length, ReadOption option)
{
    size_t readSize = mFileNode->Read(buffer, length, mCursor, option);
    mCursor += readSize;

    return readSize;
}

ByteSliceList* NormalFileReader::Read(size_t length, size_t offset, ReadOption option)
{ 
    return mFileNode->Read(length, offset, option);
}

void* NormalFileReader::GetBaseAddress() const
{
    return mFileNode->GetBaseAddress();
}

size_t NormalFileReader::GetLength() const
{
    return mFileNode->GetLength();
}

const string& NormalFileReader::GetPath() const
{
    return mFileNode->GetPath();
}

FSOpenType NormalFileReader::GetOpenType()
{ 
    return mFileNode->GetOpenType();
}

void NormalFileReader::Close()
{
}

void NormalFileReader::Lock(size_t offset, size_t length)
{
    return mFileNode->Lock(offset, length);
}

size_t NormalFileReader::Prefetch(size_t length, size_t offset, ReadOption option)
{
    return mFileNode->Prefetch(length, offset, option);
}

future_lite::Future<uint32_t>
NormalFileReader::ReadVUInt32Async(size_t offset, ReadOption option)
{
    return mFileNode->ReadVUInt32Async(offset, option);
}

IE_NAMESPACE_END(file_system);
