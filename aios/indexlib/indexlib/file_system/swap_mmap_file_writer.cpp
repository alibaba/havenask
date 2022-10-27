#include "indexlib/file_system/swap_mmap_file_writer.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/storage.h"

using namespace std;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, SwapMmapFileWriter);

SwapMmapFileWriter::SwapMmapFileWriter(const SwapMmapFileNodePtr& fileNode,
                                       Storage* storage)
    : mFileNode(fileNode)
    , mStorage(storage)
{
    assert(mFileNode);
    assert(mFileNode->GetOpenType() == FSOT_MMAP);
    assert(mStorage);
}

SwapMmapFileWriter::~SwapMmapFileWriter() 
{
}

void SwapMmapFileWriter::Open(const std::string& path) 
{
    INDEXLIB_FATAL_ERROR(UnSupported, "not support call open!");
}
    
size_t SwapMmapFileWriter::Write(const void* buffer, size_t length)
{
    size_t ret = mFileNode ? mFileNode->Write(buffer, length) : 0;
    if (mFileNode)
    {
        mStorage->Truncate(mFileNode->GetPath(), mFileNode->GetLength());
    }
    return ret;
}
    
size_t SwapMmapFileWriter::GetLength() const
{
    return mFileNode ? mFileNode->GetLength() : 0;
}
    
const string& SwapMmapFileWriter::GetPath() const
{
    if (!mFileNode)
    {
        static string tmp;
        return tmp;
    }
    return mFileNode->GetPath();
}
    
void SwapMmapFileWriter::Close()
{
    if (mFileNode)
    {
        mFileNode->SetRemainFile();
    }
    mFileNode.reset();
}

void SwapMmapFileWriter::Resize(size_t fileLen)
{
    if (mFileNode)
    {
        mStorage->Truncate(mFileNode->GetPath(), fileLen);
        mFileNode->Resize(fileLen);
    }
}

void SwapMmapFileWriter::Sync()
{
    if (mFileNode)
    {
        mFileNode->Sync();
    }
}

IE_NAMESPACE_END(file_system);

