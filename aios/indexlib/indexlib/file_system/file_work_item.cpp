#include "indexlib/file_system/file_work_item.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);

WriteWorkItem::WriteWorkItem(BufferedFileOutputStream* stream, FileBuffer* fileBuffer)
    : mStream(stream)
    , mFileBuffer(fileBuffer)
{
}

WriteWorkItem::~WriteWorkItem() 
{
}

void WriteWorkItem::process() 
{
    mStream->Write(mFileBuffer->GetBaseAddr(), mFileBuffer->GetCursor());
}

void WriteWorkItem::destroy() 
{
    mFileBuffer->SetCursor(0);
    mFileBuffer->Notify();
    delete this;
}

void WriteWorkItem::drop() 
{
    destroy();
}

// read
ReadWorkItem::ReadWorkItem(FileNode* fileNode, FileBuffer* fileBuffer, 
                           size_t offset, ReadOption option)
    : mFileNode(fileNode)
    , mFileBuffer(fileBuffer)
    , mOffset(offset)
    , mOption(option)
{
}

ReadWorkItem::~ReadWorkItem() 
{
}

void ReadWorkItem::process() 
{
    size_t size = mFileNode->Read(mFileBuffer->GetBaseAddr(), 
                                  mFileBuffer->GetBufferSize(),
                                  mOffset, mOption);
    mFileBuffer->SetCursor(size);
}

void ReadWorkItem::destroy() 
{
    mFileBuffer->Notify();
    delete this;
}

void ReadWorkItem::drop() 
{
    destroy();
}

IE_NAMESPACE_END(file_system);

