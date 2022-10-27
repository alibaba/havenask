#include "indexlib/storage/file_work_item.h"
#include "indexlib/storage/common_file_wrapper.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_BEGIN(storage);

WriteWorkItem::WriteWorkItem(File *file, FileBuffer *fileBuffer)
    : mFile(file)
    , mFileBuffer(fileBuffer)
{
}

WriteWorkItem::~WriteWorkItem() 
{
}

void WriteWorkItem::process() 
{
    CommonFileWrapper::Write(mFile, mFileBuffer->GetBaseAddr(), 
                             mFileBuffer->GetCursor());
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
ReadWorkItem::ReadWorkItem(File *file, FileBuffer *fileBuffer)
    : mFile(file)
    , mFileBuffer(fileBuffer)
{
}

ReadWorkItem::~ReadWorkItem() 
{
}

void ReadWorkItem::process() 
{
    size_t size = CommonFileWrapper::Read(mFile, mFileBuffer->GetBaseAddr(), 
            mFileBuffer->GetBufferSize());
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

IE_NAMESPACE_END(storage);

