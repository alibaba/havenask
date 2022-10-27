#include "indexlib/file_system/file_system_define.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/file_system/buffered_file_node.h"
#include "indexlib/file_system/file_work_item.h"
#include "indexlib/file_system/buffered_file_output_stream.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/storage/file_buffer.h"
#include "indexlib/util/thread_pool.h"
#include "indexlib/util/path_util.h"
#include <fslib/fs/FileSystem.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BufferedFileWriter);

BufferedFileWriter::BufferedFileWriter(const FSWriterParam& param)
    : FileWriter(param)
    , mLength(0)
    , mIsClosed(0)
    , mBuffer(new FileBuffer(param.bufferSize))
    , mSwitchBuffer(NULL)
{
    if (param.asyncDump)
    {
        mSwitchBuffer = new FileBuffer(param.bufferSize);
        mThreadPool = FileSystemWrapper::GetThreadPool(fslib::WRITE);
    }

}

BufferedFileWriter::~BufferedFileWriter() 
{
    if (!mIsClosed)
    {
        IE_LOG(ERROR, "file [%s] not CLOSED in destructor. Exception flag [%d]",
               mFilePath.c_str(),
               misc::IoExceptionController::HasFileIOException());
        assert(misc::IoExceptionController::HasFileIOException());        
    }
    DELETE_AND_SET_NULL(mBuffer);
    DELETE_AND_SET_NULL(mSwitchBuffer);
}

void BufferedFileWriter::ResetBufferParam(size_t bufferSize, bool asyncWrite)
{
    if (mLength > 0)
    {
        INDEXLIB_FATAL_ERROR(UnSupported,
                             "Not support ResetBufferParam after Write Data");
    }

    DELETE_AND_SET_NULL(mBuffer);
    DELETE_AND_SET_NULL(mSwitchBuffer);
    mBuffer = new FileBuffer(bufferSize);
    if (asyncWrite)
    {
        mSwitchBuffer = new FileBuffer(bufferSize);
        mThreadPool = FileSystemWrapper::GetThreadPool(fslib::WRITE);
    }
    else
    {
        mThreadPool.reset();
    }
}

void BufferedFileWriter::OpenWithoutCheck(const string& path)
{
    mFilePath = path;
    mStream.reset(new BufferedFileOutputStream(mWriterParam.raidConfig));
    string dumpPath = GetDumpPath();
    mStream->Open(dumpPath);
    mIsClosed = false;
}

string BufferedFileWriter::GetDumpPath() const
{
    if (mWriterParam.atomicDump)
    {
        return mFilePath + TEMP_FILE_SUFFIX;
    }
    return mFilePath;
}

void BufferedFileWriter::Open(const string& path)
{
    mFilePath = path;
    string dumpPath = GetDumpPath();
    if (FileSystemWrapper::IsExistIgnoreError(dumpPath))
    {
        IE_LOG(WARN, "file: %s already exists and will be removed", dumpPath.c_str());
        FileSystemWrapper::DeleteFile(dumpPath);
    }
    mStream.reset(new BufferedFileOutputStream(mWriterParam.raidConfig));
    mStream->Open(dumpPath);
    mIsClosed = false;
}

size_t BufferedFileWriter::Write(const void* buffer, size_t length)
{
    const char* cursor = (const char*)buffer;
    size_t leftLen = length;
    while (true)
    {
        uint32_t leftLenInBuffer = mBuffer->GetBufferSize() - mBuffer->GetCursor();
        uint32_t lengthToWrite =  leftLenInBuffer < leftLen ? leftLenInBuffer : leftLen;
        mBuffer->CopyToBuffer(cursor, lengthToWrite);
        cursor += lengthToWrite;
        leftLen -= lengthToWrite;
        if (leftLen <= 0)
        {
            assert(leftLen == 0);
            mLength += length;
            break;
        }
        if (mBuffer->GetCursor() == mBuffer->GetBufferSize())
        {
            DumpBuffer();
        }
    }

    return length;
}

size_t BufferedFileWriter::GetLength() const
{
    return mLength;
}

const string& BufferedFileWriter::GetPath() const
{
    return mFilePath;
}

void BufferedFileWriter::Close()
{
    if (mIsClosed)
    {
        return;
    }
    mIsClosed = true;
    DumpBuffer();

    if (mSwitchBuffer)
    {
        mSwitchBuffer->Wait();
    }

    mStream->Close();

    if (mWriterParam.atomicDump)
    {
        string dumpPath = GetDumpPath();
        IE_LOG(DEBUG, "Rename temp file[%s] to file[%s].", 
               dumpPath.c_str(), mFilePath.c_str());
        try
        {
            FileSystemWrapper::Rename(dumpPath, mFilePath);
        }
        catch (ExistException& e)
        {
            IE_LOG(WARN, "file [%s] already exist, remove it for renaming purpose", mFilePath.c_str());
            FileSystemWrapper::DeleteFile(mFilePath);
            FileSystemWrapper::Rename(dumpPath, mFilePath);
        }
        catch (...)
        {
            throw;
        }
    }

    if (mThreadPool)
    {
        mThreadPool->CheckException();
    }
}

void BufferedFileWriter::DumpBuffer() 
{
    if (mThreadPool)
    {
        mSwitchBuffer->Wait();
        std::swap(mBuffer, mSwitchBuffer);
        WorkItem* item = new WriteWorkItem(mStream.get(), mSwitchBuffer);
        mSwitchBuffer->SetBusy();
        mThreadPool->PushWorkItem(item);
    }
    else
    {
        mStream->Write(mBuffer->GetBaseAddr(), mBuffer->GetCursor());
        mBuffer->SetCursor(0);
    }
}

IE_NAMESPACE_END(file_system);

