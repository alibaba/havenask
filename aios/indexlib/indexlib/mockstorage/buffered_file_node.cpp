#include "indexlib/misc/exception.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/buffered_file_node.h"
#include "indexlib/storage/exception_trigger.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BufferedFileNode);

BufferedFileNode::BufferedFileNode(bool readOnly,
                                   const SessionFileCachePtr& fileCache)
    : mLength(0)
    , mFileBeginOffset(0)
    , mReadOnly(readOnly)
{
}

BufferedFileNode::~BufferedFileNode() 
{
    if (mFile)
    {
        try
        {
            Close();
        }
        catch(...)
        {}
    }    
}

void BufferedFileNode::DoOpen(const string& path, FSOpenType openType)
{
    CheckError();
    if (mReadOnly)
    {
        FileMeta fileMeta;
        fileMeta.fileLength = 0;
        ErrorCode ec = FileSystem::getFileMeta(path, fileMeta);
        if (ec != EC_OK)
        {
            INDEXLIB_FATAL_IO_ERROR(ec, "Get file : [%s] meta FAILED", path.c_str());
        }

        mLength = (size_t)fileMeta.fileLength;
        mFileBeginOffset = 0;
        mFile = CreateFile(path, READ, false, mLength, mFileBeginOffset, mLength);
    }
    else
    {
        mFile = CreateFile(path, WRITE, false, -1, 0, -1);
    }

    if (unlikely(!mFile))
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Open file: [%s] FAILED", path.c_str());
    }
    if (!mFile->isOpened())
    {
        ErrorCode ec = mFile->getLastError();
        INDEXLIB_FATAL_IO_ERROR(ec, "Open file: [%s] FAILED", path.c_str());
    }
}

void BufferedFileNode::DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType)
{    
    CheckError();
    const string& packageFileDataPath = packageOpenMeta.GetPhysicalFilePath();
    if (mReadOnly)
    {
        mFileBeginOffset = packageOpenMeta.GetOffset();
        mLength = packageOpenMeta.GetLength();
        mFile = CreateFile(
            packageFileDataPath, READ, false,
            packageOpenMeta.GetPhysicalFileLength(), mFileBeginOffset, mLength);
    }
    else
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "BufferedFileNode does not support"
                             " open innerFile for WRITE mode");
    }

    if (unlikely(!mFile))
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Open file: [%s] FAILED", mPath.c_str());
    }
    if (!mFile->isOpened())
    {
        ErrorCode ec = mFile->getLastError();
        INDEXLIB_FATAL_IO_ERROR(ec, "Open file: [%s] FAILED", 
                                packageFileDataPath.c_str());
    }
}


FSFileType BufferedFileNode::GetType() const
{
    return FSFT_BUFFERED;
}

size_t BufferedFileNode::GetLength() const
{
    return mLength;
}

void* BufferedFileNode::GetBaseAddress() const
{
    IE_LOG(DEBUG, "Not support GetBaseAddress");
    return NULL;
}

void BufferedFileNode::CheckError()
{
    if (ExceptionTrigger::CanTriggerException())
    {
        INDEXLIB_THROW(misc::FileIOException,
                       "buffered file exception.");
    }
}

size_t BufferedFileNode::Read(void* buffer, size_t length, size_t offset, ReadOption option)
{
    CheckError();
    assert(mFile.get());
    size_t leftToReadLen = length;
    size_t totalReadLen = 0;
    while (leftToReadLen > 0) 
    {
        size_t readLen = leftToReadLen > DEFAULT_READ_WRITE_LENGTH 
                         ? DEFAULT_READ_WRITE_LENGTH : leftToReadLen;
        ssize_t actualReadLen= mFile->pread(
            (uint8_t*)buffer + totalReadLen, readLen, offset + mFileBeginOffset);
        if (actualReadLen == -1) 
        {
            INDEXLIB_FATAL_IO_ERROR(mFile->getLastError(),
                    "Read data from file: [%s] FAILED", 
                    mFile->getFileName());
        } 
        else if (actualReadLen == 0) 
        {
            break;
        }
        leftToReadLen -= actualReadLen;
        totalReadLen += actualReadLen;
        offset += actualReadLen;
    }
    return totalReadLen;
}

ByteSliceList* BufferedFileNode::Read(size_t length, size_t offset, ReadOption option)
{
    INDEXLIB_FATAL_ERROR(UnSupported, "Not support Read without buffer");
    return NULL;
}

size_t BufferedFileNode::Write(const void* buffer, size_t length)
{
    CheckError();
    if (mReadOnly)
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "ReadOnly file not support write.");
    }
    assert(mFile.get());
    size_t leftToWriteLen = length;
    size_t totalWriteLen = 0;
    while (leftToWriteLen > 0) 
    {
        size_t writeLen = leftToWriteLen > DEFAULT_READ_WRITE_LENGTH 
                          ? DEFAULT_READ_WRITE_LENGTH : leftToWriteLen;
        ssize_t actualWriteLen = mFile->write(
                (uint8_t*)buffer + totalWriteLen, writeLen);
        if ((ssize_t)writeLen != actualWriteLen) 
        {
            INDEXLIB_FATAL_IO_ERROR(mFile->getLastError(),
                    "Write data to file: [%s] FAILED",
                    mFile->getFileName());
        }
        leftToWriteLen -= actualWriteLen;
        totalWriteLen += actualWriteLen;
    }

    mLength += length;

    return length;
}

void BufferedFileNode::Close()
{
    CheckError();
    if (mFile.get() && mFile->isOpened())
    {
        ErrorCode ec = mFile->close();
        if (ec != EC_OK)
        {
            INDEXLIB_FATAL_IO_ERROR(ec, "Close FAILED, file: [%s]",
                    mFile->getFileName());
        }
    }
}

void BufferedFileNode::IOCtlPrefetch(
    const FilePtr& file, ssize_t blockSize, size_t beginOffset, ssize_t fileLength)
{
    if (unlikely(!file))
    {
        return;
    }
    ErrorCode ec = file->ioctl(IOCtlRequest::AdvisePrefetch);
    if (ec == EC_NOTSUP)
    {
        IE_LOG(DEBUG, "file [%s] advice prefetch not support", mPath.c_str());
        return;
    }
    else if (unlikely(ec != EC_OK))
    {
        IE_LOG(WARN, "file [%s] advice prefetch error [%d]", mPath.c_str(), ec);
        return;
    }
    ec = file->ioctl(IOCtlRequest::SetPrefetchParams, 4, (int64_t)blockSize,
                     (int64_t)beginOffset, (int64_t)fileLength);
    if (unlikely(ec != EC_OK && ec != EC_NOTSUP))
    {
        IE_LOG(WARN, "file [%s] advice prefetch block size [%ld] error [%d]",
               mPath.c_str(), blockSize, ec);
        return;
    }
    IE_LOG(DEBUG, "file [%s] advice prefetch blockSize[%ld], beginOffset[%lu], fileLength[%lu]",
           mPath.c_str(), blockSize, beginOffset, fileLength);
}

FilePtr BufferedFileNode::CreateFile(
    const string& fileName, Flag mode, bool useDirectIO,
    ssize_t physicalFileLength, size_t beginOffset, ssize_t fileLength)
{
    return FilePtr(FileSystem::openFile(fileName, mode, useDirectIO, fileLength));
}

IE_NAMESPACE_END(file_system);

