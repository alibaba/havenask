#include "indexlib/misc/exception.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/storage/data_flush_controller.h"
#include "indexlib/file_system/buffered_file_node.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BufferedFileNode);

BufferedFileNode::BufferedFileNode(bool readOnly, const SessionFileCachePtr& fileCache)
    : mLength(0)
    , mFileBeginOffset(0)
    , mReadOnly(readOnly)
    , mFileCache(fileCache)
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
    const string& packageFileDataPath = packageOpenMeta.GetPhysicalFilePath();
    if (mReadOnly)
    {
        mFileBeginOffset = packageOpenMeta.GetOffset();
        mLength = packageOpenMeta.GetLength();
        mFile = CreateFile(packageFileDataPath, READ, false,
                           packageOpenMeta.GetPhysicalFileLength(), mFileBeginOffset, mLength);
    }
    else
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "BufferedFileNode does not support"
                             " open innerFile for WRITE mode");
    }

    if (unlikely(!mFile))
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Open file: [%s] FAILED",
                             packageFileDataPath.c_str());
    }
    if (!mFile->isOpened())
    {
        ErrorCode ec = mFile->getLastError();
        INDEXLIB_FATAL_IO_ERROR(ec, "Open file: [%s] FAILED", 
                                packageFileDataPath.c_str());
    }
}

void BufferedFileNode::DoOpen(const string& path, const FileMeta& fileMeta,
                              FSOpenType openType)
{
    if (mReadOnly)
    {
        mLength = (size_t)fileMeta.fileLength;
        mFileBeginOffset = 0;
        mFile = CreateFile(path, READ, false,
                           fileMeta.fileLength, mFileBeginOffset, fileMeta.fileLength);
    }
    else
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "BufferedFileNode does not support"
                             " open solid file for WRITE mode");
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

size_t BufferedFileNode::Read(void* buffer, size_t length, size_t offset, ReadOption option)
{
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

void BufferedFileNode::CheckError()
{
}

size_t BufferedFileNode::Write(const void* buffer, size_t length)
{
    DataFlushController* flushController = DataFlushController::GetInstance();
    assert(flushController);
    
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

        flushController->Write(mFile.get(), (uint8_t*)buffer + totalWriteLen, writeLen);
        leftToWriteLen -= writeLen;
        totalWriteLen += writeLen;
    }

    mLength += length;
    return length;
}

void BufferedFileNode::Close()
{
    if (mFile.get() && mFile->isOpened())
    {
        if (mFileCache)
        {
            mFileCache->Put(mFile.release(), mOriginalFileName);
            return;
        }
        
        ErrorCode ec = mFile->close();
        if (ec != EC_OK)
        {
            string fileName = mFile->getFileName();
            mFile.reset();
            INDEXLIB_FATAL_IO_ERROR(ec, "Close FAILED, file: [%s]",
                    fileName.c_str());
        }
        mFile.reset();
    }
}

void BufferedFileNode::IOCtlPrefetch(
    const FilePtr& file, ssize_t blockSize, size_t beginOffset, ssize_t fileLength)
{
    if (unlikely(!file))
    {
        return;
    }
    if ((fileLength == 0) || (fileLength > 0 && fileLength <= blockSize))
    {
        return;
    }
    int64_t blockCount = fileLength < 0 ? 4 : fileLength / blockSize + 1;
    ErrorCode ec = file->ioctl(IOCtlRequest::SetPrefetchParams, blockCount, (int64_t)blockSize,
                               (int64_t)beginOffset, (int64_t)fileLength);
    if (ec != EC_OK && ec != EC_NOTSUP)
    {
        IE_LOG(WARN, "file [%s] advice prefetch block size [%ld] error [%d]",
               mPath.c_str(), blockSize, ec);
        return;
    }
    ec = file->ioctl(IOCtlRequest::AdvisePrefetch);
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
    IE_LOG(DEBUG, "file [%s] advice prefetch blockSize[%lu], beginOffset[%lu], fileLength[%ld]", mPath.c_str(), blockSize, beginOffset, fileLength);
}

FilePtr BufferedFileNode::CreateFile(
    const string& fileName, Flag mode, bool useDirectIO,
    ssize_t physicalFileLength, size_t beginOffset, ssize_t fileLength)
{
    mOriginalFileName = fileName;
    if (mInPackage && mode == READ && !useDirectIO && mFileCache)
    {
        return FilePtr(mFileCache->Get(fileName, fileLength));
    }
    mFileCache.reset();
    FilePtr file(FileSystem::openFile(fileName, mode, useDirectIO, physicalFileLength));
    IOCtlPrefetch(file, FSReaderParam::DEFAULT_BUFFER_SIZE, beginOffset, fileLength);
    return file;
}

IE_NAMESPACE_END(file_system);

