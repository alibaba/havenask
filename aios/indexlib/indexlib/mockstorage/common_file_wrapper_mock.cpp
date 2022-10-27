#include "indexlib/storage/common_file_wrapper.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace future_lite;

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, CommonFileWrapper);

CommonFileWrapper::CommonFileWrapper(fslib::fs::File* file, bool useDirectIO,
                                     bool needClose) 
    : FileWrapper(file, needClose)
    , mUseDirectIO(useDirectIO)
{
}

CommonFileWrapper::~CommonFileWrapper() 
{
    if (!mNeedClose)
    {
        return;
    }
    if (mFile)
    {
        IE_LOG(ERROR, "file [%s] not CLOSED in destructor. Exception flag [%d]",
               mFile->getFileName(),
               misc::IoExceptionController::HasFileIOException());
        assert(misc::IoExceptionController::HasFileIOException());
        DELETE_AND_SET_NULL(mFile);
    }    
}

void CommonFileWrapper::Close()
{
    if (!mNeedClose)
    {
        return;
    }
    FileWrapper::CheckError(CLOSE_ERROR, mFile);
    FileWrapper::Close();    
}

size_t CommonFileWrapper::Read(void* buffer, size_t length)
{
    assert(mFile);
    FileWrapper::CheckError(READ_ERROR, mFile);
    return Read(mFile, buffer, length, mUseDirectIO);
}

size_t CommonFileWrapper::PRead(void* buffer, size_t length, off_t offset)
{
    assert(mFile);
    FileWrapper::CheckError(READ_ERROR, mFile);
    size_t leftToReadLen = length;
    size_t totalReadLen = 0;
    while (leftToReadLen > 0) 
    {
        size_t readLen = leftToReadLen > DEFAULT_READ_WRITE_LENGTH 
                         ? DEFAULT_READ_WRITE_LENGTH : leftToReadLen;
        ssize_t actualReadLen= mFile->pread(
                (uint8_t*)buffer + totalReadLen, readLen, offset);
        if (actualReadLen == -1) 
        {
            INDEXLIB_FATAL_IO_ERROR(mFile->getLastError(),
                    "PRead data from file: [%s] FAILED", 
                    mFile->getFileName());
        } 
        else if (actualReadLen == 0) 
        {
            break;
        }
        leftToReadLen -= actualReadLen;
        totalReadLen += actualReadLen;
        offset += actualReadLen;
        if (mUseDirectIO && (actualReadLen % MIN_ALIGNMENT) != 0)
        {
            break;
        }
    }
    return totalReadLen;
}

void CommonFileWrapper::Write(const void* buffer, size_t length)
{
    assert(mFile);
    FileWrapper::CheckError(WRITE_ERROR, mFile);
    FileWrapper::CheckError(COMMON_FILE_WRITE_ERROR, mFile);
    Write(mFile, buffer, length);
}

size_t CommonFileWrapper::Read(File *file, void *buffer, size_t length,
                               bool useDirectIO)
{
    FileWrapper::CheckError(READ_ERROR, file);
    size_t leftToReadLen = length;
    size_t totalReadLen = 0;
    while (leftToReadLen > 0) 
    {
        size_t readLen = leftToReadLen > DEFAULT_READ_WRITE_LENGTH 
                         ? DEFAULT_READ_WRITE_LENGTH : leftToReadLen;
        ssize_t actualReadLen = file->read((uint8_t*)buffer + totalReadLen, 
                readLen);
        if (actualReadLen == -1)
        {
            INDEXLIB_FATAL_IO_ERROR(file->getLastError(),
                    "Read data from file: [%s] FAILED", 
                    file->getFileName());
        } 
        else if (actualReadLen == 0) 
        {
            break;
        }
        leftToReadLen -= actualReadLen;
        totalReadLen += actualReadLen;
        if (useDirectIO && (actualReadLen % MIN_ALIGNMENT) != 0)
        {
            break;
        }
    }
    return totalReadLen;
}

void CommonFileWrapper::Write(File *file, const void *buffer, size_t length)
{
    FileWrapper::CheckError(WRITE_ERROR, file);
    FileWrapper::CheckError(COMMON_FILE_WRITE_ERROR, file);
    size_t leftToWriteLen = length;
    size_t totalWriteLen = 0;
    while (leftToWriteLen > 0) 
    {
        size_t writeLen = leftToWriteLen > DEFAULT_READ_WRITE_LENGTH 
                          ? DEFAULT_READ_WRITE_LENGTH : leftToWriteLen;
        ssize_t actualWriteLen = file->write(
                (uint8_t*)buffer + totalWriteLen, writeLen);
        if (-1 == actualWriteLen) 
        {
            INDEXLIB_FATAL_IO_ERROR(file->getLastError(),
                    "Write data to file: [%s] FAILED",
                    file->getFileName());
        }
        leftToWriteLen -= actualWriteLen;
        totalWriteLen += actualWriteLen;
    }
}

future_lite::Future<size_t>
CommonFileWrapper::PReadVAsync(const iovec* iov, int iovcnt, off_t offset, int advice)
{
    FileWrapper::CheckError(READ_ERROR, mFile);
    assert(mFile);
    size_t totalReadLen = 0;
    for (int i = 0; i < iovcnt; ++i)
    {
        totalReadLen += iov[i].iov_len;
    }
    if (totalReadLen > PANGU_MAX_READ_BYTES)
    {
        INDEXLIB_FATAL_IO_ERROR(mFile->getLastError(),
                                "PReadV data from file: [%s] FAILED, "
                                "total size [%ld] exceeds PANGU_MAX_READ_BYTES[%ld]", 
                                mFile->getFileName(), totalReadLen, PANGU_MAX_READ_BYTES);
    }
    Promise<size_t> promise;
    auto future = promise.getFuture();
    MoveWrapper<decltype(promise)> p(std::move(promise));

    auto controller = new IOController;
    controller->setAdvice(advice);
    mFile->preadv(controller, iov, iovcnt, offset, [p, controller, this]() mutable {
        if (controller->getErrorCode() == EC_OK)
        {
            p.get().setValue(controller->getIoSize());
        }
        else
        {
            try
            {
                INDEXLIB_FATAL_IO_ERROR(controller->getErrorCode(),
                    "preadv file[%s] failed", mFile->getFileName());
            }
            catch (...)
            {
                p.get().setException(std::current_exception());
            }
        }
        delete controller;
    });
    return future;
}

future_lite::Future<size_t> CommonFileWrapper::PReadAsync(void* buffer, size_t length, off_t offset, int advice)
{
    assert(mFile);
    auto readLen = PRead(buffer, length, offset);
    return makeReadyFuture<size_t>(readLen);
}


IE_NAMESPACE_END(storage);

