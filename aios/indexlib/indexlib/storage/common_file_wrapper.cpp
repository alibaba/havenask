
#include "indexlib/storage/common_file_wrapper.h"
#include "indexlib/storage/data_flush_controller.h"
#include "indexlib/util/future_executor.h"
#include <future_lite/MoveWrapper.h>

using namespace std;
using namespace fslib;
using namespace fslib::fs;

using future_lite::Future;
using future_lite::Promise;
using future_lite::Try;
using future_lite::MoveWrapper;

IE_NAMESPACE_USE(util);

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
    FileWrapper::Close();    
}

size_t CommonFileWrapper::Read(void* buffer, size_t length)
{
    assert(mFile);
    return Read(mFile, buffer, length, mUseDirectIO);
}

size_t CommonFileWrapper::PRead(void* buffer, size_t length, off_t offset)
{
    return PReadAsync(buffer, length, offset, IO_ADVICE_NORMAL).get();
}

Future<size_t> CommonFileWrapper::PReadAsync(void* buffer, size_t length, off_t offset, int advice)
{
    return InternalPReadASync(buffer, length, offset, advice);
}

size_t CommonFileWrapper::PReadV(const iovec* iov, int iovcnt, off_t offset)
{
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
    ssize_t readLen = mFile->preadv(iov, iovcnt, offset);
    if (readLen == -1) 
    {
        INDEXLIB_FATAL_IO_ERROR(mFile->getLastError(),
                                "PReadV data from file: [%s] FAILED", 
                                mFile->getFileName());
    }
    return readLen;
}

Future<size_t> CommonFileWrapper::PReadVAsync(const iovec* iov, int iovcnt, off_t offset, int advice)
{
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

void CommonFileWrapper::Write(const void* buffer, size_t length)
{
    assert(mFile);
    Write(mFile, buffer, length);
}

size_t CommonFileWrapper::Read(File *file, void *buffer, size_t length,
                               bool useDirectIO)
{
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
    DataFlushController* flushController = DataFlushController::GetInstance();
    assert(flushController);

    size_t leftToWriteLen = length;
    size_t totalWriteLen = 0;
    while (leftToWriteLen > 0) 
    {
        size_t writeLen = leftToWriteLen > DEFAULT_READ_WRITE_LENGTH 
                          ? DEFAULT_READ_WRITE_LENGTH : leftToWriteLen;
        flushController->Write(file, (uint8_t*)buffer + totalWriteLen, writeLen);
        leftToWriteLen -= writeLen;
        totalWriteLen += writeLen;
    }
}

Future<size_t> CommonFileWrapper::InternalPReadASync(
    void* buffer, size_t length, off_t offset, int advice)
{
    if (length == 0)
    {
        return future_lite::makeReadyFuture((size_t)0);
    }
    size_t readLen = length > DEFAULT_READ_WRITE_LENGTH 
        ? DEFAULT_READ_WRITE_LENGTH : length;
    auto future = SinglePreadAsync(buffer, readLen, offset, advice)
        .thenValue([buffer, length, offset, advice, this](size_t result) mutable {
        if (result > 0)
        {
            if (mUseDirectIO && (result % MIN_ALIGNMENT) != 0)
            {
                return future_lite::makeReadyFuture<size_t>(result);
            }
            return InternalPReadASync((char*)buffer + result, length - result, offset + result, advice)
                .thenValue([result](size_t value) {
                               return result + value;
                });
        }
        else
        {
            return future_lite::makeReadyFuture((size_t)0);
        }
    });
    return future;
}

Future<size_t> CommonFileWrapper::SinglePreadAsync(
    void* buffer, size_t length, off_t offset, int advice)
{
    Promise<size_t> promise;
    auto future = promise.getFuture();
    MoveWrapper<decltype(promise)> p(std::move(promise));
    IOController* controller = new IOController();
    controller->setAdvice(advice);
    mFile->pread(controller, buffer, length, offset, [p, controller, this]() mutable {
        if (controller->getErrorCode() == EC_OK)
        {
            p.get().setValue(controller->getIoSize());
        }
        else
        {
            try
            {
                INDEXLIB_FATAL_IO_ERROR(controller->getErrorCode(), "pread file[%s] failed", mFile->getFileName());
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

IE_NAMESPACE_END(storage);

