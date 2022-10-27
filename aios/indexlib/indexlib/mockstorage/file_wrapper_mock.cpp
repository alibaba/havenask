#include "indexlib/storage/file_wrapper.h"
#include "indexlib/storage/exception_trigger.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_BEGIN(storage);

IE_LOG_SETUP(storage, FileWrapper);

FileWrapper::FileWrapper(File* file, bool needClose)
    : mFile(file)
    , mNeedClose(needClose)
{
}
    
FileWrapper::~FileWrapper()
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
    }
    DELETE_AND_SET_NULL(mFile);    
}

void FileWrapper::Seek(int64_t offset, SeekFlag flag)
{
    assert(mFile);
    CheckError(SEEK_ERROR, mFile);
    ErrorCode ec = mFile->seek(offset, flag);
    if (ec != EC_OK)
    {
        INDEXLIB_FATAL_IO_ERROR(ec, "Seek to offset: [%lu] FAILED, file: [%s]",
                                offset, mFile->getFileName());
    }
}

void FileWrapper::SetError(uint32_t errorType, const std::string& errorFile)
{
    mErrorTypeList[errorFile] = errorType;
}

void FileWrapper::CheckError(uint32_t errorType, fslib::fs::File* file)
{
    string fileName;
    if (file)
    {
        fileName = string(file->getFileName());
    }

    if (ExceptionTrigger::CanTriggerException())
    {
        INDEXLIB_THROW(misc::FileIOException,
                       "file [%s] exception.", fileName.c_str());
    }

    if (!file)
    {
        return;
    }

    if ((mErrorTypeList.find(fileName) != mErrorTypeList.end())
        && mErrorTypeList[fileName] == errorType)
    {
        mErrorTypeList.erase(fileName);
        INDEXLIB_THROW(misc::FileIOException, "file exception.");
    }
}

int64_t FileWrapper::Tell()
{
    assert(mFile);

    CheckError(TELL_ERROR, mFile);
    int64_t ret = mFile->tell();
    if (ret < 0)
    {
        INDEXLIB_FATAL_IO_ERROR(mFile->getLastError(),
                                "Tell FAILED, file: [%s]",
                                mFile->getFileName());
    }
    return ret;
}

bool FileWrapper::IsEof()
{

    return mFile->isEof();
}

const char* FileWrapper::GetFileName() const
{
    assert(mFile);

    return mFile->getFileName();
}

ErrorCode FileWrapper::GetLastError() const
{
    assert(mFile);    
    return mFile->getLastError();
}

void FileWrapper::Flush()
{
    CheckError(FLUSH_ERROR, mFile);
    if (mFile && mFile->isOpened())
    {
        ErrorCode ec = mFile->flush();
        if (ec != EC_OK)
        {
            INDEXLIB_FATAL_IO_ERROR(ec, "Flush FAILED, file: [%s]",
                    mFile->getFileName());
        }
    }
}

void FileWrapper::Close()
{
    CheckError(CLOSE_ERROR, mFile);
    if (mFile && mFile->isOpened())
    {
        ErrorCode ec = mFile->close();
        if (ec != EC_OK)
        {
            INDEXLIB_FATAL_IO_ERROR(ec, "Close FAILED, file: [%s]",
                    mFile->getFileName());
        }

        delete mFile;
        mFile = NULL;
    }
}

IE_NAMESPACE_END(storage);

