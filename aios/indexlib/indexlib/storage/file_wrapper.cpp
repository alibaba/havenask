#include "indexlib/storage/file_wrapper.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_BEGIN(storage);

IE_LOG_SETUP(storage, FileWrapper);

map<string, uint32_t> FileWrapper::mErrorTypeList = map<string, uint32_t>();

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

    ErrorCode ec = mFile->seek(offset, flag);
    if (ec != EC_OK)
    {
        INDEXLIB_FATAL_IO_ERROR(ec, "Seek to offset: [%lu] FAILED, file: [%s]",
                                offset, mFile->getFileName());
    }
}

int64_t FileWrapper::Tell()
{
    assert(mFile);

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
    assert(mFile);

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

void FileWrapper::CheckError(uint32_t errorType, fslib::fs::File* file)
{
}

void FileWrapper::Flush()
{
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
    if (!mNeedClose)
    {
        return;
    }
    if (mFile && mFile->isOpened())
    {
        ErrorCode ec = mFile->close();

        if (ec != EC_OK)
        {
            string fileName(mFile->getFileName());
            INDEXLIB_FATAL_IO_ERROR(ec, "Close FAILED, file: [%s]",
                    fileName.c_str());
        }
        delete mFile;
        mFile = NULL;
    }
}

IE_NAMESPACE_END(storage);

