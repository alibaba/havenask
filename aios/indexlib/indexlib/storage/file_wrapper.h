#ifndef __INDEXLIB_FILE_WRAPPER_H
#define __INDEXLIB_FILE_WRAPPER_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/storage/file_buffer.h"
#include "indexlib/storage/fslib_option.h"
#include <sys/uio.h>
#include <future_lite/Future.h>
#include <exception>

IE_NAMESPACE_BEGIN(storage);

class FileWrapper
{
protected:
    static const uint32_t DEFAULT_BUF_SIZE = 1024;
    static const uint32_t DEFAULT_READ_WRITE_LENGTH = 64 * 1024 * 1024;

public:
    FileWrapper(fslib::fs::File* file, bool needClose = true);
    virtual ~FileWrapper();

public:
    /**
     * Read data to buffer
     * @param buffer [in] buffer to store the content
     * @param length [in] length in bytes of content to read 
     * @return the length of content actually read
     * @throw FileIOException if read failed.
     */
    virtual size_t Read(void* buffer, size_t length) = 0;

    /**
     * pread read content from current file to buffer, file offset is not changed
     * @param buffer [in] buffer to store 
     * @param length [in] length in bytes of content to read 
     * @param offset [in] file offset from which to read in bytes 
     * @return size_t, the length of content actually read;
     * @throw FileIOException if read failed.
     */
    virtual size_t PRead(void* buffer, size_t length, off_t offset) = 0;
    virtual future_lite::Future<size_t>
    PReadAsync(void* buffer, size_t length, off_t offset, int advice)
    {
        try
        {
            return future_lite::makeReadyFuture(PRead(buffer, length, offset));
        }
        catch (...)
        {
            return future_lite::makeReadyFuture<size_t>(std::current_exception());
        }
    }    

    virtual size_t PReadV(const iovec* iov, int iovcnt, off_t offset) { assert(false); return -1; };
    virtual future_lite::Future<size_t>
    PReadVAsync(const iovec* iov, int iovcnt, off_t offset, int advice)
    {
        try
        {
            return future_lite::makeReadyFuture(PReadV(iov, iovcnt, offset));
        }
        catch (...)
        {
            return future_lite::makeReadyFuture<size_t>(std::current_exception());
        }
    }

    /**
     * Write buffer to file
     * @param buffer [in] buffer to write from 
     * @param length [in] length in bytes of content to write
     * @throw FileIOException if write failed.
     */
    virtual void Write(const void* buffer, size_t length) = 0;

    /**
     * Set the current position of file to the position passed
     * @param offset [in] offset base on the flag
     * @param flag [in] FILE_SEEK_SET/FILE_SEEK_CUR/FILE_SEEK_END
     * @throw FileIOException if seek failed
     */
    virtual void Seek(int64_t offset, fslib::SeekFlag flag);

    /**
     * tell return current position of the file
     * @return int64_t, current position of the file
     * @throw FileIOException if tell failed
     */
    virtual int64_t Tell();

    /**
     * check whether the file reaches the end 
     * @return bool, true if the file reaches the end, otherwise return false
     */
    bool IsEof();

    /**
     * Return file name of the file
     */
    const char* GetFileName() const;

    /**
     * Return error code of last failed operation
     */
    fslib::ErrorCode GetLastError() const;

    /**
     * Flush the content of the file to persistent storage
     * @throw FileIOException if some error occurred
     */
    virtual void Flush();

    /**
     * Close the file
     * @throw FileIOException if some error occurred
     */
    virtual void Close();

    static void SetError(uint32_t errorType, const std::string& errorFile);
    static void CleanError() 
    {
        mErrorTypeList.clear();
    }

protected:
    static void CheckError(uint32_t errorType, fslib::fs::File* file);

protected:
    fslib::fs::File* mFile;
    bool mNeedClose;
    
private:
    FileWrapper(const FileWrapper&);

//just for test
public:
    static const uint32_t ERROR_NONE = 0x00;
    static const uint32_t READ_ERROR = 0x01;
    static const uint32_t WRITE_ERROR = 0x02;
    static const uint32_t TELL_ERROR = 0x04;
    static const uint32_t SEEK_ERROR = 0x08;
    static const uint32_t CLOSE_ERROR = 0x10;
    static const uint32_t FLUSH_ERROR = 0x20;
    static const uint32_t COMMON_FILE_WRITE_ERROR = 0x40;
    static std::map<std::string, uint32_t> mErrorTypeList;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FileWrapper);

//////////////////////////////////////////////////////////


IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_FILE_WRAPPER_H
