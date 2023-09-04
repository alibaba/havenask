/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef FSLIB_FILE_H
#define FSLIB_FILE_H

#include <functional>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/uio.h>

#include "autil/Log.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"

#if (__cplusplus >= 201703L)
#include "fslib/fs/IOController.h"
#endif

FSLIB_BEGIN_NAMESPACE(fs);

class File {
public:
    File(const std::string &fileName, ErrorCode ec = EC_OK) {
        _fileName = fileName;
        _lastErrorCode = ec;
    }

    virtual ~File() {}

public:
    /**
     * read read content from current file to buffer
     * @param buffer [in] buffer to store the content
     * @param length [in] length in bytes of content to read
     * @return ssize_t, the length of content actually read, -1 indicates an error
     */
    virtual ssize_t read(void *buffer, size_t length) = 0;

    /**
     * write write content from buffer to current file
     * @param buffer [in] buffer to write from
     * @param length [in] length in bytes of content to write
     * @return ssize_t, the length of content actually write, -1 indicates an error
     */
    virtual ssize_t write(const void *buffer, size_t length) = 0;

    /**
     * pread read content from current file to buffer, file offset is not changed
     * @param buffer [in] buffer to store
     * @param length [in] length in bytes of content to read
     * @param offset [in] file offset from which to read in bytes
     * @return ssize_t, the length of content actually read, -1 indicates an error
     */
    virtual ssize_t pread(void *buffer, size_t length, off_t offset) = 0;

#if (__cplusplus >= 201703L)
    virtual void
    pread(IOController *controller, void *buffer, size_t length, off_t offset, std::function<void()> callback) {
        auto ret = pread(buffer, length, offset);
        if (ret < 0) {
            controller->setErrorCode(EC_UNKNOWN);
        } else {
            controller->setIoSize(ret);
            controller->setErrorCode(EC_OK);
        }
        callback();
    }
#endif

    /**
     * preadv() reads iovcnt buffers from current file into the buffers described by iov
     * @param buffer [iov] points to an array of iovec structures, refer to preadv(2)
     * @param length [iovcnt] count of iovec
     * @param offset [offset] file offset from which to read in bytes
     * @return ssize_t, the length of content actually read, -1 indicates an error
     */
    virtual ssize_t preadv(const iovec *iov, int iovcnt, off_t offset) {
        ssize_t readBytes = 0;
        for (int i = 0; i < iovcnt; ++i) {
            int ret = pread(iov[i].iov_base, iov[i].iov_len, offset + readBytes);
            if (ret < 0) {
                return ret;
            }
            readBytes += ret;
            if (static_cast<size_t>(ret) != iov[i].iov_len) {
                break;
            }
        }
        return readBytes;
    }

#if (__cplusplus >= 201703L)
    virtual void
    preadv(IOController *controller, const iovec *iov, int iovcnt, off_t offset, std::function<void()> callback) {
        auto ret = preadv(iov, iovcnt, offset);
        if (ret < 0) {
            controller->setErrorCode(EC_UNKNOWN);
        } else {
            controller->setIoSize(ret);
            controller->setErrorCode(EC_OK);
        }
        callback();
    }
#endif
    /**
     * pwrite write content from buffer to current file, file offset is not changed
     * @param buffer [in] buffer to write from
     * @param length [in] length in bytes of content to write
     * @param offset [in] file offset from which to write in bytes
     * @return ssize_t, the length of content actually write, -1 indicates an error
     */
    virtual ssize_t pwrite(const void *buffer, size_t length, off_t offset) = 0;

#if (__cplusplus >= 201703L)
    virtual void
    pwrite(IOController *controller, void *buffer, size_t length, off_t offset, std::function<void()> callback) {
        auto ret = pwrite(buffer, length, offset);
        if (ret < 0) {
            controller->setErrorCode(_lastErrorCode);
        } else {
            controller->setIoSize(ret);
            controller->setErrorCode(EC_OK);
        }
        callback();
    }
#endif
    virtual ssize_t pwritev(const iovec *iov, int iovcnt, off_t offset) {
        ssize_t writtenBytes = 0;
        for (int i = 0; i < iovcnt; ++i) {
            int ret = pwrite(iov[i].iov_base, iov[i].iov_len, offset + writtenBytes);
            if (ret < 0) {
                return ret;
            }
            writtenBytes += ret;
            if (static_cast<size_t>(ret) != iov[i].iov_len) {
                break;
            }
        }
        return writtenBytes;
    }

#if (__cplusplus >= 201703L)
    virtual void
    pwritev(IOController *controller, const iovec *iov, int iovcnt, off_t offset, std::function<void()> callback) {
        auto ret = pwritev(iov, iovcnt, offset);
        if (ret < 0) {
            controller->setErrorCode(_lastErrorCode);
        } else {
            controller->setIoSize(ret);
            controller->setErrorCode(EC_OK);
        }
        callback();
    }
#endif
    /**
     * flush dump the content from stream to the file on disk
     * pangu file auto flush per 64M(by default, and you can reset it) bits you write
     * @return ErrorCode, EC_OK means success, other means fail.
     */
    virtual ErrorCode flush() = 0;

    /**
     * Similar to posix fsync, Flush out the data in client's
     * user buffer. all the way to the disk device (but the disk may have
     * it in its cache).
     * see: hdfs plugin or pangu plugin
     * @return ErrorCode, EC_OK means success, other means fail.
     */
    virtual ErrorCode sync() { return EC_NOTSUP; }

    /**
     * close close the file
     * @return ErrorCode, EC_OK means success, other means fail.
     */
    virtual ErrorCode close() = 0;

    /**
     * seek set the current position of file to the position passed
     * @param offset [in] offset base on the flag
     * @param flag [in] FILE_SEEK_SET/FILE_SEEK_CUR/FILE_SEEK_END
     * @return ErrorCode, EC_OK means success, other means fail.
     */
    virtual ErrorCode seek(int64_t offset, SeekFlag flag) = 0;

    /**
     * tell return current position of the file
     * @return int64_t, current position of the file
     */
    virtual int64_t tell() = 0;

    /**
     * getLastError return the error code of last operation
     * @return ErrorCode, error code of the last failed operation
     */
    virtual ErrorCode getLastError() const { return _lastErrorCode; }

    /**
     * getFileName return the pointer to the name of the file
     * @return char*
     */
    const char *getFileName() const { return _fileName.c_str(); }

    /**
     * isOpened check whether the file is successfully opened
     * @return bool, true if the file is successfully opened, otherwise return false
     */
    virtual bool isOpened() const = 0;

    /**
     * isEof check whether the file reaches the end
     * @return bool, true if the file reaches the end, otherwise return false
     */
    virtual bool isEof() = 0;

    /**
     * ioctl allow out-of-band hints and control commands passed in-and-out between `fslib` and user.
     */
    ErrorCode ioctl(IOCtlRequest request, ...) {
        va_list args;
        va_start(args, request);
        ErrorCode ret = ioctlImpl(request, args);
        va_end(args);
        return ret;
    }

    virtual ErrorCode ioctlImpl(IOCtlRequest request, va_list args) { return EC_NOTSUP; }

private:
    friend class FileTest;

protected:
    std::string _fileName;
    ErrorCode _lastErrorCode;
};

FSLIB_TYPEDEF_AUTO_PTR(File);

FSLIB_END_NAMESPACE(fs);

#endif // FSLIB_FILE_H
