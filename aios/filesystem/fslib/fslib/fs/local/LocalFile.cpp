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
#include "fslib/fs/local/LocalFile.h"

#include <aio.h>
#include <condition_variable>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <mutex>
#include <unistd.h>

#include "fslib/fs/local/LocalFileSystem.h"
#include "fslib/util/LongIntervalLog.h"

using namespace std;
FSLIB_BEGIN_NAMESPACE(fs);
AUTIL_DECLARE_AND_SETUP_LOGGER(fs, LocalFile);

LocalFile::LocalFile(const string &fileName, FILE *file, ErrorCode ec) : File(fileName, ec), _file(file) {}

LocalFile::~LocalFile() {
    if (_file) {
        FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
        fclose(_file);
        _file = NULL;
    }
}

ssize_t LocalFile::read(void *buffer, size_t length) {
    if (_file) {
        FSLIB_LONG_INTERVAL_LOG("fileName[%s], length[%ld]", _fileName.c_str(), length);
        ssize_t more = (ssize_t)length;
        if (more < 0) {
            AUTIL_LOG(ERROR, "read file %s fail, length overflow ssize_t", _fileName.c_str());
            _lastErrorCode = EC_BADARGS;
            return -1;
        }

        ssize_t ret = 0;
        off_t off = 0;
        while (more > 0) {
            ret = fread((char *)buffer + off, 1, more, _file);
            if (ferror(_file) != 0) {
                AUTIL_LOG(
                    ERROR, "read file %s fail at off %ld, with error %s!", _fileName.c_str(), off, strerror(errno));
                _lastErrorCode = LocalFileSystem::convertErrno(errno);
                return -1;
            }
            more -= ret;
            off += ret;
            if (feof(_file)) {
                break;
            }
        }
        return off;
    }

    AUTIL_LOG(ERROR, "read file %s fail, can not read file which is opened fail!", _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return -1;
}

ssize_t LocalFile::write(const void *buffer, size_t length) {
    if (_file) {
        FSLIB_LONG_INTERVAL_LOG("fileName[%s], length[%ld]", _fileName.c_str(), length);
        ssize_t more = (ssize_t)length;
        if (more < 0) {
            AUTIL_LOG(ERROR, "write file %s fail, length overflow ssize_t", _fileName.c_str());
            _lastErrorCode = EC_BADARGS;
            return -1;
        }

        ssize_t ret = 0;
        off_t off = 0;
        while (more > 0) {
            ret = fwrite((char *)buffer + off, 1, more, _file);
            if (ferror(_file) != 0) {
                AUTIL_LOG(ERROR, "write file %s fail, with error %s!", _fileName.c_str(), strerror(errno));
                _lastErrorCode = LocalFileSystem::convertErrno(errno);
                return -1;
            }
            more -= ret;
            off += ret;
        }
        return off;
    }

    AUTIL_LOG(ERROR, "write file %s fail, can not write file which is opened fail!", _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return -1;
}

ssize_t LocalFile::pread(void *buffer, size_t length, off_t offset) {
    if (NULL == _file) {
        FSLIB_LONG_INTERVAL_LOG("fileName[%s], length[%ld]", _fileName.c_str(), length);
        AUTIL_LOG(ERROR, "pread file %s fail, can not read file which is opened fail!", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }

    int fd = fileno(_file);
    ssize_t more = (ssize_t)length;
    if (more < 0) {
        AUTIL_LOG(ERROR, "pread file %s fail, length overflow ssize_t", _fileName.c_str());
        _lastErrorCode = EC_BADARGS;
        return -1;
    }

    off_t off = 0;
    ssize_t ret = 0;
    while (more > 0) {
        ret = ::pread(fd, (char *)buffer + off, more, offset + off);
        if (ret == -1) {
            AUTIL_LOG(ERROR, "pread file %s  fail, with error %s!", _fileName.c_str(), strerror(errno));
            _lastErrorCode = LocalFileSystem::convertErrno(errno);
            return ret;
        }
        if (ret == 0) {
            break;
        }
        more -= ret;
        off += ret;
    }
    return off;
}

namespace {

#if (__cplusplus >= 201703L)
struct AioSigval {
    aiocb aioCb;
    IOController *controller;
    std::function<void()> callback;

    AioSigval(IOController *IOController) : controller(IOController) { bzero((char *)&aioCb, sizeof(aioCb)); }
};
#endif
} // namespace

// void LocalFile::pread(IOController* controller, void* buffer, size_t length, off_t offset,
//                       std::function<void()> callback)
// {
//     File::pread(controller, buffer, length, offset, callback);
//     FIXME: use kernel native AIO
//     auto aioSigval = new AioSigval(controller);
//     aioSigval->callback = callback;
//     aioSigval->aioCb.aio_buf = buffer;
//     aioSigval->aioCb.aio_fildes = fileno(_file);
//     aioSigval->aioCb.aio_nbytes = length;
//     aioSigval->aioCb.aio_offset = offset;
//     aioSigval->aioCb.aio_sigevent.sigev_notify = SIGEV_THREAD;

//     aioSigval->aioCb.aio_sigevent.sigev_notify_function = [](sigval_t sigval) {
//         auto aioSigval = static_cast<AioSigval*>(sigval.sival_ptr);
//         if (aio_error(&(aioSigval->aioCb)) == 0) {
//             auto ret = aio_return(&(aioSigval->aioCb));
//             if (ret < 0) {
//                 aioSigval->controller->setErrorCode(LocalFileSystem::convertErrno(errno));
//             } else {
//                 aioSigval->controller->setErrorCode(EC_OK);
//                 aioSigval->controller->setIoSize(ret);
//             }
//         } else {
//             aioSigval->controller->setErrorCode(LocalFileSystem::convertErrno(errno));
//         }
//         aioSigval->callback();
//         delete aioSigval;
//     };

//     aioSigval->aioCb.aio_sigevent.sigev_notify_attributes = nullptr;
//     aioSigval->aioCb.aio_sigevent.sigev_value.sival_ptr = aioSigval;
//     if (aio_read(&(aioSigval->aioCb)) < 0) {
//         controller->setErrorCode(LocalFileSystem::convertErrno(errno));
//         callback();
//     }
// }

ssize_t LocalFile::preadv(const iovec *iov, int iovcnt, off_t offset) {
    if (NULL == _file) {
        FSLIB_LONG_INTERVAL_LOG("fileName[%s], iovcnt[%d]", _fileName.c_str(), iovcnt);
        AUTIL_LOG(ERROR, "preadv file %s fail, can not read file which is opened fail!", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }

    int fd = fileno(_file);
    ssize_t ret = ::preadv(fd, iov, iovcnt, offset);
    if (ret == -1) {
        AUTIL_LOG(ERROR, "preadv file %s  fail, with error %s!", _fileName.c_str(), strerror(errno));
        _lastErrorCode = LocalFileSystem::convertErrno(errno);
        return ret;
    }
    return ret;

    // IOController controller;
    // std::mutex mtx;
    // std::condition_variable cv;
    // bool done = false;
    // File::preadv(&controller, iov, iovcnt, offset, [&mtx, &cv, &done]() {
    //     std::unique_lock<std::mutex> lock(mtx);
    //     done = true;
    //     cv.notify_one();
    // });
    // std::unique_lock<std::mutex> lock(mtx);
    // cv.wait(lock, [&done]() { return done; });
    // return controller.getErrorCode() == EC_OK ? controller.getIoSize() : -1;
}

ssize_t LocalFile::pwrite(const void *buffer, size_t length, off_t offset) {
    if (NULL == _file) {
        FSLIB_LONG_INTERVAL_LOG("fileName[%s], length[%ld]", _fileName.c_str(), length);
        AUTIL_LOG(ERROR, "pwrite file %s fail, can not write file which is opened fail!", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }

    int fd = fileno(_file);
    ssize_t more = (ssize_t)length;
    if (more < 0) {
        AUTIL_LOG(ERROR, "pwrite file %s fail, length overflow ssize_t", _fileName.c_str());
        _lastErrorCode = EC_BADARGS;
        return -1;
    }

    off_t off = 0;
    ssize_t ret = 0;
    while (more > 0) {
        ret = ::pwrite(fd, (char *)buffer + off, more, offset + off);
        if (ret == -1) {
            AUTIL_LOG(ERROR, "pwrite file %s fail, with error %s!", _fileName.c_str(), strerror(errno));
            _lastErrorCode = LocalFileSystem::convertErrno(errno);
            return ret;
        }

        more -= ret;
        off += ret;
    }

    return off;
}

ErrorCode LocalFile::flush() {
    if (_file) {
        FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
        int ret = fflush(_file);
        if (0 == ret) {
            int fd = fileno(_file);
            ret |= fsync(fd);
        }
        if (0 != ret) {
            AUTIL_LOG(ERROR, "flush file %s fail, with error %s!", _fileName.c_str(), strerror(errno));
            _lastErrorCode = LocalFileSystem::convertErrno(errno);
            return _lastErrorCode;
        }
        return EC_OK;
    }

    AUTIL_LOG(ERROR, "flush file %s fail, can not flush file which is opened fail!", _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return EC_BADF;
}

ErrorCode LocalFile::sync() {
    if (_file) {
        FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
        int fd = fileno(_file);
        if (fsync(fd) != 0) {
            AUTIL_LOG(ERROR, "sync file %s fail, with error %s!", _fileName.c_str(), strerror(errno));
            _lastErrorCode = LocalFileSystem::convertErrno(errno);
            return _lastErrorCode;
        }
        return EC_OK;
    }

    AUTIL_LOG(ERROR, "sync file %s fail, can not sync file which is opened fail!", _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return EC_BADF;
}

ErrorCode LocalFile::close() {
    if (_file) {
        FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
        if (fclose(_file) != 0) {
            AUTIL_LOG(ERROR, "close file %s fail, with error %s!", _fileName.c_str(), strerror(errno));
            _lastErrorCode = LocalFileSystem::convertErrno(errno);
            _file = NULL;
            return _lastErrorCode;
        }
        _file = NULL;
        return EC_OK;
    }

    AUTIL_LOG(ERROR, "close file %s fail, can not close file which is opened fail!", _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return EC_BADF;
}

ErrorCode LocalFile::seek(int64_t offset, SeekFlag flag) {
    if (_file) {
        FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
        int ret = 0;
        switch (flag) {
        case FILE_SEEK_SET:
            ret = fseek(_file, offset, SEEK_SET);
            break;
        case FILE_SEEK_CUR:
            ret = fseek(_file, offset, SEEK_CUR);
            break;
        case FILE_SEEK_END:
            ret = fseek(_file, offset, SEEK_END);
            break;
        default:
            AUTIL_LOG(ERROR, "seek file %s fail, SeekFlag %d is not supported.", _fileName.c_str(), flag);
            _lastErrorCode = EC_NOTSUP;
            return _lastErrorCode;
        };

        if (ret != 0) {
            AUTIL_LOG(ERROR, "seek file %s fail, with error %s!", _fileName.c_str(), strerror(errno));
            _lastErrorCode = LocalFileSystem::convertErrno(errno);
            return _lastErrorCode;
        }

        return EC_OK;
    }

    AUTIL_LOG(ERROR, "seek file %s fail, can not seek file which is opened fail!", _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return EC_BADF;
}

int64_t LocalFile::tell() {
    if (_file) {
        FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
        int64_t ret = ftell(_file);
        if (ret < 0) {
            AUTIL_LOG(ERROR, "tell file %s fail, with error %s!", _fileName.c_str(), strerror(errno));
            _lastErrorCode = LocalFileSystem::convertErrno(errno);
        }
        return ret;
    }

    AUTIL_LOG(ERROR, "tell file %s fail, can not tell file which is opened fail!", _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return -1;
}

bool LocalFile::isOpened() const { return (_file != NULL); }

bool LocalFile::isEof() {
    if (_file) {
        FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
        return feof(_file) != 0;
    }

    AUTIL_LOG(ERROR, "isEof fail, can not tell whether the file %s reaches end!", _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return true;
}

FSLIB_END_NAMESPACE(fs);
