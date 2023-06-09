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
#include <unistd.h>
#include <errno.h>
#include "fslib/fs/local/LocalDirectFile.h"
#include "fslib/fs/local/LocalFileSystem.h"
#include "fslib/util/LongIntervalLog.h"
#include "fslib/util/EnvUtil.h"
#if (__cplusplus >= 201703L)
#include "future_lite/Executor.h"
#include "future_lite/IOExecutor.h"
#endif
#include <thread>

using namespace std;
FSLIB_BEGIN_NAMESPACE(fs);
AUTIL_DECLARE_AND_SETUP_LOGGER(fs, LocalDirectFile);

size_t LocalDirectFile::_alignment = getpagesize();
size_t LocalDirectFile::_minAlignment = 512;
bool LocalDirectFile::_asyncCoroRead = []() -> bool {
    return util::EnvUtil::GetEnv<int>("FSLIB_LOCAL_ASYNC_CORO_READ", 1);
}();

LocalDirectFile::LocalDirectFile(const string& fileName, int fd, ErrorCode ec)
    : File(fileName, ec)
    , _fd(fd)
    , _isEof(false)
{
}

LocalDirectFile::~LocalDirectFile() { 
    if (_fd > 0) {
        FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
        ::close(_fd);
        _fd = -1;
    }
    _isEof = false;
}

ssize_t LocalDirectFile::read(void* buffer, size_t length) {
    if (_fd >= 0) {
        FSLIB_LONG_INTERVAL_LOG("fileName[%s], length[%ld]", 
                                _fileName.c_str(), length);
        ssize_t more = (ssize_t)length;
        if (more < 0) {
            AUTIL_LOG(ERROR, "direct read file %s fail, length overflow ssize_t",
                      _fileName.c_str());
            _lastErrorCode = EC_BADARGS;
            return -1;
        }

        ssize_t ret = 0;
        off_t off = 0;
        while (more > 0) {
            ret = ::read(_fd, (char *)buffer + off, more);
            if (ret == -1) {
                int32_t curErrno = errno;
                AUTIL_LOG(ERROR, "direct read file %s fail, with error %s! "
                        "buffer[%p], off[%ld], more[%ld]",
                        _fileName.c_str(), strerror(errno), buffer, off, more);
                _lastErrorCode = LocalFileSystem::convertErrno(curErrno);
                return -1;
            }
            if (ret == 0) {
                break;
            }
            more -= ret;
            off += ret;
            if (ret % _minAlignment != 0) {
                break;
            }
        }
        if ((size_t)off < length && length > 0) {
            _isEof = true;
        }
        return off;
    }

    AUTIL_LOG(ERROR, "direct read file %s fail, can not read file which is"
              " opened fail!", _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return -1;
}


ssize_t LocalDirectFile::removeDirectIO() {
    int fmode = ::fcntl(_fd, F_GETFL);
    if ((fmode & O_DIRECT) == 0) {
        AUTIL_LOG(DEBUG, "file [%s] current mode is %o, "
                  "O_DIRECT mode already removed", _fileName.c_str(), fmode);
        return 0;
    }
    int preFmode = fmode;
    fmode = fmode - O_DIRECT;
    if (0 != ::fcntl(_fd, F_SETFL, fmode)) {
        int32_t curErrno = errno;        
        AUTIL_LOG(ERROR, "change file mode %s fail, with error %s!", 
                  _fileName.c_str(), strerror(errno));
        _lastErrorCode = LocalFileSystem::convertErrno(curErrno);
        return -1;            
    }
    AUTIL_LOG(DEBUG, "file [%s], remove directIO file mode, "
              "change mode from %o to %o", _fileName.c_str(), preFmode, fmode);
    return 0;
}

ssize_t LocalDirectFile::write(const void* buffer, size_t length) {
    if (_fd >= 0) {
        FSLIB_LONG_INTERVAL_LOG("fileName[%s], length[%ld]", 
                                _fileName.c_str(), length);
        ssize_t more = (ssize_t)length;
        if (more < 0) {
            AUTIL_LOG(ERROR, "direct write file %s fail, length overflow ssize_t",
                      _fileName.c_str());
            _lastErrorCode = EC_BADARGS;
            return -1;
        }

        ssize_t ret = 0;
        off_t off = 0;
        while (more > 0) {
            ret = doWrite((char*)buffer + off, more);
            if (ret == -1) {
                return -1;
            }
            more -= ret;
            off += ret;
        }
        return off;
    }
    AUTIL_LOG(ERROR, "direct write file %s fail, "
              "can not write file which is opened fail!", _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return -1;
}

ssize_t LocalDirectFile::doWrite(const void* buffer, size_t length) {
    int64_t offset = tell();
    size_t rest = 0;
    if (offset % _alignment != 0) {
        if (0 != removeDirectIO()) {
            AUTIL_LOG(ERROR, "remove O_DIRECT flag failed! fileName [%s], "
                      "Error code: %d", _fileName.c_str(), _lastErrorCode);
            return -1;
        } else {
            AUTIL_LOG(WARN, "fileName[%s], length[%ld] is not aligned, "
                      "O_DIRECT flag removed!", _fileName.c_str(), offset);
        }
        rest = 0;
    } else {
        rest = length % _alignment;
    }
    ssize_t ret = ::write(_fd, buffer, length - rest);
    if (ret == -1) {
        int32_t curErrno = errno;        
        AUTIL_LOG(ERROR, "write file [%s] fail, with error %s! "
                  "buffer[%p], length[%lu], rest[%lu]", 
                  _fileName.c_str(), strerror(errno), buffer, length, rest);
        _lastErrorCode = LocalFileSystem::convertErrno(curErrno);
        return -1;
    }

    if ((size_t)ret < length - rest) {
        AUTIL_LOG(INFO, "write %ld less than %lu, again", ret, (length - rest));
        return ret;
    }
        
    if (rest != 0) {
        if (0 != removeDirectIO()) {
            return -1;
        }
        ssize_t restRet = ::write(_fd, 
                (void *)((char *)buffer + length - rest), rest);
        if (restRet == -1) {
            int32_t curErrno = errno;            
            AUTIL_LOG(ERROR, "write file [%s] fail, with error %s! "
                      "buffer[%p], length[%lu], rest[%lu]", 
                      _fileName.c_str(), strerror(errno), buffer, length, rest);
            _lastErrorCode = LocalFileSystem::convertErrno(curErrno);
            return -1;
        }
        ret += restRet;
    }

    return (ssize_t)ret;
}

ssize_t LocalDirectFile::pread(void* buffer, size_t length, off_t offset) {
    if (_fd < 0) {
        AUTIL_LOG(ERROR, "direct pread file %s fail, can not read file which"
                  " is opened fail!", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }
    
    int fd = _fd;
    ssize_t more = (ssize_t)length;
    if (more < 0) {
        AUTIL_LOG(ERROR, "pread file %s fail, length overflow ssize_t",
                  _fileName.c_str());
        _lastErrorCode = EC_BADARGS;
        return -1;
    }
    FSLIB_LONG_INTERVAL_LOG("fileName[%s], length[%ld]", _fileName.c_str(), length);
    off_t off = 0;
    ssize_t ret = 0;
    while (more > 0) {
        ret = ::pread(fd, (char*)buffer + off, more, offset + off);
        if (ret == -1) {
            int32_t curErrno = errno;            
            AUTIL_LOG(ERROR, "pread file %s fail, with error %s! "
                      "buffer[%p], off[%ld], more[%ld], offset[%ld]", 
                      _fileName.c_str(), strerror(errno), buffer, off, more, offset);
            _lastErrorCode = LocalFileSystem::convertErrno(curErrno);
            return ret;
        }
        if (ret == 0) {
            break;
        }
        more -= ret;
        off += ret;
        if (ret % _minAlignment != 0) {
            break;
        }        
    }
    return off;
}

ssize_t LocalDirectFile::pwrite(const void* buffer, size_t length, off_t offset)
{
    if (_fd >= 0) {
        FSLIB_LONG_INTERVAL_LOG("fileName[%s], length[%ld]", 
                                _fileName.c_str(), length);
        ssize_t len = (ssize_t)length;
        if (len < 0) {
            AUTIL_LOG(ERROR, "pwrite file %s fail, length overflow ssize_t",
                      _fileName.c_str());
            _lastErrorCode = EC_BADARGS;
            return -1;
        }

        size_t rest = length % _alignment;
        ssize_t more = length - rest;
        off_t off = 0;
        while (more > 0) {
            ssize_t ret = ::pwrite(_fd, (void *)((char *)buffer + off), 
                    more, offset + off);
            if (ret == -1) {
                int32_t curErrno = errno;                
                AUTIL_LOG(ERROR, "write file %s fail, with error %s! "
                        "buffer[%p], off[%ld], more[%ld], offset[%ld]", 
                        _fileName.c_str(), strerror(errno), buffer, off, more, offset);
                _lastErrorCode = LocalFileSystem::convertErrno(curErrno);
                return -1;
            }
            if (ret == 0) {
                break;
            }
            more -= ret;
            off += ret;
        }
        
        if (rest != 0) {
            if (0 != removeDirectIO()) {
                return -1;
            }
            more = rest;
            while (more > 0) {
                ssize_t ret = ::pwrite(_fd, 
                        (void *)((char *)buffer + off), more, offset + off);
                if (ret == -1) {
                    int32_t curErrno = errno;                    
                    AUTIL_LOG(ERROR, "write file %s fail, with error %s! "
                            "buffer[%p], off[%ld] more[%ld], offset[%ld]", 
                            _fileName.c_str(), strerror(errno), buffer, off, more, offset);
                    _lastErrorCode = LocalFileSystem::convertErrno(curErrno);
                    return -1;
                }
                if (ret == 0) {
                    break;
                }
                more -= ret;
                off += ret;
            }
            fsync(_fd);
        }
        return (ssize_t)off;
    }
    
    AUTIL_LOG(ERROR, "direct write file %s fail, can not write file which is opened fail!", _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return -1;
}

#if (__cplusplus >= 201703L)
void LocalDirectFile::pread(IOController* controller, void* buffer, size_t length,
                            off_t offset, std::function<void()> callback) {
    if (!_asyncCoroRead || !controller->getExecutor() ||
        !controller->getExecutor()->getIOExecutor()) {
        return File::pread(controller, buffer, length, offset, std::move(callback));
    } else {
        controller->getExecutor()->getIOExecutor()->submitIO(
            _fd, future_lite::IOCB_CMD_PREAD, buffer, length, offset,
            [controller, callback = std::move(callback)](int32_t res) mutable {
                if ((signed long)(res) < 0) {
                    controller->setErrorCode(
                        LocalFileSystem::convertErrno(-(signed long)(res)));
                } else {
                    controller->setIoSize(res);
                    controller->setErrorCode(EC_OK);
                }
                callback();
            });
    }
}

void LocalDirectFile::preadv(IOController* controller, const iovec* iov, int iovcnt,
                             off_t offset, std::function<void()> callback) {
    if (!_asyncCoroRead || !controller->getExecutor() ||
        !controller->getExecutor()->getIOExecutor()) {
        return File::preadv(controller, iov, iovcnt, offset, std::move(callback));
    } else {
        controller->getExecutor()->getIOExecutor()->submitIOV(
            _fd, future_lite::IOCB_CMD_PREADV, iov, iovcnt, offset,
            [controller, callback = std::move(callback)](int32_t res) mutable {
                if ((signed long)(res) < 0) {
                    controller->setErrorCode(
                        LocalFileSystem::convertErrno(-(signed long)(res)));
                } else {
                    controller->setIoSize(res);
                    controller->setErrorCode(EC_OK);
                }
                callback();
            });
    }
}

void LocalDirectFile::pwrite(IOController* controller, void* buffer, size_t length,
                            off_t offset, std::function<void()> callback) {
    if (!_asyncCoroRead || !controller->getExecutor() ||
        !controller->getExecutor()->getIOExecutor()) {
        return File::pwrite(controller, buffer, length, offset, std::move(callback));
    } else {
        controller->getExecutor()->getIOExecutor()->submitIO(
            _fd, future_lite::IOCB_CMD_PWRITE, buffer, length, offset,
            [controller, callback = std::move(callback)](int32_t res) mutable {
                if ((signed long)(res) < 0) {
                    controller->setErrorCode(
                        LocalFileSystem::convertErrno(-(signed long)(res)));
                } else {
                    controller->setIoSize(res);
                    controller->setErrorCode(EC_OK);
                }
                callback();
            });
    }
}

void LocalDirectFile::pwritev(IOController* controller, const iovec* iov, int iovcnt,
                              off_t offset, std::function<void()> callback) {
    if (!_asyncCoroRead || !controller->getExecutor() ||
        !controller->getExecutor()->getIOExecutor()) {
        return File::pwritev(controller, iov, iovcnt, offset, std::move(callback));
    } else {
        controller->getExecutor()->getIOExecutor()->submitIOV(
            _fd, future_lite::IOCB_CMD_PWRITEV, iov, iovcnt, offset,
            [controller, callback = std::move(callback)](int32_t res) mutable {
                if ((signed long)(res) < 0) {
                    controller->setErrorCode(
                        LocalFileSystem::convertErrno(-(signed long)(res)));
                } else {
                    controller->setIoSize(res);
                    controller->setErrorCode(EC_OK);
                }
                callback();
            });
    }
}
#endif

ErrorCode LocalDirectFile::flush() {
    return EC_OK;
}

ErrorCode LocalDirectFile::sync() {
    if (_fd >= 0) {
        ::fsync(_fd); //promise meta flushded in directio
    }

    return EC_OK;
}

ErrorCode LocalDirectFile::close() {
    if (_fd >= 0) {
        FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
        if (::close(_fd) != 0) {
            int32_t curErrno = errno;            
            AUTIL_LOG(ERROR, "direct close file %s fail, with error %s!",
                      _fileName.c_str(), strerror(errno));
            _lastErrorCode = LocalFileSystem::convertErrno(curErrno);
            _fd = -1;
            _isEof = false;
            return _lastErrorCode;
        }
        _fd = -1;
        _isEof = false;
        return EC_OK;
    }

    AUTIL_LOG(ERROR, "close file %s fail, can not close file which is opened"
              " fail!", _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return EC_BADF;
}

ErrorCode LocalDirectFile::seek(int64_t offset, SeekFlag flag) {
    if (_fd >= 0) {
        FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
        off_t ret = 0;
        switch(flag) {
        case FILE_SEEK_SET:
            ret = lseek(_fd, offset, SEEK_SET);
            break;
        case FILE_SEEK_CUR:
            ret = lseek(_fd, offset, SEEK_CUR);
            break;
        case FILE_SEEK_END:
            ret = lseek(_fd, offset, SEEK_END);
            break;
        default:
            AUTIL_LOG(ERROR, "seek file %s fail, SeekFlag %d not supported.", 
                      _fileName.c_str(), flag);
            _lastErrorCode = EC_NOTSUP;
            return _lastErrorCode;
        };
        
        if (ret < 0) {
            int32_t curErrno = errno;            
            AUTIL_LOG(ERROR, "direct seek file %s fail, with error %s!",
                      _fileName.c_str(), strerror(errno));
            _lastErrorCode = LocalFileSystem::convertErrno(curErrno);
            return _lastErrorCode;
        }
        
        return EC_OK;
    }

    AUTIL_LOG(ERROR, "direct seek file %s fail, can not seek file which is "
              "opened fail!", _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return EC_BADF;
}

bool LocalDirectFile::isAppendMode() {
    int fmode = ::fcntl(_fd, F_GETFL);
    return (fmode & O_APPEND);
}

int64_t LocalDirectFile::tell() {
    if (_fd >= 0) {
        FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
        off_t ret = 0;
        if (isAppendMode()) {
            ret = lseek(_fd, 0, SEEK_END);
        } else {
            ret = lseek(_fd, 0, SEEK_CUR);
        }
        if (ret < 0) {
            int32_t curErrno = errno;            
            AUTIL_LOG(ERROR, "direct tell file %s fail, with error %s!",
                      _fileName.c_str(), strerror(errno));
            _lastErrorCode = LocalFileSystem::convertErrno(curErrno);
        }
        return (int64_t)ret;
    }

    AUTIL_LOG(ERROR, "direct tell file %s fail, can not tell file which is "
              "opened fail!", _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return -1;
}

bool LocalDirectFile::isOpened() const {
    return (_fd >= 0);
}

bool LocalDirectFile::isEof() {
    if (_fd >=0) {
        return _isEof;
    }

    AUTIL_LOG(ERROR, "direct isEof fail, can not tell whether the file %s "
              "reaches end!", _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return true;
}

FSLIB_END_NAMESPACE(fs);

