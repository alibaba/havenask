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
#include "fslib/fs/MMapFile.h"

#include <errno.h>
#include <sys/mman.h>

#include "fslib/fs/local/LocalFileSystem.h"

using namespace std;
FSLIB_BEGIN_NAMESPACE(fs);
AUTIL_DECLARE_AND_SETUP_LOGGER(fs, MMapFile);

const int64_t MMapFile::MUNMAP_SLICE_SIZE = 1024 * 1024;

MMapFile::MMapFile(string fileName, int fd, char *base, int64_t length, int64_t pos, ErrorCode ec)
    : _fileName(fileName), _fd(fd), _length(length), _pos(pos), _base(base), _lastErrorCode(ec) {}

MMapFile::~MMapFile() { close(); }

char *MMapFile::getBaseAddress() { return _base; }

int64_t MMapFile::getLength() { return _length; }

ssize_t MMapFile::read(void *buffer, size_t len) {
    if (_base == NULL) {
        AUTIL_LOG(ERROR, "read mmapfile %s fail, file is not opened.", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }

    int64_t rlen;
    if (_pos >= _length) {
        return 0;
    }
    if (_pos + (int64_t)len > _length) {
        rlen = _length - _pos;
    } else {
        rlen = len;
    }

    memcpy(buffer, _base + _pos, rlen);
    _pos += rlen;
    return rlen;
}

ssize_t MMapFile::write(const void *buffer, size_t len) {
    if (_base == NULL) {
        AUTIL_LOG(ERROR, "write mmap file %s fail, file is not opened.", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }

    int64_t wlen = len;
    if (_pos + (int64_t)len > _length) {
        wlen = _length - _pos;
    }

    if (wlen > 0) {
        memcpy(_base + _pos, buffer, wlen);
        _pos += wlen;
    }

    return wlen;
}

ssize_t MMapFile::pread(void *buffer, size_t len, off_t offset) {
    AUTIL_LOG(ERROR, "pread mmap file %s fail, not support.", _fileName.c_str());
    _lastErrorCode = EC_NOTSUP;
    return -1;
}

ssize_t MMapFile::pwrite(const void *buffer, size_t len, off_t offset) {
    AUTIL_LOG(ERROR, "pwrite mmap file %s fail, not support.", _fileName.c_str());
    _lastErrorCode = EC_NOTSUP;
    return -1;
}

ErrorCode MMapFile::flush() {
    if (_base == NULL) {
        AUTIL_LOG(ERROR, "flush mmap file %s fail, file is not opened.", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return _lastErrorCode;
    }

    int ret = msync(_base, _length, MS_SYNC);
    if (ret == -1) {
        AUTIL_LOG(ERROR, "flush mmap file %s fail, %s.", _fileName.c_str(), strerror(errno));
        _lastErrorCode = LocalFileSystem::convertErrno(errno);
        return _lastErrorCode;
    }

    return EC_OK;
}

ErrorCode MMapFile::close() {
    ErrorCode ec = unloadData();

    if (_fd >= 0) {
        if (::close(_fd) < 0) {
            AUTIL_LOG(ERROR, "close fd of mmap file %s fail, %s.", _fileName.c_str(), strerror(errno));
            ec = LocalFileSystem::convertErrno(errno);
        }
        _fd = -1;
    }

    if (ec != EC_OK) {
        _lastErrorCode = ec;
        return ec;
    } else {
        return EC_OK;
    }
}

ErrorCode MMapFile::seek(int64_t offset, SeekFlag flag) {
    if (_base == NULL) {
        AUTIL_LOG(ERROR, "seek mmap file[%s] fail, file is not opened.", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return _lastErrorCode;
    }

    switch (flag) {
    case FILE_SEEK_SET:
        break;
    case FILE_SEEK_CUR:
        offset += _pos;
        break;
    case FILE_SEEK_END:
        offset += _length;
        break;
    default:
        AUTIL_LOG(ERROR, "seek mmap file %s fail, SeekFlag %d is not supported.", _fileName.c_str(), flag);
        _lastErrorCode = EC_NOTSUP;
        return _lastErrorCode;
    };

    if (offset < 0 || offset > _length) {
        _pos = 0;
        AUTIL_LOG(ERROR,
                  "seek mmap file %s fail, seek too many. current position can not be"
                  "negative.",
                  _fileName.c_str());
        _lastErrorCode = EC_NOTSUP;
        return _lastErrorCode;
    }

    _pos = offset;
    return EC_OK;
}

int64_t MMapFile::tell() {
    if (_base == NULL) {
        AUTIL_LOG(ERROR, "tell mmap file %s fail, file is not opened.", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }

    return _pos;
}

ErrorCode MMapFile::getLastError() const { return _lastErrorCode; }

bool MMapFile::isOpened() const { return _lastErrorCode == EC_OK; }

ErrorCode MMapFile::unloadData() {
    if (!_base) {
        return EC_OK;
    }
    AUTIL_LOG(DEBUG, "Begin unload mmap file [%s], length [%ldB]", _fileName.c_str(), _length);

    ErrorCode ec = EC_OK;
    for (int64_t offset = 0; offset < _length; offset += MUNMAP_SLICE_SIZE) {
        int64_t actualLen = min(_length - offset, MUNMAP_SLICE_SIZE);
        if (munmap(_base + offset, actualLen) < 0) {
            AUTIL_LOG(ERROR, "munmap file %s offset[%ld] fail, %s.", _fileName.c_str(), offset, strerror(errno));
            ec = LocalFileSystem::convertErrno(errno);
        }
        usleep(0);
    }
    _base = NULL;
    return ec;
}

int MMapFile::getFd() const { return _fd; }

const char *MMapFile::getFileName() const { return _fileName.c_str(); }

ErrorCode MMapFile::populate(bool lock, int64_t sliceSize, int64_t interval) { return EC_NOTSUP; }

void MMapFile::setDontDump() {
    if (_base) {
        if (-1 == madvise(_base, _length, MADV_DONTDUMP)) {
            AUTIL_LOG(WARN, "mmap file %s set MADV_DONTDUMP failed", _fileName.c_str());
        }
    }
}

FSLIB_END_NAMESPACE(fs);
