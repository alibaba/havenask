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
#include "fslib/fs/zookeeper/ZookeeperFile.h"

#include <errno.h>

#include "autil/TimeUtility.h"
#include "fslib/fs/zookeeper/ZookeeperFileSystem.h"
#include "fslib/util/LongIntervalLog.h"
#include "fslib/util/SafeBuffer.h"

using namespace std;
using namespace autil;
using namespace fslib::util;

FSLIB_PLUGIN_BEGIN_NAMESPACE(zookeeper);
AUTIL_DECLARE_AND_SETUP_LOGGER(zookeeper, ZookeeperFile);

ZookeeperFile::ZookeeperFile(
    zhandle_t *zh, const string &server, string &path, Flag flag, int64_t fileSize, int8_t retryCnt, ErrorCode ec)
    : File(path, ec), _zh(zh), _server(server), _buffer(NULL), _curPos(0), _retryCnt(retryCnt), _flag(flag) {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
    if (flag == WRITE) {
        _size = 0;
    } else {
        _size = fileSize;
        if (_zh) {
            _buffer = new char[_size];
            int zrc = ZookeeperFileSystem::zooGetWithRetry(
                _zh, _fileName.c_str(), 0, _buffer, &_size, NULL, _retryCnt, _server.c_str());
            _size = (_size < 0) ? 0 : _size;
            if (zrc != ZOK) {
                AUTIL_LOG(ERROR, "load data of file %s fail, %s!", _fileName.c_str(), zerror(zrc));
                _lastErrorCode = ZookeeperFileSystem::convertErrno(zrc);
                _size = 0;
            }
        }
    }

    if (ec == EC_OK) {
        _isEof = false;
    } else {
        _isEof = true;
    }
}

ZookeeperFile::~ZookeeperFile() {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
    AUTIL_LOG(DEBUG, "zookeeper file destruct, zkhandle:%p", _zh);

    if (_zh) {
        close();
    }

    if (_buffer) {
        delete[] _buffer;
        _buffer = NULL;
    }
}

ssize_t ZookeeperFile::read(void *buffer, size_t length) {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s], length[%ld]", _fileName.c_str(), length);
    if ((int32_t)length < 0) {
        AUTIL_LOG(ERROR, "read file %s fail, length should not larger than 2G.", _fileName.c_str());
        _lastErrorCode = EC_NOTSUP;
        return -1;
    }

    if (_zh) {
        if (_flag != READ) {
            AUTIL_LOG(ERROR, "read file %s fail, file is not opened in read mode", _fileName.c_str());
            _lastErrorCode = EC_BADF;
            return -1;
        }

        ssize_t readLen = 0;
        if ((int)length + _curPos <= _size) {
            readLen = length;
        } else {
            readLen = _size - _curPos;
        }

        memcpy(buffer, _buffer + _curPos, readLen);
        _curPos += readLen;

        if (readLen < (ssize_t)length) {
            _isEof = true;
        }
        return readLen;
    }

    AUTIL_LOG(ERROR, "read file %s fail, can not read file which is opened fail!", _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return -1;
}

ssize_t ZookeeperFile::write(const void *buffer, size_t length) {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s], length[%ld]", _fileName.c_str(), length);
    if ((int32_t)length < 0) {
        AUTIL_LOG(ERROR, "write file %s fail, length should not larger than 2G.", _fileName.c_str());
        _lastErrorCode = EC_NOTSUP;
        return -1;
    }

    if (_zh) {
        if (_flag == READ) {
            AUTIL_LOG(ERROR, "write file %s fail, file is opened in read mode", _fileName.c_str());
            _lastErrorCode = EC_BADF;
            return -1;
        }

        int zrc;
        if (_size > 0) {
            SafeBuffer tmpBuf(_size + length);
            int actLen = _size;
            zrc = ZookeeperFileSystem::zooGetWithRetry(
                _zh, _fileName.c_str(), 0, tmpBuf.getBuffer(), &actLen, NULL, _retryCnt, _server.c_str());
            if (zrc != ZOK) {
                AUTIL_LOG(ERROR, "write file %s fail, %s!", _fileName.c_str(), zerror(zrc));
                _lastErrorCode = ZookeeperFileSystem::convertErrno(zrc);
                return -1;
            }

            memcpy(tmpBuf.getBuffer() + _size, buffer, length);
            zrc = ZookeeperFileSystem::zooSetWithRetry(
                _zh, _fileName.c_str(), tmpBuf.getBuffer(), _size + length, -1, _retryCnt, _server.c_str());
        } else {
            zrc = ZookeeperFileSystem::zooSetWithRetry(
                _zh, _fileName.c_str(), (char *)buffer, length, -1, _retryCnt, _server.c_str());
        }

        if (zrc != ZOK) {
            AUTIL_LOG(ERROR, "write file %s fail, %s!", _fileName.c_str(), zerror(zrc));
            _lastErrorCode = ZookeeperFileSystem::convertErrno(zrc);
            return -1;
        }
        _curPos = length;
        _size += length;
        return length;
    }

    AUTIL_LOG(ERROR, "write file %s fail, can not write file which is opened fail!", _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return -1;
}

ssize_t ZookeeperFile::pread(void *buffer, size_t length, off_t offset) {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s], length[%ld]", _fileName.c_str(), length);
    if ((int32_t)length < 0) {
        AUTIL_LOG(ERROR, "pread file %s fail, length should not larger than 2G.", _fileName.c_str());
        _lastErrorCode = EC_NOTSUP;
        return -1;
    }

    if (_zh) {
        if (_flag != READ) {
            AUTIL_LOG(ERROR,
                      "pread file %s fail, file is not opened in read"
                      " mode",
                      _fileName.c_str());
            _lastErrorCode = EC_BADF;
            return -1;
        }

        if (offset >= _size) {
            AUTIL_LOG(DEBUG,
                      "pread file %s fail, offset is longer than file"
                      " length",
                      _fileName.c_str());
            return 0;
        }

        ssize_t readLen = 0;
        if ((int)length <= (_size - offset)) {
            readLen = length;
        } else {
            readLen = _size - offset;
        }

        memcpy(buffer, _buffer + offset, readLen);
        return readLen;
    }

    AUTIL_LOG(ERROR, "read file %s fail, can not read file which is opened fail!", _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return -1;
}

ssize_t ZookeeperFile::pwrite(const void *buffer, size_t length, off_t offset) {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s], length[%ld]", _fileName.c_str(), length);
    AUTIL_LOG(ERROR, "zookeeper not support pwrite!");
    _lastErrorCode = EC_NOTSUP;
    return -1;
}

ErrorCode ZookeeperFile::flush() {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
    if (!_zh) {
        AUTIL_LOG(ERROR,
                  "flush file %s fail, can not flush file "
                  "which is opened fail!",
                  _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return EC_BADF;
    }

    if (_flag == READ) {
        AUTIL_LOG(ERROR,
                  "flush file %s fail, can not flush file "
                  "opened in read mode!",
                  _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return EC_BADF;
    }

    return EC_OK;
}

ErrorCode ZookeeperFile::close() {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
    AUTIL_LOG(DEBUG, "close file %s.", _fileName.c_str());

    if (_zh) {
        int zrc = zookeeper_close(_zh);
        if (zrc != ZOK) {
            AUTIL_LOG(ERROR, "close file %s fail, %s!", _fileName.c_str(), zerror(zrc));
            ErrorCode ret = ZookeeperFileSystem::convertErrno(zrc);
            _lastErrorCode = ret;
            return ret;
        }
        _zh = NULL;
        return EC_OK;
    }

    AUTIL_LOG(ERROR,
              "close file %s fail, can not close file "
              "which is opened fail!",
              _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return EC_BADF;
}

ErrorCode ZookeeperFile::seek(int64_t offset, SeekFlag seekFlag) {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
    if (!_zh) {
        AUTIL_LOG(ERROR,
                  "seek file %s fail, can not seek file "
                  "which is opened fail!",
                  _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return EC_BADF;
    }

    if (_flag != READ) {
        AUTIL_LOG(ERROR,
                  "seek file %s fail, only support seek file "
                  "opened in read mode!",
                  _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return EC_BADF;
    }

    off_t pos = 0;
    switch (seekFlag) {
    case FILE_SEEK_SET:
        pos = offset;
        break;
    case FILE_SEEK_CUR:
        pos = offset + _curPos;
        break;
    case FILE_SEEK_END:
        pos = offset + _size;
        break;
    default:
        AUTIL_LOG(ERROR, "seek file %s fail, SeekFlag %d is not supported.", _fileName.c_str(), seekFlag);
        _lastErrorCode = EC_NOTSUP;
        return EC_NOTSUP;
    };

    if (pos >= (off_t)_size) {
        _curPos = _size;
    } else if (pos < 0) {
        _curPos = 0;
    } else {
        _curPos = (int)pos;
    }

    _isEof = false;
    return EC_OK;
}

int64_t ZookeeperFile::tell() {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
    if (!_zh) {
        AUTIL_LOG(ERROR,
                  "tell file %s fail, can not tell file "
                  "which is opened fail!",
                  _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }

    return _curPos;
}

bool ZookeeperFile::isOpened() const { return (_zh != NULL); }

bool ZookeeperFile::isEof() {
    if (_zh) {
        return _isEof;
    }

    AUTIL_LOG(ERROR,
              "isEof file %s fail, can not tell whether "
              "the file reaches the end!",
              _fileName.c_str());
    _lastErrorCode = EC_BADF;
    return true;
}

FSLIB_PLUGIN_END_NAMESPACE(zookeeper);
