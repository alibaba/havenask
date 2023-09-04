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
#include "fslib/fs/hdfs/HdfsFile.h"

#include "fslib/fs/hdfs/HdfsFileSystem.h"
#include "fslib/util/LongIntervalLog.h"

using namespace std;
FSLIB_PLUGIN_BEGIN_NAMESPACE(hdfs);
AUTIL_DECLARE_AND_SETUP_LOGGER(hdfs, HdfsFile);

HdfsFile::HdfsFile(const string &path,
                   const string &dstPath,
                   HdfsConnectionPtr &connection,
                   hdfsFile file,
                   PanguMonitor *panguMonitor,
                   int64_t curPos,
                   ErrorCode ec)
    : File(path, ec)
    , _connection(connection)
    , _file(file)
    , _curPos(curPos)
    , _dstPath(dstPath)
    , _fileMonitor(new FileMonitor(panguMonitor, path)) {
    if (_connection != NULL) {
        AUTIL_LOG(DEBUG, "opened file %s with connection %p", _fileName.c_str(), _connection->getHdfsFs());
    } else {
        AUTIL_LOG(DEBUG, "opened file %s with connection NULL", _fileName.c_str());
    }

    if (!file) {
        _isEof = true;
    } else {
        _isEof = false;
    }
}

HdfsFile::~HdfsFile() {
    if (_file) {
        close();
    }
}

ssize_t HdfsFile::read(void *buffer, size_t length) {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s], length[%ld]", _fileName.c_str(), length);
    if (!_file) {
        AUTIL_LOG(ERROR, "fail to read hadoop file <%s> which is not opened", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }

    tSize actualLen = (tSize)length;
    if (actualLen < 0) {
        AUTIL_LOG(ERROR,
                  "fail to read hadoop file <%s>, length %zd "
                  "should not be larger than 2G",
                  _fileName.c_str(),
                  length);
        _lastErrorCode = EC_NOTSUP;
        return -1;
    }

    int64_t beginPos = _curPos;
    int32_t totalLen = 0;
    while (totalLen < actualLen) {
        auto beginTime = autil::TimeUtility::currentTime();
        int32_t ret = hdfsRead(_connection->getHdfsFs(), _file, (uint8_t *)buffer + totalLen, actualLen - totalLen);
        _fileMonitor->reportReadLatency(autil::TimeUtility::currentTime() - beginTime);
        if (ret < 0) {
            AUTIL_LOG(ERROR,
                      "fail to read hadoop file <%s> with address %p "
                      "from connection %p, %s",
                      _fileName.c_str(),
                      this,
                      _connection->getHdfsFs(),
                      strerror(errno));
            _lastErrorCode = HdfsFileSystem::convertErrno(errno);
            ret = hdfsSeek(_connection->getHdfsFs(), _file, beginPos);
            if (ret < 0) {
                AUTIL_LOG(
                    ERROR, "fail to seek hadoop file <%s> when read fail, %s", _fileName.c_str(), strerror(errno));
            }
            _connection->setError(true);
            return -1;
        } else if (ret == 0) {
            _isEof = true;
            break;
        }
        totalLen += ret;
        _fileMonitor->reportReadSize(ret);
    }
    _curPos += totalLen;
    return totalLen;
}

ssize_t HdfsFile::write(const void *buffer, size_t length) {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s], length[%ld]", _fileName.c_str(), length);
    if (!_file) {
        AUTIL_LOG(ERROR, "fail to write hadoop file <%s> which is not opened", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }

    tSize actualLen = (tSize)length;
    if (actualLen < 0) {
        AUTIL_LOG(ERROR,
                  "fail to write hadoop file <%s>, length %zd "
                  "should not be larger than 2G",
                  _fileName.c_str(),
                  length);
        _lastErrorCode = EC_NOTSUP;
        return -1;
    }
    int32_t totalLen = 0;
    while (totalLen < actualLen) {
        auto beginTime = autil::TimeUtility::currentTime();
        int32_t len = hdfsWrite(_connection->getHdfsFs(), _file, (uint8_t *)buffer + totalLen, actualLen - totalLen);
        _fileMonitor->reportWriteLatency(autil::TimeUtility::currentTime() - beginTime);
        if (len < 0) {
            AUTIL_LOG(ERROR,
                      "fail to write hadoop file <%s> with address %p"
                      " from connection %p, %s",
                      _fileName.c_str(),
                      this,
                      _connection->getHdfsFs(),
                      strerror(errno));
            _lastErrorCode = HdfsFileSystem::convertErrno(errno);
            _connection->setError(true);
            return -1;
        }
        totalLen += len;
        _fileMonitor->reportWriteSize(len);
    }
    _curPos += totalLen;
    return totalLen;
}

ssize_t HdfsFile::pread(void *buffer, size_t length, off_t offset) {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s], length[%ld]", _fileName.c_str(), length);
    if (!_file) {
        AUTIL_LOG(ERROR, "fail to pread hadoop file <%s> which is not opened", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }

    tSize actualLen = (tSize)length;
    if (actualLen < 0) {
        AUTIL_LOG(ERROR,
                  "fail to pread hadoop file <%s>, length %zd "
                  "should not be larger than 2G",
                  _fileName.c_str(),
                  length);
        _lastErrorCode = EC_NOTSUP;
        return -1;
    }

    int32_t totalLen = 0;
    while (totalLen < actualLen) {
        int64_t startOff = offset + totalLen;
        auto beginTime = autil::TimeUtility::currentTime();
        int32_t ret =
            hdfsPread(_connection->getHdfsFs(), _file, startOff, (uint8_t *)buffer + totalLen, actualLen - totalLen);
        _fileMonitor->reportReadLatency(autil::TimeUtility::currentTime() - beginTime);
        if (ret < 0) {
            AUTIL_LOG(ERROR, "fail to pread hadoop file <%s>, %s", _fileName.c_str(), strerror(errno));
            _lastErrorCode = HdfsFileSystem::convertErrno(errno);
            _connection->setError(true);
            return -1;
        } else if (ret == 0) {
            AUTIL_LOG(DEBUG, "pread hadoop file <%s> which has reached end", _fileName.c_str());
            return totalLen;
        }
        _fileMonitor->reportReadSize(ret);
        totalLen += ret;
    }

    return totalLen;
}

ssize_t HdfsFile::pwrite(const void *buffer, size_t length, off_t offset) {
    AUTIL_LOG(ERROR, "fail to pwrite hadoop file <%s>, operation not support.", _fileName.c_str());
    _lastErrorCode = EC_NOTSUP;
    return -1;
}

ErrorCode HdfsFile::flush() {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
    if (!_file) {
        AUTIL_LOG(ERROR, "fail to flush hadoop file <%s> which is not opened", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return _lastErrorCode;
    }

    int ret = hdfsHFlush(_connection->getHdfsFs(), _file);
    if (ret < 0) {
        AUTIL_LOG(ERROR, "fail to flush hadoop file <%s>, %s", _fileName.c_str(), strerror(errno));
        _lastErrorCode = HdfsFileSystem::convertErrno(errno);
        _connection->setError(true);
        return _lastErrorCode;
    }

    return EC_OK;
}

ErrorCode HdfsFile::sync() {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
    if (!_file) {
        AUTIL_LOG(ERROR, "fail to sync hadoop file <%s> which is not opened", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return _lastErrorCode;
    }

    int ret = hdfsHSync(_connection->getHdfsFs(), _file);
    if (ret < 0) {
        AUTIL_LOG(ERROR, "fail to sync hadoop file <%s>, %s", _fileName.c_str(), strerror(errno));
        _lastErrorCode = HdfsFileSystem::convertErrno(errno);
        _connection->setError(true);
        return _lastErrorCode;
    }

    return EC_OK;
}

ErrorCode HdfsFile::close() {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
    bool isSucceed = true;

    if (!_file) {
        AUTIL_LOG(ERROR, "fail to close hadoop file <%s> which is not opened", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return _lastErrorCode;
    }

    int ret = hdfsCloseFile(_connection->getHdfsFs(), _file);
    if (ret < 0) {
        AUTIL_LOG(ERROR, "fail to close hadoop file <%s>, %s", _fileName.c_str(), strerror(errno));
        _lastErrorCode = HdfsFileSystem::convertErrno(errno);
        isSucceed = false;
        _connection->setError(true);
    }
    _file = NULL;
    _isEof = true;
    _curPos = 0;
    if (isSucceed) {
        return EC_OK;
    } else {
        return _lastErrorCode;
    }
}

ErrorCode HdfsFile::seek(int64_t offset, SeekFlag flag) {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
    if (!_file) {
        AUTIL_LOG(ERROR, "fail to seek hadoop file <%s> which is not opened", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return _lastErrorCode;
    }

    int64_t fileLen = -1;
    // optimization: no need to access to hdfs when flag=FILE_SEEK_SET && offset=0
    if (flag != FILE_SEEK_SET || offset != 0) {
        fileLen = getFileLen();
        if (fileLen == -1) {
            AUTIL_LOG(ERROR,
                      "fail to seek hadoop file <%s>, cannot get fileLen."
                      "error: %s",
                      _fileName.c_str(),
                      strerror(errno));
            _lastErrorCode = HdfsFileSystem::convertErrno(errno);
            return _lastErrorCode;
        }
    }

    int64_t actualOffset = 0;
    switch (flag) {
    case FILE_SEEK_SET:
        actualOffset = offset;
        break;
    case FILE_SEEK_CUR: {
        int64_t currentOffset = hdfsTell(_connection->getHdfsFs(), _file);
        if (currentOffset < 0) {
            AUTIL_LOG(ERROR,
                      "fail to seek hadoop file <%s>, cannot get current "
                      "position, %s",
                      _fileName.c_str(),
                      strerror(errno));
            _lastErrorCode = HdfsFileSystem::convertErrno(errno);
            _connection->setError(true);
            return _lastErrorCode;
        }

        actualOffset = currentOffset + offset;
        break;
    }
    case FILE_SEEK_END: {
        actualOffset = fileLen + offset;
        break;
    }
    default:
        AUTIL_LOG(ERROR,
                  "fail to seek hadoop file <%s>. flag %d "
                  "does not support",
                  _fileName.c_str(),
                  flag);
        _lastErrorCode = EC_NOTSUP;
        return _lastErrorCode;
    }

    if (actualOffset < 0) {
        actualOffset = 0;
    } else if (offset != 0 && actualOffset > fileLen) {
        actualOffset = fileLen;
    }

    int ret = hdfsSeek(_connection->getHdfsFs(), _file, actualOffset);
    if (ret < 0) {
        AUTIL_LOG(ERROR, "fail to seek hadoop file <%s>, %s", _fileName.c_str(), strerror(errno));
        _lastErrorCode = HdfsFileSystem::convertErrno(errno);
        _connection->setError(true);
        return _lastErrorCode;
    }

    _curPos = actualOffset;
    _isEof = false;
    return EC_OK;
}

int64_t HdfsFile::tell() {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _fileName.c_str());
    if (!_file) {
        AUTIL_LOG(ERROR, "fail to tell hadoop file <%s> which is not opened", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }

    return _curPos;
}

bool HdfsFile::isOpened() const { return (_connection != NULL && _file != NULL); }

bool HdfsFile::isEof() { return _isEof; }

int64_t HdfsFile::getFileLen() {
    hdfsFileInfo *fileInfo = hdfsGetPathInfo(_connection->getHdfsFs(), _dstPath.c_str());
    if (!fileInfo) {
        return -1;
    }
    int64_t fileLen = fileInfo->mSize;
    hdfsFreeFileInfo(fileInfo, 1);
    AUTIL_LOG(DEBUG, "get hadoop file len: [%ld]", fileLen);
    return fileLen;
}
FSLIB_PLUGIN_END_NAMESPACE(fs);
