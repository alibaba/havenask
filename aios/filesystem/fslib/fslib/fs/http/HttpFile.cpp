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
#include "fslib/fs/http/HttpFile.h"

#include <sstream>

#include "autil/Lock.h"
#include "fslib/fs/http/HttpFileSystem.h"

using namespace std;
using namespace autil;

FSLIB_PLUGIN_BEGIN_NAMESPACE(http);
AUTIL_DECLARE_AND_SETUP_LOGGER(http, HttpFile);
HttpFile::HttpFile(const string &fileName, ErrorCode ec)
    : File(fileName, ec)
    , _isEof(true)
    , _isOpened(false)
    , _url(fileName)
    , _offset(0)
    , _buf(NULL)
    , _buffered(false)
    , _supportRange(false) {
    memset((void *)&_fileMeta, 0, sizeof(FileMeta));

    if (ec == EC_OK) {
        _curl = curl_easy_init();
        if (_curl) {
            CURLcode res;
            res = curl_easy_setopt(_curl, CURLOPT_URL, fileName.c_str());
            if (res == CURLE_OK) {
                _isEof = false;
                _isOpened = true;
            } else {
                AUTIL_LOG(ERROR, "error when open file: %s", curl_easy_strerror(res));
            }
        }
    }
}

HttpFile::~HttpFile() { close(); }

struct MemoryStruct {
    char *memory;
    size_t curSize;
    size_t maxSize;
};

static size_t throw_away(void *ptr, size_t size, size_t nmemb, void *data) {
    (void)ptr;
    (void)data;
    /* we are not interested in the headers itself,
       so we only return the size we would have saved ... */
    return (size_t)(size * nmemb);
}

bool HttpFile::initFileInfo() {
    if (!_curl) {
        AUTIL_LOG(ERROR, "no valid curl handle");
        return false;
    }
    bool exitStatus = true;
    CURLcode res;
    /* No download if the file */
    CALL_WITH_CHECK(curl_easy_setopt, _curl, CURLOPT_NOBODY, 1L);
    /* Ask for filetime */
    CALL_WITH_CHECK(curl_easy_setopt, _curl, CURLOPT_FILETIME, 1L);
    CALL_WITH_CHECK(curl_easy_setopt, _curl, CURLOPT_HEADERFUNCTION, throw_away);
    CALL_WITH_CHECK(curl_easy_setopt, _curl, CURLOPT_HEADER, 0L);

    CALL_WITH_CHECK(curl_easy_setopt, _curl, CURLOPT_TIMEOUT, 10L);
    CALL_WITH_CHECK(curl_easy_setopt, _curl, CURLOPT_CONNECTTIMEOUT, 5L);
    CALL_WITH_CHECK(curl_easy_setopt, _curl, CURLOPT_NOSIGNAL, 1L);

    res = curl_easy_perform(_curl);

    if (CURLE_OK == res) {
        if (!setFileMetaAfterPerf()) {
            AUTIL_LOG(WARN, "get fileMeta from http response header failed");
            _isEof = true;
            exitStatus = false;
        }
        if (!checkRangeSupport()) {
            AUTIL_LOG(WARN, "check server's support for range failed");
            exitStatus = false;
        }
    } else {
        AUTIL_LOG(ERROR, "error when try get file info : [%s]", curl_easy_strerror(res));
        exitStatus = false;
    }
    return exitStatus;
}

bool HttpFile::setFileMetaAfterPerf() {
    CURLcode res;
    long fileTime = -1;
    double fileSize = 0.0;
    res = curl_easy_getinfo(_curl, CURLINFO_FILETIME, &fileTime);
    if ((CURLE_OK == res) && (fileTime >= 0)) {
        _fileMeta.createTime = (DateTime)fileTime;
        _fileMeta.lastModifyTime = (DateTime)fileTime;
    } else {
        /* some http server don't support get file time */
        AUTIL_LOG(WARN, "get file create time failed : [%s], filetime: %ld", curl_easy_strerror(res), fileTime);
    }
    res = curl_easy_getinfo(_curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &fileSize);
    if ((CURLE_OK == res) && (fileSize > 0.0)) {
        _fileMeta.fileLength = (int64_t)fileSize;
    } else {
        AUTIL_LOG(WARN, "get file size failed: [%s], fileSize: %f", curl_easy_strerror(res), fileSize);
        return false;
    }
    return true;
}

bool HttpFile::checkRangeSupport() {
    CURLcode res;
    CALL_WITH_CHECK(curl_easy_setopt, _curl, CURLOPT_NOBODY, 1L);
    CALL_WITH_CHECK(curl_easy_setopt, _curl, CURLOPT_FILETIME, 1L);
    CALL_WITH_CHECK(curl_easy_setopt, _curl, CURLOPT_HEADERFUNCTION, throw_away);
    CALL_WITH_CHECK(curl_easy_setopt, _curl, CURLOPT_HEADER, 0L);
    CALL_WITH_CHECK(curl_easy_setopt, _curl, CURLOPT_RANGE, "0-0");
    res = curl_easy_perform(_curl);

    if (CURLE_OK == res) {
        double fileSize = 0.0;
        res = curl_easy_getinfo(_curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &fileSize);
        if ((CURLE_OK == res) && (fileSize > 0.0)) {
            if (fileSize == 1) {
                _supportRange = true;
            } else {
                _supportRange = false;
            }
            return true;
        } else {
            AUTIL_LOG(WARN, "get file size for check range support failed : [%s]", curl_easy_strerror(res));
            return false;
        }

    } else {
        AUTIL_LOG(ERROR, "can't get range support info from server: [%s]", curl_easy_strerror(res));
        return false;
    }
}

ssize_t HttpFile::read(void *buffer, size_t length) {
    ScopedLock lock(_mutex);
    if (_isEof) {
        AUTIL_LOG(INFO, "http file [%s] has reached end", _url.c_str());
        return 0;
    }
    if (!_isOpened) {
        AUTIL_LOG(WARN, "http file [%s] is not opened", _url.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }

    ssize_t ret = preadWithLock(buffer, length, _offset);
    if (EC_OK != seekWithLock(ret, FILE_SEEK_CUR)) {
        AUTIL_LOG(ERROR,
                  "move offset after read failed, curOffset: %ld, "
                  "readLength: %ld, fileLength: %ld",
                  _offset,
                  ret,
                  _fileMeta.fileLength);
        return -1;
    }
    return ret;
}

ssize_t HttpFile::write(const void *buffer, size_t length) {
    ScopedLock lock(_mutex);
    _lastErrorCode = EC_NOTSUP;
    return -1;
}

static size_t WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp) {
    size_t realsize = size * nmemb;
    MemoryStruct *mem = (MemoryStruct *)userp;
    if (realsize + mem->curSize > mem->maxSize) {
        AUTIL_LOG(ERROR,
                  "read content beyond buffer size, drop the content, "
                  "realsize:%ld, cursize:%ld, maxsize:%ld",
                  realsize,
                  mem->curSize,
                  mem->maxSize);
        realsize = mem->maxSize - mem->curSize;
    }

    if (mem->memory == NULL) {
        AUTIL_LOG(ERROR, "try write to a null buffer");
        return 0;
    }

    memcpy(&(mem->memory[mem->curSize]), contents, realsize);
    mem->curSize += realsize;

    return realsize;
}

ssize_t HttpFile::preadByRange(void *buffer, size_t length, off_t offset) {
    MemoryStruct mem;
    mem.memory = (char *)buffer;
    mem.curSize = 0;
    mem.maxSize = length;

    CALL_WITH_CHECK(curl_easy_setopt, _curl, CURLOPT_NOBODY, 0L);
    CALL_WITH_CHECK(curl_easy_setopt, _curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
    CALL_WITH_CHECK(curl_easy_setopt, _curl, CURLOPT_WRITEDATA, (void *)&mem);

    int64_t timeout = length / (1024 * 1024);
    if (timeout < 10) {
        timeout = 10;
    }
    CALL_WITH_CHECK(curl_easy_setopt, _curl, CURLOPT_TIMEOUT, timeout);
    stringstream ss;
    ss << offset << "-" << length + offset - 1;
    CALL_WITH_CHECK(curl_easy_setopt, _curl, CURLOPT_RANGE, ss.str().c_str());
    AUTIL_LOG(INFO, "read [%s] %s bytes", _url.c_str(), ss.str().c_str());
    CURLcode res = curl_easy_perform(_curl);
    if (res != CURLE_OK) {
        AUTIL_LOG(ERROR, "error when get data from server: [%s]", curl_easy_strerror(res));
        return -1;
    }
    return mem.curSize;
}

ssize_t HttpFile::preadByCache(void *buffer, size_t length, off_t offset) {
    if (!_buffered) {
        _buf = new char[_fileMeta.fileLength];
        memset(_buf, 0, _fileMeta.fileLength);
        ssize_t downloadSize = preadByRange(_buf, _fileMeta.fileLength, 0);
        if (downloadSize == -1 || downloadSize != _fileMeta.fileLength) {
            AUTIL_LOG(ERROR, "buffer file failed!, filesize is %lu but get %ld", _fileMeta.fileLength, downloadSize);
            return -1;
        }
        _buffered = true;
    }
    memcpy(buffer, _buf + offset, length);
    return length;
}

ssize_t HttpFile::preadWithLock(void *buffer, size_t length, off_t offset) {
    if (offset >= _fileMeta.fileLength) {
        AUTIL_LOG(INFO, "offset %ld is larger than file length %ld", offset, _fileMeta.fileLength);
        return 0;
    }
    if ((off_t)length + offset > _fileMeta.fileLength) {
        size_t realLength = _fileMeta.fileLength - offset;
        AUTIL_LOG(INFO, "adjust read length from %ld to %ld", length, realLength);
        length = realLength;
    }
    return (_supportRange) ? preadByRange(buffer, length, offset) : preadByCache(buffer, length, offset);
}

ssize_t HttpFile::pread(void *buffer, size_t length, off_t offset) {
    ScopedLock lock(_mutex);

    if (!_isOpened) {
        AUTIL_LOG(WARN, "http file [%s] is not opened", _url.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }

    return preadWithLock(buffer, length, offset);
}

ssize_t HttpFile::pwrite(const void *buffer, size_t length, off_t offset) {
    ScopedLock lock(_mutex);
    _lastErrorCode = EC_NOTSUP;
    return -1;
}

ErrorCode HttpFile::flush() {
    ScopedLock lock(_mutex);
    _lastErrorCode = EC_NOTSUP;
    return EC_NOTSUP;
}

ErrorCode HttpFile::close() {
    ScopedLock lock(_mutex);
    if (_isOpened && _curl) {
        curl_easy_cleanup(_curl);
        _isOpened = false;
        _isEof = true;
        _curl = NULL;
        if (_buf) {
            delete[] _buf;
            _buf = NULL;
        }
    }

    return EC_OK;
}

ErrorCode HttpFile::seek(int64_t offset, SeekFlag flag) {
    ScopedLock lock(_mutex);
    return seekWithLock(offset, flag);
}

ErrorCode HttpFile::seekWithLock(int64_t offset, SeekFlag flag) {
    int64_t tmpOffset = _offset;
    switch (flag) {
    case FILE_SEEK_SET:
        tmpOffset = offset;
        break;
    case FILE_SEEK_CUR:
        tmpOffset += offset;
        break;
    case FILE_SEEK_END:
        tmpOffset = _fileMeta.fileLength + offset;
        break;
    default:
        AUTIL_LOG(ERROR, "not exist flag: %d", flag);
        return EC_NOTSUP;
    }
    if (tmpOffset < 0 || tmpOffset > _fileMeta.fileLength) {
        AUTIL_LOG(ERROR, "try seek to invalid offset: %ld", tmpOffset);
        return EC_BADARGS;
    } else {
        _offset = tmpOffset;
        if (_offset == _fileMeta.fileLength) {
            _isEof = true;
        } else {
            _isEof = false;
        }
        return EC_OK;
    }
}

int64_t HttpFile::tell() { return _offset; }

bool HttpFile::isOpened() const { return _isOpened; }

bool HttpFile::isEof() { return _isEof; }

FSLIB_PLUGIN_END_NAMESPACE(http);
