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
#include "fslib/fs/oss/OssFile.h"

#include <sstream>

#include "apr-1/apr_date.h"
#include "autil/Lock.h"
#include "autil/StringUtil.h"
#include "fslib/fs/oss/OssFileSystem.h"

using namespace std;
using namespace autil;

FSLIB_PLUGIN_BEGIN_NAMESPACE(oss);
AUTIL_DECLARE_AND_SETUP_LOGGER(oss, OssFile);

OssFile::OssFile(const string &fileName,
                 const string &bucket,
                 const string &object,
                 const string &accessKeyId,
                 const string &accessKeySecret,
                 const string &endpoint,
                 Flag flag,
                 ErrorCode ec)
    : File(fileName, ec)
    , _isEof(true)
    , _isOpened(false)
    , _offset(0)
    , _buf(NULL)
    , _buffered(false)
    , _isAppendable(false)
    , _writePos(0)
    , _nextAppendPos(0)
    , _flag(flag) {
    memset((void *)&_fileMeta, 0, sizeof(FileMeta));
    _bucketStr = bucket;
    _objectStr = object;
    aos_str_set(&_bucket, _bucketStr.c_str());
    aos_str_set(&_object, _objectStr.c_str());
    _fileName = fileName;
    if (ec != EC_OK) {
        return;
    }
    _isEof = false;
    _isCname = 0; // not used at current
    _accessKeyId = accessKeyId;
    _accessKeySecret = accessKeySecret;
    _endpoint = endpoint;
    _isOpened = true;
}

OssFile::~OssFile() { close(); }

oss_request_options_t *OssFile::getOssRequestOptions(aos_pool_t *pool) {
    oss_request_options_t *options = oss_request_options_create(pool);
    options->config = oss_config_create(options->pool);
    aos_str_set(&options->config->access_key_id, _accessKeyId.c_str());
    aos_str_set(&options->config->access_key_secret, _accessKeySecret.c_str());
    aos_str_set(&options->config->endpoint, _endpoint.c_str());
    options->config->is_cname = _isCname;
    options->ctl = aos_http_controller_create(options->pool, 0);
    return options;
}

struct MemoryStruct {
    char *memory;
    size_t curSize;
    size_t maxSize;
};

ErrorCode OssFile::parseOssCode(int32_t httpCode, const string &errorCode) {
    if (httpCode >= 200 && httpCode < 300) {
        return EC_OK;
    }
    if (httpCode == 404) {
        return EC_NOENT;
    }
    if (httpCode == 400) {
        if (errorCode == "InvalidArgument" || errorCode == "InvalidBucketName" || errorCode == "InvalidObjectName" ||
            errorCode == "InvalidTargetBucketForLogging" || errorCode == "InvalidPart" ||
            errorCode == "InvalidPartOrder") {
            return EC_BADARGS;
        }
        return EC_UNKNOWN;
    }
    return EC_UNKNOWN;
}

ErrorCode OssFile::initFileInfo() {
    aos_pool_t *pool = NULL;
    aos_pool_create(&pool, NULL);
    aos_table_t *headers = aos_table_make(pool, 0);
    aos_table_t *head_resp_headers = NULL;
    oss_request_options_t *options = getOssRequestOptions(pool);
    aos_status_t *s = oss_head_object(options, &_bucket, &_object, headers, &head_resp_headers);
    if (!aos_status_is_ok(s)) {
        AUTIL_LOG(DEBUG,
                  "get oss head object failed, http_code[%d], error_code[%s], error_msg[%s].",
                  s->code,
                  s->error_code,
                  s->error_msg);
        int32_t httpCode = s->code;
        string errorCode = s->error_code;
        _isEof = true;
        aos_pool_destroy(pool);
        _lastErrorCode = parseOssCode(httpCode, errorCode);
        return _lastErrorCode;
    }
    const char *object_last_modified = apr_table_get(head_resp_headers, "Last-Modified");
    const char *object_etag = apr_table_get(head_resp_headers, "ETag");
    const char *object_size_str = apr_table_get(head_resp_headers, OSS_CONTENT_LENGTH);
    const char *object_type = apr_table_get(head_resp_headers, OSS_OBJECT_TYPE);
    const char *append_position = apr_table_get(head_resp_headers, OSS_NEXT_APPEND_POSITION);

    if (!object_last_modified || !object_etag || !object_size_str) {
        // Invalid http response header
        s = aos_status_create(options->pool);
        aos_status_set(s, AOSE_INTERNAL_ERROR, AOS_SERVER_ERROR_CODE, "Unexpected response header");
        AUTIL_LOG(ERROR, "Unexpected aos response header.");
        aos_pool_destroy(pool);
        _lastErrorCode = EC_UNKNOWN;
        return EC_UNKNOWN;
    }

    if (0 == strncmp(OSS_OBJECT_TYPE_APPENDABLE, object_type, strlen(OSS_OBJECT_TYPE_APPENDABLE))) {
        _isAppendable = true;
        _nextAppendPos = aos_atoi64(append_position);
    }

    _fileMeta.lastModifyTime = apr_date_parse_rfc(object_last_modified) / 1000000;
    AUTIL_LOG(DEBUG, "%s,%lud", object_last_modified, _fileMeta.lastModifyTime);
    _fileMeta.fileLength = aos_atoi64(object_size_str);
    aos_pool_destroy(pool);
    return EC_OK;
}

ssize_t OssFile::read(void *buffer, size_t length) {
    ScopedLock lock(_mutex);
    if (_isEof) {
        AUTIL_LOG(INFO, "oss file [%s] has reached end", _fileName.c_str());
        return 0;
    }
    if (!_isOpened) {
        AUTIL_LOG(WARN, "oss file [%s] is not opened", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }

    if (_flag != READ) {
        AUTIL_LOG(ERROR, "read file %s fail, file is not opened in read mode", _fileName.c_str());
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

ssize_t OssFile::write(const void *buffer, size_t length) {
    ScopedLock lock(_mutex);
    if (!_isOpened) {
        AUTIL_LOG(WARN, "oss file [%s] is not opened", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }

    if (_flag == READ) {
        AUTIL_LOG(ERROR, "write file %s fail, file is opened in read mode", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }

    aos_pool_t *pool = NULL;
    aos_pool_create(&pool, NULL);

    aos_list_t aos_buffer;
    aos_buf_t *content = NULL;
    char *str = (char *)buffer;
    aos_table_t *headers = aos_table_make(pool, 0);
    aos_table_t *resp_headers = NULL;
    oss_request_options_t *options = getOssRequestOptions(pool);

    aos_list_init(&aos_buffer);
    content = aos_buf_pack(pool, str, length);
    aos_list_add_tail(&content->node, &aos_buffer);
    aos_status_t *s =
        oss_append_object_from_buffer(options, &_bucket, &_object, _writePos, &aos_buffer, headers, &resp_headers);

    if (!aos_status_is_ok(s)) {
        AUTIL_LOG(ERROR,
                  "write file[%s] fail. "
                  "http_code[%d], error_code[%s], error_msg[%s].",
                  _fileName.c_str(),
                  s->code,
                  s->error_code,
                  s->error_msg);
        aos_pool_destroy(pool);
        return -1;
    }

    const char *append_position = apr_table_get(resp_headers, OSS_NEXT_APPEND_POSITION);
    if (append_position) {
        _nextAppendPos = aos_atoi64(append_position);
        _writePos = _nextAppendPos;
    } else {
        AUTIL_LOG(WARN, "get file[%s] append position failed", _fileName.c_str());
    }

    aos_pool_destroy(pool);
    return length;
}

ssize_t OssFile::preadByRange(void *buffer, size_t length, off_t offset) {
    MemoryStruct mem;
    mem.memory = (char *)buffer;
    mem.curSize = 0;
    mem.maxSize = length;

    aos_pool_t *pool = NULL;
    aos_pool_create(&pool, NULL);

    oss_request_options_t *options = getOssRequestOptions(pool);

    // calucate request batchNum and batchLength
    size_t maxBatchLen = 1073741824; // 1G
    int batchNum = ceil((double(length) / maxBatchLen));
    vector<size_t> batchLengths(batchNum - 1, maxBatchLen);
    batchLengths.push_back(length - (batchNum - 1) * maxBatchLen);

    off_t batchOffset = offset;
    int64_t pos = 0;
    for (size_t batchLength : batchLengths) {
        aos_table_t *headers = aos_table_make(pool, 1);
        aos_table_t *respHeaders = NULL;
        aos_list_t aosBuffer;
        aos_list_init(&aosBuffer);
        stringstream ss;
        ss << "bytes=" << batchOffset << "-" << batchLength + batchOffset - 1;
        apr_table_set(headers, "Range", ss.str().c_str());
        AUTIL_LOG(INFO, "read [%s] %s.", _fileName.c_str(), ss.str().c_str());
        aos_status_t *s =
            oss_get_object_to_buffer(options, &_bucket, &_object, headers, NULL, &aosBuffer, &respHeaders);
        if (!aos_status_is_ok(s)) {
            AUTIL_LOG(ERROR,
                      "error when get object to buffer, http_code[%d], error_code[%s] error_msg[%s].",
                      s->code,
                      s->error_code,
                      s->error_msg);
            aos_pool_destroy(pool);
            return -1;
        }

        // copy from aosBuffer to buffer
        aos_buf_t *content = NULL;
        int64_t size = 0;
        aos_list_for_each_entry(aos_buf_t, content, &aosBuffer, node) {
            size = aos_buf_size(content);
            memcpy((char *)buffer + pos, content->pos, (size_t)size);
            mem.curSize += size;
            pos += size;
        }
        batchOffset += batchLength;
    }

    aos_pool_destroy(pool);
    return mem.curSize;
}

ssize_t OssFile::preadWithLock(void *buffer, size_t length, off_t offset) {
    if (offset >= _fileMeta.fileLength) {
        AUTIL_LOG(INFO, "offset %ld is larger than file length %ld", offset, _fileMeta.fileLength);
        return 0;
    }
    if ((off_t)length + offset > _fileMeta.fileLength) {
        size_t realLength = _fileMeta.fileLength - offset;
        AUTIL_LOG(INFO, "adjust read length from %ld to %ld", length, realLength);
        length = realLength;
    }
    return preadByRange(buffer, length, offset);
}

ssize_t OssFile::pread(void *buffer, size_t length, off_t offset) {
    ScopedLock lock(_mutex);

    if (!_isOpened) {
        AUTIL_LOG(WARN, "oss file [%s] is not opened", _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }

    if (_flag != READ) {
        AUTIL_LOG(ERROR,
                  "pread file %s fail, file is not opened in read"
                  " mode",
                  _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }

    return preadWithLock(buffer, length, offset);
}

ssize_t OssFile::pwrite(const void *buffer, size_t length, off_t offset) {
    ScopedLock lock(_mutex);
    _lastErrorCode = EC_NOTSUP;
    return -1;
}

ErrorCode OssFile::flush() {
    ScopedLock lock(_mutex);
    if (!_isOpened) {
        AUTIL_LOG(ERROR,
                  "flush file %s fail, can not flush file"
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

ErrorCode OssFile::close() {
    ScopedLock lock(_mutex);
    if (_isOpened) {
        _isOpened = false;
        _isEof = true;
        if (_buf) {
            delete[] _buf;
            _buf = NULL;
        }
    }
    return EC_OK;
}

ErrorCode OssFile::seek(int64_t offset, SeekFlag flag) {
    ScopedLock lock(_mutex);
    if (_flag != READ) {
        AUTIL_LOG(ERROR,
                  "seek file %s fail, only support seek file "
                  "opened in read mode!",
                  _fileName.c_str());
        _lastErrorCode = EC_BADF;
        return EC_BADF;
    }

    return seekWithLock(offset, flag);
}

ErrorCode OssFile::seekWithLock(int64_t offset, SeekFlag flag) {
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

int64_t OssFile::tell() { return _offset; }

bool OssFile::isOpened() const { return _isOpened; }

bool OssFile::isEof() { return _isEof; }

bool OssFile::updateAppendPos() {
    initFileInfo();
    if (!_isAppendable) {
        return false;
    }
    _writePos = _nextAppendPos;
    return true;
}

void OssFile::setLastError(ErrorCode ec) { _lastErrorCode = ec; }

FSLIB_PLUGIN_END_NAMESPACE(oss);
