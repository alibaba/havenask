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
#ifndef FSLIB_PLUGIN_OSSFILE_H
#define FSLIB_PLUGIN_OSSFILE_H

#include <fslib/common.h>
#include <oss_c_sdk/aos_http_io.h>
#include <oss_c_sdk/oss_api.h>
#include <vector>

FSLIB_PLUGIN_BEGIN_NAMESPACE(oss);

class OssFile : public File {
public:
    OssFile(const std::string &fileName,
            const std::string &bucket,
            const std::string &object,
            const std::string &accessKeyId,
            const std::string &accessKeySecret,
            const std::string &endpoint,
            Flag flag,
            ErrorCode ec = EC_OK);
    ~OssFile();

public:
    /*override*/ ssize_t read(void *buffer, size_t length);

    /*override*/ ssize_t write(const void *buffer, size_t length);

    /*override*/ ssize_t pread(void *buffer, size_t length, off_t offset);

    /*override*/ ssize_t pwrite(const void *buffer, size_t length, off_t offset);

    /*override*/ ErrorCode flush();

    /*override*/ ErrorCode close();

    /*override*/ ErrorCode seek(int64_t offset, SeekFlag flag);

    /*override*/ int64_t tell();

    /*override*/ bool isOpened() const;

    /*override*/ bool isEof();

    ErrorCode initFileInfo();

    fslib::FileMeta getFileMeta() const { return _fileMeta; }

    bool updateAppendPos();

    void setLastError(ErrorCode ec);

private:
    ssize_t preadWithLock(void *buffer, size_t length, off_t offset);

    ErrorCode seekWithLock(int64_t offset, SeekFlag flag);

    bool setFileMetaAfterPerf();

    bool checkRangeSupport();

    ssize_t preadByRange(void *buffer, size_t length, off_t offset);

    oss_request_options_t *getOssRequestOptions(aos_pool_t *pool);

    ErrorCode parseOssCode(int32_t httpCode, const std::string &errorCode);

private:
    bool _isEof;
    bool _isOpened;
    int64_t _offset; // FOR READ
    fslib::FileMeta _fileMeta;
    char *_buf;
    bool _buffered;
    autil::ThreadMutex _mutex;
    std::string _bucketStr;
    std::string _objectStr;
    aos_string_t _bucket;
    aos_string_t _object;
    int _isCname;
    std::string _fileName;
    std::string _accessKeyId;
    std::string _accessKeySecret;
    std::string _endpoint;
    bool _isAppendable;
    int64_t _writePos; // FOR WRITE
    int64_t _nextAppendPos;
    Flag _flag;
};

typedef std::shared_ptr<OssFile> OssFilePtr;

FSLIB_PLUGIN_END_NAMESPACE(oss);

#endif // FSLIB_PLUGIN_OSSFILE_H
