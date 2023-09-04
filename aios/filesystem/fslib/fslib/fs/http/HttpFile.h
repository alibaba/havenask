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
#ifndef FSLIB_PLUGIN_HTTPFILE_H
#define FSLIB_PLUGIN_HTTPFILE_H

#include <curl/curl.h>
#include <fslib/common.h>
#include <vector>

FSLIB_PLUGIN_BEGIN_NAMESPACE(http);

class HttpFile : public File {
public:
    HttpFile(const std::string &fileName, ErrorCode ec = EC_OK);
    ~HttpFile();

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

    bool initFileInfo();

    fslib::FileMeta getFileMeta() const { return _fileMeta; }

private:
    ssize_t preadWithLock(void *buffer, size_t length, off_t offset);

    ErrorCode seekWithLock(int64_t offset, SeekFlag flag);

    bool setFileMetaAfterPerf();

    bool checkRangeSupport();

    ssize_t preadByRange(void *buffer, size_t length, off_t offset);

    ssize_t preadByCache(void *buffer, size_t length, off_t offset);

public:
    /* public for test */
    bool getSupportRange() { return _supportRange; }

private:
    CURL *_curl;
    bool _isEof;
    bool _isOpened;
    std::string _url;
    int64_t _offset;
    fslib::FileMeta _fileMeta;
    char *_buf;
    bool _buffered;
    bool _supportRange;
    autil::ThreadMutex _mutex;
};

typedef std::shared_ptr<HttpFile> HttpFilePtr;

FSLIB_PLUGIN_END_NAMESPACE(http);

#endif // FSLIB_PLUGIN_HTTPFILE_H
