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
#ifndef FSLIB_PLUGIN_ZOOKEEPERFILE_H
#define FSLIB_PLUGIN_ZOOKEEPERFILE_H

#include <zookeeper/zookeeper.h>

#include "autil/Log.h"
#include "fslib/common.h"

FSLIB_PLUGIN_BEGIN_NAMESPACE(zookeeper);

class ZookeeperFile : public File {
public:
    ZookeeperFile(zhandle_t *zh,
                  const std::string &server,
                  std::string &path,
                  Flag flag,
                  int64_t fileSize,
                  int8_t retryCnt,
                  ErrorCode ec = EC_OK);

    ~ZookeeperFile();

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

public:
    // for test
    void setLastErrorCode(ErrorCode ec) { _lastErrorCode = ec; }

private:
    zhandle_t *_zh;
    const std::string _server;
    char *_buffer;
    int _size;
    int _curPos;
    int8_t _retryCnt;
    bool _isEof;
    Flag _flag;
};

typedef std::unique_ptr<ZookeeperFile> ZookeeperFilePtr;

FSLIB_PLUGIN_END_NAMESPACE(zookeeper);

#endif // FSLIB_PLUGIN_ZOOKEEPERFILE_H
