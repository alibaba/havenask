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
#ifndef FSLIB_LOCALDIRECTFILE_H
#define FSLIB_LOCALDIRECTFILE_H

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "autil/Log.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"

FSLIB_BEGIN_NAMESPACE(fs);

class LocalDirectFile : public File {
public:
    using File::pread;
    using File::preadv;
    using File::pwrite;
    using File::pwritev;

public:
    LocalDirectFile(const std::string &fileName, int fd, ErrorCode ec = EC_OK);
    ~LocalDirectFile();

public:
    /*override*/ ssize_t read(void *buffer, size_t length) override;

    /*override*/ ssize_t write(const void *buffer, size_t length) override;

    /*override*/ ssize_t pread(void *buffer, size_t length, off_t offset) override;

    /*override*/ ssize_t pwrite(const void *buffer, size_t length, off_t offset) override;

#if (__cplusplus >= 201703L)
    void
    pread(IOController *controller, void *buffer, size_t length, off_t offset, std::function<void()> callback) override;
    void preadv(
        IOController *controller, const iovec *iov, int iovcnt, off_t offset, std::function<void()> callback) override;
    void pwrite(
        IOController *controller, void *buffer, size_t length, off_t offset, std::function<void()> callback) override;
    void pwritev(
        IOController *controller, const iovec *iov, int iovcnt, off_t offset, std::function<void()> callback) override;
#endif
    /*override*/ ErrorCode flush() override;

    /*override*/ ErrorCode sync() override;

    /*override*/ ErrorCode close() override;

    /*override*/ ErrorCode seek(int64_t offset, SeekFlag flag) override;

    /*override*/ int64_t tell() override;

    /*override*/ bool isOpened() const override;

    /*override*/ bool isEof() override;

private:
    ssize_t removeDirectIO();
    bool isAppendMode();
    ssize_t doWrite(const void *buffer, size_t length);

private:
    int _fd;
    bool _isEof;
    size_t _minAlignment;
    static size_t _alignment;
    static bool _asyncCoroRead;
};

FSLIB_TYPEDEF_AUTO_PTR(LocalDirectFile);

FSLIB_END_NAMESPACE(fs);

#endif // FSLIB_LOCALDIRECTFILE_H
