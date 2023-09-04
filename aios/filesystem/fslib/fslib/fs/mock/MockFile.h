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
#pragma once

#include <vector>

#include "fslib/fs/File.h"
#include "fslib/fs/mock/common.h"

FSLIB_PLUGIN_BEGIN_NAMESPACE(mock);

class MockFile : public File {
public:
    MockFile(File *file);
    ~MockFile();

public:
    ssize_t read(void *buffer, size_t length) override;
    ssize_t write(const void *buffer, size_t length) override;
    ssize_t pread(void *buffer, size_t length, off_t offset) override;
    ssize_t preadv(const iovec *iov, int iovcnt, off_t offset) override;
    // void pread(IOController* ctrl, void* buffer, size_t length, off_t offset,
    //            std::function<void()> callback) override;
    // void preadv(IOController* ctrl, const iovec* iov, int iovcnt, off_t offset,
    //             std::function<void()> callback) override;
    ssize_t pwrite(const void *buffer, size_t length, off_t offset) override;
    ErrorCode flush() override;
    ErrorCode sync() override;
    ErrorCode close() override;
    ErrorCode seek(int64_t offset, SeekFlag flag) override;
    int64_t tell() override;
    bool isOpened() const override;
    bool isEof() override;

private:
    File *_file;

private:
    friend class MockFileSystemTest;
};

typedef std::shared_ptr<MockFile> MockFilePtr;

FSLIB_PLUGIN_END_NAMESPACE(mock);
