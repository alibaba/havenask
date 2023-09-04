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
#ifndef FSLIB_MMAPFILE_H
#define FSLIB_MMAPFILE_H

#include "autil/Log.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"

FSLIB_BEGIN_NAMESPACE(fs);

class MMapFile {
public:
    friend class MMapFileTest;

public:
    MMapFile(std::string fileName, int fd, char *base, int64_t length, int64_t pos, ErrorCode ec);
    virtual ~MMapFile();

public:
    char *getBaseAddress();

    int64_t getLength();

    ssize_t read(void *buffer, size_t length);

    ssize_t write(const void *buffer, size_t length);

    ssize_t pread(void *buffer, size_t length, off_t offset);

    ssize_t pwrite(const void *buffer, size_t length, off_t offset);

    ErrorCode flush();

    ErrorCode close();

    ErrorCode seek(int64_t offset, SeekFlag flag);

    int64_t tell();

    ErrorCode getLastError() const;

    bool isOpened() const;

    int getFd() const;

    const char *getFileName() const;

    virtual ErrorCode populate(bool lock, int64_t sliceSize, int64_t interval);

    void setDontDump();

private:
    ErrorCode unloadData();

protected:
    std::string _fileName;
    int _fd;
    int64_t _length;
    int64_t _pos;
    char *_base;
    ErrorCode _lastErrorCode;

    static const int64_t MUNMAP_SLICE_SIZE;
};

FSLIB_TYPEDEF_AUTO_PTR(MMapFile);

FSLIB_END_NAMESPACE(fs);

#endif // FSLIB_MMAPFILE_H
