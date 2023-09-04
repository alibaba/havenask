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

#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/common/Common.h"

namespace fslib {
namespace fs {
class File;
} // namespace fs
} // namespace fslib

namespace swift {
namespace storage {

class FileWrapper {
public:
    FileWrapper(const std::string &fileName, bool isAppending = false);
    ~FileWrapper();

private:
    FileWrapper(const FileWrapper &);
    FileWrapper &operator=(const FileWrapper &);

public:
    bool open();
    int64_t pread(void *buffer, size_t len, int64_t offset, bool &succ);
    void close();
    int64_t getLastAccessTime() { return _lastAccessTime; }
    void setBad(bool flag) { _isBad = flag; }
    bool isBad() { return _isBad; }
    bool isAppending() { return _isAppending; }
    int64_t getFileLength();

private:
    int64_t doGetFileLength();
    bool doOpen();

private:
    std::string _fileName;
    fslib::fs::File *_file;
    autil::ThreadMutex _readMutex;
    int64_t _lastAccessTime;
    volatile int64_t _fileLength;
    bool _isAppending;
    bool _isBad;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FileWrapper);

} // namespace storage
} // namespace swift
