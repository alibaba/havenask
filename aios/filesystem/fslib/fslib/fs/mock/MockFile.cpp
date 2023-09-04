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
#include "fslib/fs/mock/MockFile.h"

#include "fslib/util/LongIntervalLog.h"

using namespace std;
using namespace autil;
using namespace fslib;

FSLIB_PLUGIN_BEGIN_NAMESPACE(mock);
AUTIL_DECLARE_AND_SETUP_LOGGER(mock, MockFile);

MockFile::MockFile(File *file) : File(file->getFileName(), file->getLastError()) { _file = file; }

MockFile::~MockFile() {
    if (_file) {
        delete _file;
        _file = NULL;
    }
}

ssize_t MockFile::read(void *buffer, size_t length) { return _file->read(buffer, length); }

ssize_t MockFile::write(const void *buffer, size_t length) { return _file->write(buffer, length); }

ssize_t MockFile::pread(void *buffer, size_t length, off_t offset) { return _file->pread(buffer, length, offset); }

ssize_t MockFile::preadv(const iovec *iov, int iovcnt, off_t offset) { return _file->preadv(iov, iovcnt, offset); }

ssize_t MockFile::pwrite(const void *buffer, size_t length, off_t offset) {
    return _file->pwrite(buffer, length, offset);
}

ErrorCode MockFile::flush() { return _file->flush(); }

ErrorCode MockFile::sync() { return _file->sync(); }

ErrorCode MockFile::close() { return _file->close(); }

ErrorCode MockFile::seek(int64_t offset, SeekFlag flag) { return _file->seek(offset, flag); }

int64_t MockFile::tell() { return _file->tell(); }

bool MockFile::isOpened() const { return _file->isOpened(); }

bool MockFile::isEof() { return _file->isEof(); }

FSLIB_PLUGIN_END_NAMESPACE(mock);
