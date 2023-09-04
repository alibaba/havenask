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
#include "fslib/fs/MockFile.h"

#include "fslib/fs/ErrorGenerator.h"

using namespace std;
FSLIB_BEGIN_NAMESPACE(fs);
AUTIL_DECLARE_AND_SETUP_LOGGER(fs, MockFile);

MockFile::MockFile(File *file) : File(file->getFileName(), file->getLastError()), _file(file) {}

MockFile::~MockFile() {
    delete _file;
    _file = NULL;
}

#define GENERATE_ERROR(operate, filename)                                                                              \
    ErrorCode __ec = ErrorGenerator::getInstance()->generateFileError(operate, filename, _file->tell());               \
    if (__ec != EC_OK) {                                                                                               \
        _lastErrorCode = __ec;                                                                                         \
        return -1;                                                                                                     \
    }

#define GENERATE_ERROR_WTH_OFFSET(operate, filename, offset)                                                           \
    ErrorCode __ec = ErrorGenerator::getInstance()->generateFileError(operate, filename, offset);                      \
    if (__ec != EC_OK) {                                                                                               \
        _lastErrorCode = __ec;                                                                                         \
        return -1;                                                                                                     \
    }

#define GENERATE_ERRORCODE(operate, filename)                                                                          \
    ErrorCode __ec = ErrorGenerator::getInstance()->generateFileError(operate, filename, _file->tell());               \
    if (__ec != EC_OK) {                                                                                               \
        _lastErrorCode = __ec;                                                                                         \
        return __ec;                                                                                                   \
    }

ssize_t MockFile::read(void *buffer, size_t length) {
    GENERATE_ERROR(OPERATION_READ, _file->getFileName());
    ssize_t ret = _file->read(buffer, length);
    _lastErrorCode = _file->getLastError();
    return ret;
}

ssize_t MockFile::write(const void *buffer, size_t length) {
    GENERATE_ERROR(OPERATION_WRITE, _file->getFileName());
    ssize_t ret = _file->write(buffer, length);
    _lastErrorCode = _file->getLastError();
    return ret;
}

ssize_t MockFile::pread(void *buffer, size_t length, off_t offset) {
    GENERATE_ERROR_WTH_OFFSET(OPERATION_PREAD, _file->getFileName(), offset);
    ssize_t ret = _file->pread(buffer, length, offset);
    _lastErrorCode = _file->getLastError();
    return ret;
}

ssize_t MockFile::pwrite(const void *buffer, size_t length, off_t offset) {
    GENERATE_ERROR_WTH_OFFSET(OPERATION_PWRITE, _file->getFileName(), offset);
    ssize_t ret = _file->pwrite(buffer, length, offset);
    _lastErrorCode = _file->getLastError();
    return ret;
}

ErrorCode MockFile::flush() {
    GENERATE_ERRORCODE(OPERATION_PWRITE, _file->getFileName());
    ErrorCode ec = _file->flush();
    _lastErrorCode = _file->getLastError();
    return ec;
}

ErrorCode MockFile::close() {
    GENERATE_ERRORCODE(OPERATION_CLOSE, _file->getFileName());
    ErrorCode ec = _file->close();
    _lastErrorCode = _file->getLastError();
    return ec;
}

ErrorCode MockFile::seek(int64_t offset, SeekFlag flag) {
    GENERATE_ERRORCODE(OPERATION_SEEK, _file->getFileName());
    ErrorCode ec = _file->seek(offset, flag);
    _lastErrorCode = _file->getLastError();
    return ec;
}

int64_t MockFile::tell() {
    GENERATE_ERROR(OPERATION_TELL, _file->getFileName());
    int64_t ret = _file->tell();
    _lastErrorCode = _file->getLastError();
    return ret;
}

bool MockFile::isOpened() const { return _file->isOpened(); }

bool MockFile::isEof() { return _file->isEof(); }

FSLIB_END_NAMESPACE(fs);
