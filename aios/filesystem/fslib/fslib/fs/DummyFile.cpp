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
#include "fslib/fs/DummyFile.h"

using namespace std;
FSLIB_BEGIN_NAMESPACE(fs);

string DummyFile::_dummyFileName = "";

DummyFile::DummyFile(ErrorCode ec) : File(_dummyFileName, ec) {}

DummyFile::~DummyFile() {}

ssize_t DummyFile::read(void *buffer, size_t length) {
    _lastErrorCode = EC_NOTSUP;
    return -1;
}

ssize_t DummyFile::write(const void *buffer, size_t length) {
    _lastErrorCode = EC_NOTSUP;
    return -1;
}

ssize_t DummyFile::pread(void *buffer, size_t length, off_t offset) {
    _lastErrorCode = EC_NOTSUP;
    return -1;
}

ssize_t DummyFile::pwrite(const void *buffer, size_t length, off_t offset) {
    _lastErrorCode = EC_NOTSUP;
    return -1;
}

ErrorCode DummyFile::flush() {
    _lastErrorCode = EC_NOTSUP;
    return EC_NOTSUP;
}

ErrorCode DummyFile::close() {
    _lastErrorCode = EC_NOTSUP;
    return EC_NOTSUP;
}

ErrorCode DummyFile::seek(int64_t offset, SeekFlag flag) {
    _lastErrorCode = EC_NOTSUP;
    return EC_NOTSUP;
}

int64_t DummyFile::tell() {
    _lastErrorCode = EC_NOTSUP;
    return -1;
}

bool DummyFile::isOpened() const { return false; }

bool DummyFile::isEof() {
    _lastErrorCode = EC_NOTSUP;
    return true;
}

FSLIB_END_NAMESPACE(fs);
