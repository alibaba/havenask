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
#include "swift/broker/storage_dfs/FileWrapper.h"

#include <cstddef>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
namespace swift {
namespace storage {
AUTIL_LOG_SETUP(swift, FileWrapper);

FileWrapper::FileWrapper(const string &fileName, bool isAppending) {
    _fileName = fileName;
    _file = NULL;
    _lastAccessTime = TimeUtility::currentTime();
    _fileLength = -1;
    _isAppending = isAppending;
    _isBad = false;
}

FileWrapper::~FileWrapper() { close(); }
bool FileWrapper::open() {
    ScopedLock lock(_readMutex);
    return doOpen();
}
int64_t FileWrapper::pread(void *buffer, size_t len, int64_t offset, bool &succ) {
    ScopedLock lock(_readMutex);
    if (!_file) {
        if (!doOpen()) {
            succ = false;
            return -1;
        }
    }
    int64_t readLen = _file->pread(buffer, len, offset);
    _lastAccessTime = TimeUtility::currentTime();
    succ = true;
    return readLen;
}

int64_t FileWrapper::getFileLength() {
    if (_fileLength >= 0) {
        return _fileLength;
    }
    ScopedLock lock(_readMutex);
    return doGetFileLength();
}

bool FileWrapper::doOpen() {
    if (_file) {
        return true;
    }
    if (_isBad) {
        return false;
    }
    _file = fs::FileSystem::openFile(_fileName, fslib::READ, false);
    if (_file == NULL) {
        AUTIL_LOG(ERROR, "open file failed! fileName:[%s]", _fileName.c_str());
        _isBad = true;
        return false;
    }
    if (_file->getLastError() != fslib::EC_OK) {
        _isBad = true;
        return false;
    }
    _isBad = false;
    AUTIL_LOG(INFO, "open file [%s] success!", _fileName.c_str());
    if (_isAppending) {
        doGetFileLength();
    }
    return true;
}

int64_t FileWrapper::doGetFileLength() {
    if (_fileLength >= 0) {
        return _fileLength;
    }
    if (!_file) {
        if (!doOpen()) {
            return -1;
        }
    }
    fslib::ErrorCode ec = _file->seek(0, FILE_SEEK_END);
    _fileLength = _file->tell();
    AUTIL_LOG(INFO, "seek file [%s], file length [%ld], ec [%d] ", _fileName.c_str(), _fileLength, int(ec));
    return _fileLength;
}

void FileWrapper::close() {
    ScopedLock lock(_readMutex);
    if (_file) {
        _file->close();
        DELETE_AND_SET_NULL(_file);
        AUTIL_LOG(INFO, "close file [%s].", _fileName.c_str());
    }
}

} // namespace storage
} // namespace swift
