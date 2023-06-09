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
#include "fslib/fs/MemDataFile.h"

using namespace std;

FSLIB_BEGIN_NAMESPACE(fs);
AUTIL_DECLARE_AND_SETUP_LOGGER(fs, MemDataFile);

MemDataFile::MemDataFile(const string& filePath, const string& data)
    : File(filePath, EC_OK)
    , _filePath(filePath)
    , _fileData(data)
    , _cursor(0)
{}

MemDataFile::~MemDataFile() {}

ssize_t MemDataFile::read(void* buffer, size_t length)
{
    if (!isOpened()) {
        AUTIL_LOG(ERROR, "isEof fail, mem data for file [%s] has been closed!",
                  _filePath.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }

    if (isEof()) {
        return 0;
    }
    int64_t toReadLen = (_cursor + (int64_t)length) >= (int64_t)_fileData.size() ?
                        ((int64_t)_fileData.size() - _cursor) : (int64_t)length;
    memcpy(buffer, _fileData.c_str() + _cursor, toReadLen);
    _cursor += toReadLen;
    return toReadLen;    
}

ssize_t MemDataFile::pread(void* buffer, size_t length, off_t offset) {
    if (!isOpened()) {
        AUTIL_LOG(ERROR, "isEof fail, mem data for file [%s] has been closed!",
                  _filePath.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }
    int64_t toReadLen = (offset + (int64_t)length) >= (int64_t)_fileData.size() ?
                        ((int64_t)_fileData.size() - offset) : (int64_t)length;
    memcpy(buffer, _fileData.c_str() + offset, toReadLen);
    return toReadLen;    
}
    
ssize_t MemDataFile::write(const void* buffer, size_t length) {
    _lastErrorCode = EC_NOTSUP;
    return -1;
}

ssize_t MemDataFile::pwrite(const void* buffer, size_t length, off_t offset) {
    _lastErrorCode = EC_NOTSUP;
    return -1;
}

ErrorCode MemDataFile::flush() {
    _lastErrorCode = EC_NOTSUP;
    return EC_NOTSUP;
}

ErrorCode MemDataFile::close() {
    _cursor = -1;
    _fileData.clear();
    return EC_OK;
}

ErrorCode MemDataFile::seek(int64_t offset, SeekFlag flag) {
    if (!isOpened()) {
        AUTIL_LOG(ERROR, "isEof fail, mem data for file [%s] has been closed!",
                  _filePath.c_str());
        _lastErrorCode = EC_BADF;
        return EC_BADF;
    }
    
    int64_t targetOffset = -1;
    switch(flag) {
    case FILE_SEEK_SET:
        targetOffset = offset;
        break;
    case FILE_SEEK_CUR:
        targetOffset = _cursor + offset;
        break;
    case FILE_SEEK_END:
        targetOffset = (int64_t)_fileData.size() + offset;
        break;
    default:
        AUTIL_LOG(ERROR, "seek file %s fail, SeekFlag %d is not supported.", 
                  _filePath.c_str(), flag);
        _lastErrorCode = EC_NOTSUP;
        return _lastErrorCode;
    };

    if (targetOffset < 0 || targetOffset > (int64_t)_fileData.size()) {
        AUTIL_LOG(ERROR, "seek file %s fail with SeekFlag %d, invalid target offset [%ld]",
                  _filePath.c_str(), flag, targetOffset);
        _lastErrorCode = EC_BADARGS;
        return EC_BADARGS;
    }
    _cursor = targetOffset;
    return EC_OK;
}

int64_t MemDataFile::tell() {
    if (!isOpened()) {
        AUTIL_LOG(ERROR, "isEof fail, mem data for file [%s] has been closed!",
                  _filePath.c_str());
        _lastErrorCode = EC_BADF;
        return -1;
    }
    
    if (isEof()) {
        return _fileData.size();
    }
    return _cursor;
}

bool MemDataFile::isOpened() const {
    return _cursor >= 0;
}

bool MemDataFile::isEof() {
    if (!isOpened()) {
        AUTIL_LOG(ERROR, "isEof fail, mem data for file [%s] has been closed!",
                  _filePath.c_str());
        _lastErrorCode = EC_BADF;
        return true;
    }
    return _cursor >= (int64_t)_fileData.size();
}

FSLIB_END_NAMESPACE(fs);

