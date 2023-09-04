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
#include "swift/util/PermissionCenter.h"

#include <assert.h>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <unistd.h>

#include "autil/TimeUtility.h"
#include "swift/util/TimeoutChecker.h"

using namespace std;
using namespace autil;

namespace swift {
namespace util {
AUTIL_LOG_SETUP(swift, PermissionCenter);

PermissionCenter::PermissionCenter(uint32_t readFileThreadLimit,
                                   int64_t maxWaitTime,
                                   uint32_t readThreadLimit,
                                   uint32_t writeThreadLimit) {
    _readLimit = readThreadLimit;
    _writeLimit = writeThreadLimit;
    _readFileLimit = readFileThreadLimit;
    _maxWaitTime = maxWaitTime;
    _curReadCount = 0;
    _curWriteCount = 0;
    _curMaxIdCount = 0;
    _curMinIdCount = 0;
    _curReadFileCount = 0;
}

PermissionCenter::~PermissionCenter() { _permissionMap.clear(); }

PermissionPtr PermissionCenter::apply(const PermissionKey &key, TimeoutChecker *timeout) {
    while (true) {
        if (timeout && timeout->checkTimeout()) {
            AUTIL_LOG(WARN,
                      "read file [%s] offset [%ld] timeout,"
                      " begin time [%ld], expired time [%ld].",
                      key.fileName.c_str(),
                      key.blockId,
                      timeout->getBeginTime(),
                      timeout->getExpireTimeout());
            return PermissionPtr();
        }
        pair<bool, PermissionPtr> ret = findAndInsertIfNotExist(key);
        // return if not in map.
        if (!ret.first && ret.second) {
            return ret.second;
        }
        if (ret.second) {
            int64_t curTime = TimeUtility::currentTime();
            if (isTooLongForPermission(ret.second, curTime)) {
                AUTIL_LOG(WARN,
                          "read file [%s] offset [%ld] too long. wait [%ld] us",
                          key.fileName.c_str(),
                          key.blockId,
                          curTime - ret.second->getStartTime());
                return PermissionPtr();
            }
            ret.second.reset();
        }
        usleep(CHECK_PERMISSION_INTERVAL);
    }
}

bool PermissionCenter::incReadCount() {
    ScopedLock lock(_readMutex);
    if (_curReadCount + _curMaxIdCount + _curMinIdCount >= _readLimit) {
        return false;
    } else {
        _curReadCount++;
        return true;
    }
}

void PermissionCenter::decReadCount() {
    ScopedLock lock(_readMutex);
    assert(_curReadCount > 0);
    _curReadCount--;
}

bool PermissionCenter::incWriteCount() {
    ScopedLock lock(_writeMutex);
    if (_curWriteCount >= _writeLimit) {
        return false;
    } else {
        _curWriteCount++;
        return true;
    }
}

void PermissionCenter::decWriteCount() {
    ScopedLock lock(_writeMutex);
    assert(_curWriteCount > 0);
    _curWriteCount--;
}

bool PermissionCenter::incReadFileCount() {
    ScopedLock lock(_readFileMutex);
    if (_curReadFileCount >= _readFileLimit) {
        return false;
    } else {
        _curReadFileCount++;
        return true;
    }
}

void PermissionCenter::decReadFileCount() {
    ScopedLock lock(_readFileMutex);
    assert(_curReadFileCount > 0);
    _curReadFileCount--;
}

bool PermissionCenter::incReadFileCount(const std::string &fileName) {
    ScopedLock lock(_readFileMutex);
    if (_fileNames.size() >= _readFileLimit) {
        return false;
    } else {
        _fileNames[fileName]++;
        return true;
    }
}

void PermissionCenter::decReadFileCount(const std::string &fileName) {
    ScopedLock lock(_readFileMutex);
    map<std::string, int32_t>::iterator iter = _fileNames.find(fileName);
    if (iter != _fileNames.end()) {
        if (--iter->second <= 0) {
            _fileNames.erase(iter);
        }
    }
}

bool PermissionCenter::incGetMaxIdCount() {
    ScopedLock lock(_readMutex);
    if (_curReadCount + _curMaxIdCount + _curMinIdCount >= _readLimit) {
        return false;
    } else {
        _curMaxIdCount++;
        return true;
    }
}

void PermissionCenter::decGetMaxIdCount() {
    ScopedLock lock(_readMutex);
    assert(_curMaxIdCount > 0);
    _curMaxIdCount--;
}

bool PermissionCenter::incGetMinIdCount() {
    ScopedLock lock(_readMutex);
    if (_curReadCount + _curMaxIdCount + _curMinIdCount >= _readLimit) {
        return false;
    } else {
        _curMinIdCount++;
        return true;
    }
}

void PermissionCenter::decGetMinIdCount() {
    ScopedLock lock(_readMutex);
    assert(_curMinIdCount > 0);
    _curMinIdCount--;
}

uint32_t PermissionCenter::getReadLimit() { return _readLimit; }

uint32_t PermissionCenter::getWriteLimit() { return _writeLimit; }

uint32_t PermissionCenter::getReadFileLimit() { return _readFileLimit; }

uint32_t PermissionCenter::getCurReadCount() {
    ScopedLock lock(_readMutex);
    return _curReadCount;
}

uint32_t PermissionCenter::getCurWriteCount() {
    ScopedLock lock(_writeMutex);
    return _curWriteCount;
}

uint32_t PermissionCenter::getCurReadFileCount() {
    ScopedLock lock(_readFileMutex);
    return _curReadFileCount;
}

uint32_t PermissionCenter::getCurMaxIdCount() {
    ScopedLock lock(_readMutex);
    return _curMaxIdCount;
}

uint32_t PermissionCenter::getCurMinIdCount() {
    ScopedLock lock(_readMutex);
    return _curMinIdCount;
}

uint32_t PermissionCenter::getActualReadFileThreadCount() {
    ScopedLock lock(_readFileMutex);
    return _permissionMap.size();
}

uint32_t PermissionCenter::getActualReadFileCount() {
    ScopedLock lock(_readFileMutex);
    return _fileNames.size();
}

bool PermissionCenter::isTooLongForPermission(const PermissionPtr &permission, int64_t curTime) {
    return curTime - permission->getStartTime() >= _maxWaitTime;
}

pair<bool, PermissionPtr> PermissionCenter::findAndInsertIfNotExist(const PermissionKey &key) {
    ScopedLock lock(_readFileMutex);
    PermissionMap::const_iterator it = _permissionMap.find(key);
    if (it != _permissionMap.end()) {
        return make_pair(true, it->second);
    }
    if (_permissionMap.size() >= _readFileLimit) {
        return make_pair(false, PermissionPtr());
    }
    // create
    PermissionPtr permission(new Permission(key, this));
    _permissionMap[key] = permission;
    return make_pair(false, permission);
}

void PermissionCenter::release(Permission *permission) {
    const PermissionKey &key = permission->getPermissionKey();
    ScopedLock lock(_readFileMutex);
    assert(_permissionMap.find(key) != _permissionMap.end());
    _permissionMap.erase(key);
}

} // namespace util
} // namespace swift
