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

#include <map>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/util/Permission.h"

namespace swift {
namespace util {
class TimeoutChecker;
} // namespace util
} // namespace swift

namespace swift {
namespace util {

class PermissionCenter {
public:
    const static uint32_t DEFAULT_READ_LIMIT = 10;
    const static uint32_t DEFAULT_READ_FILE_LIMIT = 5;
    const static uint32_t DEFAULT_WRITE_LIMIT = 10;
    const static int64_t DEFAULT_MAX_WAIT_TIME = (int64_t)2 * 1000 * 1000; // 2s
public:
    PermissionCenter(uint32_t readFileThreadLimit = DEFAULT_READ_FILE_LIMIT,
                     int64_t maxWaitTime = DEFAULT_MAX_WAIT_TIME,
                     uint32_t readThreadLimit = DEFAULT_READ_LIMIT,
                     uint32_t writeThreadLimit = DEFAULT_WRITE_LIMIT);
    ~PermissionCenter();

private:
    PermissionCenter(const PermissionCenter &);
    PermissionCenter &operator=(const PermissionCenter &);

public:
    PermissionPtr apply(const PermissionKey &key, TimeoutChecker *timeout);

    bool incReadCount();
    void decReadCount();
    bool incWriteCount();
    void decWriteCount();
    bool incReadFileCount();
    void decReadFileCount();
    bool incReadFileCount(const std::string &fileName);
    void decReadFileCount(const std::string &fileName);

    bool incGetMaxIdCount();
    void decGetMaxIdCount();
    bool incGetMinIdCount();
    void decGetMinIdCount();

    uint32_t getReadLimit();
    uint32_t getWriteLimit();
    uint32_t getReadFileLimit();

    uint32_t getCurReadCount();
    uint32_t getCurWriteCount();
    uint32_t getCurReadFileCount();
    uint32_t getCurMaxIdCount();
    uint32_t getCurMinIdCount();
    uint32_t getActualReadFileThreadCount();
    uint32_t getActualReadFileCount();

public:
    /*
     * only for Permission call back
     */
    void release(Permission *permission);

private:
    bool isTooLongForPermission(const PermissionPtr &permission, int64_t curTime);
    std::pair<bool, PermissionPtr> findAndInsertIfNotExist(const PermissionKey &key);

private:
    static const int64_t CHECK_PERMISSION_INTERVAL = 1000 * 40; // 40 ms
private:
    typedef std::map<PermissionKey, PermissionPtr> PermissionMap;

private:
    PermissionMap _permissionMap;
    autil::ThreadMutex _readMutex;
    autil::ThreadMutex _writeMutex;
    autil::ThreadMutex _readFileMutex;
    std::map<std::string, int32_t> _fileNames;
    uint32_t _readLimit;
    uint32_t _writeLimit;
    uint32_t _readFileLimit;
    int64_t _maxWaitTime;
    uint32_t _curReadCount;
    uint32_t _curWriteCount;
    uint32_t _curReadFileCount;
    uint32_t _curMaxIdCount;
    uint32_t _curMinIdCount;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace util
} // namespace swift
