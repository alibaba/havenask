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

#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "swift/common/Common.h"

namespace swift {
namespace util {

struct PermissionKey {
    PermissionKey(const std::string &name = "", int64_t id = 0) : fileName(name), blockId(id) {}
    ~PermissionKey() {}

    std::string fileName;
    int64_t blockId;
};

inline bool operator<(const PermissionKey &lft, const PermissionKey &rht) {
    if (lft.fileName != rht.fileName) {
        return lft.fileName < rht.fileName;
    }
    return lft.blockId < rht.blockId;
}

class PermissionCenter;

class Permission {
public:
    Permission(const PermissionKey &key, PermissionCenter *center);
    ~Permission();

private:
    Permission(const Permission &);
    Permission &operator=(const Permission &);

public:
    const PermissionKey &getPermissionKey() const { return _key; }
    int64_t getStartTime() const { return _startTime; }
    void release();

private:
    PermissionKey _key;
    int64_t _startTime;
    PermissionCenter *_center;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(Permission);

class ScopedPermission {
public:
    ScopedPermission(PermissionCenter *permissionCenter, PermissionPtr permission) {
        _permissionCenter = permissionCenter;
        _permission = permission;
    }
    ~ScopedPermission();

private:
    ScopedPermission(const ScopedPermission &);
    ScopedPermission &operator=(const ScopedPermission &);

private:
    PermissionCenter *_permissionCenter;
    PermissionPtr _permission;
};

class ScopedReadPermission {
public:
    ScopedReadPermission(PermissionCenter *permissionCenter) { _permissionCenter = permissionCenter; }
    ~ScopedReadPermission();

private:
    ScopedReadPermission(const ScopedReadPermission &);
    ScopedReadPermission &operator=(const ScopedReadPermission &);

private:
    PermissionCenter *_permissionCenter;
};

class ScopedWritePermission {
public:
    ScopedWritePermission(PermissionCenter *permissionCenter) { _permissionCenter = permissionCenter; }
    ~ScopedWritePermission();

private:
    ScopedWritePermission(const ScopedWritePermission &);
    ScopedWritePermission &operator=(const ScopedWritePermission &);

private:
    PermissionCenter *_permissionCenter;
};

class ScopedReadFilePermission {
public:
    ScopedReadFilePermission(PermissionCenter *permissionCenter, const std::string &fileName) {
        _permissionCenter = permissionCenter;
        _fileName = fileName;
    }
    ScopedReadFilePermission(PermissionCenter *permissionCenter) { _permissionCenter = permissionCenter; }
    ~ScopedReadFilePermission();

private:
    ScopedReadFilePermission(const ScopedReadFilePermission &);
    ScopedReadFilePermission &operator=(const ScopedReadFilePermission &);

private:
    PermissionCenter *_permissionCenter;
    std::string _fileName;
};

class ScopedMaxIdPermission {
public:
    ScopedMaxIdPermission(PermissionCenter *permissionCenter) { _permissionCenter = permissionCenter; }
    ~ScopedMaxIdPermission();

private:
    ScopedMaxIdPermission(const ScopedMaxIdPermission &);
    ScopedMaxIdPermission &operator=(const ScopedMaxIdPermission &);

private:
    PermissionCenter *_permissionCenter;
};

class ScopedMinIdPermission {
public:
    ScopedMinIdPermission(PermissionCenter *permissionCenter) { _permissionCenter = permissionCenter; }
    ~ScopedMinIdPermission();

private:
    ScopedMinIdPermission(const ScopedMinIdPermission &);
    ScopedMinIdPermission &operator=(const ScopedMinIdPermission &);

private:
    PermissionCenter *_permissionCenter;
};

} // namespace util
} // namespace swift
