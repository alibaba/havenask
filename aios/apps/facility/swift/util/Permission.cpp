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
#include "swift/util/Permission.h"

#include <memory>

#include "autil/TimeUtility.h"
#include "swift/util/PermissionCenter.h"

using namespace autil;

namespace swift {
namespace util {
AUTIL_LOG_SETUP(swift, Permission);

Permission::Permission(const PermissionKey &key, PermissionCenter *center) {
    _key = key;
    _startTime = TimeUtility::currentTime();
    _center = center;
}

Permission::~Permission() {}

void Permission::release() { _center->release(this); }

ScopedPermission::~ScopedPermission() {
    if (_permissionCenter) {
        _permission->release();
    }
}

ScopedReadPermission::~ScopedReadPermission() {
    if (_permissionCenter) {
        _permissionCenter->decReadCount();
    }
}
ScopedWritePermission::~ScopedWritePermission() {
    if (_permissionCenter) {
        _permissionCenter->decWriteCount();
    }
}
ScopedReadFilePermission::~ScopedReadFilePermission() {
    if (_permissionCenter) {
        if (_fileName.empty()) {
            _permissionCenter->decReadFileCount();
        } else {
            _permissionCenter->decReadFileCount(_fileName);
        }
    }
}
ScopedMaxIdPermission::~ScopedMaxIdPermission() {
    if (_permissionCenter) {
        _permissionCenter->decGetMaxIdCount();
    }
}
ScopedMinIdPermission::~ScopedMinIdPermission() {
    if (_permissionCenter) {
        _permissionCenter->decGetMinIdCount();
    }
}

} // namespace util
} // namespace swift
