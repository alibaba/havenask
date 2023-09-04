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
#ifndef FSLIB_SCOPEDFILEREADWRITELOCK_H
#define FSLIB_SCOPEDFILEREADWRITELOCK_H

#include "autil/Log.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileReadWriteLock.h"

FSLIB_BEGIN_NAMESPACE(fs);

class ScopedFileReadWriteLock {
public:
    friend class FileSystem;
    friend class ScopedFileReadWriteLockTest;

public:
    ScopedFileReadWriteLock();
    ~ScopedFileReadWriteLock();

private:
    void release();
    bool init(FileReadWriteLock *lock, const char mode);

private:
    FileReadWriteLock *_lock;
    char _mode;
};

FSLIB_TYPEDEF_AUTO_PTR(ScopedFileReadWriteLock);

FSLIB_END_NAMESPACE(fs);

#endif // FSLIB_SCOPEDFILEREADWRITELOCK_H
