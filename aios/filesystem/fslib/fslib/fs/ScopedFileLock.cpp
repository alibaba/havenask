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
#include "fslib/fs/ScopedFileLock.h"

FSLIB_BEGIN_NAMESPACE(fs);
AUTIL_DECLARE_AND_SETUP_LOGGER(fs, ScopedFileLock);

ScopedFileLock::ScopedFileLock() 
    : _lock(NULL)
{ 
}

ScopedFileLock::~ScopedFileLock() { 
    if (_lock) {
        release();
    }
}

void ScopedFileLock::release() {
    _lock->unlock();
    delete _lock;
    _lock = NULL;
}

bool ScopedFileLock::init(FileLock* lock) {
    if (lock == NULL) {
        AUTIL_LOG(ERROR, "lock passed is NULL");
        return false;
    } 
    
    if (_lock) {
        // TODO: currently not support recursively init
        AUTIL_LOG(ERROR, "current ScopedFileLock is still used");
        release();
        _lock = lock;
        return false;
    }
       
    _lock = lock;
    if (_lock->lock() == -1) {
        return false;
    }

    return true;
}

FSLIB_END_NAMESPACE(fs);

