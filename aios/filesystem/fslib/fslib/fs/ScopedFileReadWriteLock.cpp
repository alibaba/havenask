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
#include "fslib/fs/ScopedFileReadWriteLock.h"

FSLIB_BEGIN_NAMESPACE(fs);
AUTIL_DECLARE_AND_SETUP_LOGGER(fs, ScopedFileReadWriteLock);

ScopedFileReadWriteLock::ScopedFileReadWriteLock()
    : _lock(NULL)
    , _mode(' ') 
{ 
}

ScopedFileReadWriteLock::~ScopedFileReadWriteLock() { 
    if (_lock) {
        release();
    }
}

void ScopedFileReadWriteLock::release() {
    if (_mode == 'r' || _mode == 'R'
        || _mode == 'w' || _mode == 'W') 
    {
        _lock->unlock();
    }
        
    delete _lock;
    _lock = NULL;
}

bool ScopedFileReadWriteLock::init(FileReadWriteLock* lock, const char mode) {
    if (lock == NULL) {
        AUTIL_LOG(ERROR, "lock passed is NULL");
        return false;
    } 
      
    if (_lock) {
        // TODO: currently not support recursively init
        AUTIL_LOG(ERROR, "current ScopedFileReadWriteLock is still used");
        release();
        _lock = lock;
        _mode = ' ';
        return false;
    }
       
    _lock = lock;
    _mode = mode;
    int ret;
    if (_mode == 'r' || _mode == 'R') {
        ret = _lock->rdlock();
    } else if (_mode == 'w' || _mode == 'W') {
        ret = _lock->wrlock();
    } else {
        AUTIL_LOG(ERROR, "mode %c not support", mode);
        return false;
    }

    if (ret == -1) {
        return false;
    }

    return true;
}

FSLIB_END_NAMESPACE(fs);

