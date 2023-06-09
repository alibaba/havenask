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
#include "autil/Lock.h"

namespace autil {

class RecursivePthreadMutexattr {
public:
    RecursivePthreadMutexattr() {
        pthread_mutexattr_init(&_mta);
        pthread_mutexattr_settype(&_mta, PTHREAD_MUTEX_RECURSIVE);
    }

    ~RecursivePthreadMutexattr() {
        pthread_mutexattr_destroy(&_mta);
    }

public:
    pthread_mutexattr_t _mta;
};

static const RecursivePthreadMutexattr
    sRecursivePthreadMutexattr = RecursivePthreadMutexattr();

const pthread_mutexattr_t *
RecursiveThreadMutex::RECURSIVE_PTHREAD_MUTEXATTR_PTR
    = &sRecursivePthreadMutexattr._mta;
const int Notifier::EXITED = (1 << 16) + 1;

}
