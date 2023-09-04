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
#ifndef ANET_MUTEX_H_
#define ANET_MUTEX_H_

#include <assert.h>
#include <pthread.h>
#include <stddef.h>

namespace anet {

class ThreadMutex {

public:
    ThreadMutex() {
        int ret = pthread_mutex_init(&_mutex, NULL);
        assert(ret == 0);
        *(int *)&ret = 0;
    }

    ~ThreadMutex() { pthread_mutex_destroy(&_mutex); }

    void lock() { pthread_mutex_lock(&_mutex); }

    void unlock() { pthread_mutex_unlock(&_mutex); }

protected:
    pthread_mutex_t _mutex;
};

class MutexGuard {
public:
    MutexGuard(ThreadMutex *mutex) {
        _mutex = mutex;
        if (_mutex) {
            _mutex->lock();
        }
    }
    ~MutexGuard() {
        if (_mutex) {
            _mutex->unlock();
        }
    }

private:
    ThreadMutex *_mutex;
};

} // namespace anet
#endif /*MUTEX_H_*/
