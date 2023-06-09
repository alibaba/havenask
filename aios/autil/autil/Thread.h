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

#include <cassert>
#include <functional>
#include <memory>
#include <pthread.h>
#include <stddef.h>
#include <string>

namespace autil {

class Thread;

typedef std::shared_ptr<Thread> ThreadPtr;
typedef pthread_t ThreadId;

class Thread {
public:
    // when create thread fail, return null ThreadPtr
    // need hold return value else will hang to the created thread terminate
    static ThreadPtr createThread(const std::function<void()> &threadFunction, const std::string &name = "");
    ~Thread() { join(); }
    void join() {
        if (_id) {
            int ret = pthread_join(_id, NULL);
            (void)ret;
            assert(ret == 0);
        }
        _id = 0;
    }
    pthread_t getId() const { return _id; }

private:
    static void *threadWrapperFunction(void *arg);

private:
    Thread() { _id = 0; }
    pthread_t _id;
};

} // namespace autil
