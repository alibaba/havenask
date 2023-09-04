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
#include "autil/Thread.h"

#include <cstddef>
#include <functional>
#include <memory>
#include <pthread.h>
#include <string>

#include "autil/Log.h"
#include "autil/ThreadNameScope.h"

using namespace std;

namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, Thread);

struct ThreadFunctionWrapperArg {
    function<void()> threadFunction;
    string name;
};

void *Thread::threadWrapperFunction(void *arg) {
    unique_ptr<ThreadFunctionWrapperArg> p(static_cast<ThreadFunctionWrapperArg *>(arg));
    AUTIL_LOG(INFO, "thread started for name[%s]", p->name.c_str());
    p->threadFunction();
    return NULL;
}

ThreadPtr Thread::createThread(const function<void()> &threadFunction, const string &name) {
    ThreadPtr threadPtr(new Thread);
    ThreadFunctionWrapperArg *arg = new ThreadFunctionWrapperArg;
    arg->threadFunction = threadFunction;
    arg->name = name;

    unique_ptr<ThreadNameScope> scope;
    if (!name.empty()) {
        scope.reset(new ThreadNameScope(name));
    }
    int rtn = pthread_create(&threadPtr->_id, NULL, &Thread::threadWrapperFunction, arg);

    if (rtn != 0) {
        delete arg;
        threadPtr->_id = 0;
        threadPtr.reset();
        AUTIL_LOG(ERROR, "Create thread error [%d]", rtn);
    }
    return threadPtr;
}

} // namespace autil
