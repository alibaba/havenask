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
#include "aios/network/gig/multi_call/java/GigJniThreadAttacher.h"

#include <iostream>
#include <unistd.h>

#include "aios/network/gig/multi_call/service/ConnectionManager.h"
#include "aios/network/gig/multi_call/service/SearchServiceManager.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigJniAttachWorkItem);
AUTIL_LOG_SETUP(multi_call, GigJniThreadAttacher);

autil::ThreadMutex GigJniThreadAttacher::_attachMutex;
JavaVM *GigJniThreadAttacher::_jvm = NULL;
thread_local bool currentThreadJvmAttatched = false;

void GigJniThreadAttacher::loadJavaVM(JavaVM *jvm) {
    std::cout << "JNI_OnLoad java vm:" << jvm << std::endl;
    _jvm = jvm;
}

void GigJniThreadAttacher::unloadJavaVM(JavaVM *jvm) {
    std::cout << "JNI_OnUnload java vm:" << jvm << std::endl;
    if (_jvm != jvm) {
        std::cerr << "not equal java vm: " << _jvm << " != " << jvm << std::endl;
    }
    _jvm = NULL;
}

bool GigJniThreadAttacher::attachCurrentThread() {
    if (currentThreadJvmAttatched) {
        return true;
        ;
    }
    if (!_jvm) {
        AUTIL_LOG(WARN, "jvm is null, attach jvm to current thread failed, return!");
        return false;
    }
    return doAttachOrDetach(true);
}

bool GigJniThreadAttacher::attachInstance(GigInstance *ins) {
    AUTIL_LOG(INFO, "Attach gig instance:%p", ins);
    if (!_jvm) {
        AUTIL_LOG(WARN, "jvm is null, return!");
        return false;
    }
    if (!ins) {
        AUTIL_LOG(WARN, "gig instance is null in Attach!");
        return false;
    }
    SearchServicePtr searchService = ins->getSearchServicePtr();
    if (!searchService) {
        AUTIL_LOG(WARN, "searchService is null!");
        return false;
    }
    SearchServiceManagerPtr serviceManager = searchService->getSearchServiceManager();
    const ConnectionManagerPtr &connectionManager = serviceManager->getConnectionManager();
    auto callbackThreadPool = connectionManager->getCallBackThreadPool();
    return attachOrDetach(callbackThreadPool, true);
}

bool GigJniThreadAttacher::detachInstance(GigInstance *ins) {
    AUTIL_LOG(INFO, "Detach gig instance:%p", ins);
    if (!_jvm) {
        AUTIL_LOG(WARN, "jvm is null, return!");
        return false;
    }
    if (!ins) {
        AUTIL_LOG(WARN, "gig instance is null in Detach!");
        return false;
    }
    SearchServicePtr searchService = ins->getSearchServicePtr();
    if (!searchService) {
        AUTIL_LOG(WARN, "searchService is null!");
        return false;
    }
    SearchServiceManagerPtr serviceManager = searchService->getSearchServiceManager();
    const ConnectionManagerPtr &connectionManager = serviceManager->getConnectionManager();
    auto callbackThreadPool = connectionManager->getCallBackThreadPool();
    return attachOrDetach(callbackThreadPool, false);
}

bool GigJniThreadAttacher::attachOrDetach(autil::LockFreeThreadPool *threadPool, bool attach) {
    if (!threadPool) {
        AUTIL_LOG(INFO, "threadPool is null, do nothing");
        return true;
    }

    autil::ScopedLock lock(_attachMutex);
    size_t threadNum = threadPool->getThreadNum();
    AUTIL_LOG(INFO, "callback thread num:%ld, push equal attach work items", threadNum);

    for (size_t i = 0; i < threadNum; i++) {
        threadPool->pushWorkItem(new GigJniAttachWorkItem(attach, _attachMutex));
    }
    while (true) {
        if ((size_t)0 == threadPool->getItemCount()) {
            break;
        }
        usleep(1000);
    }

    return true;
}

std::string GigJniThreadAttacher::currrentThreadName() {
    char buf[16];
    pthread_t threadId = pthread_self();
    if (pthread_getname_np(threadId, buf, 16) == 0) {
        return std::string(buf);
    } else {
        AUTIL_LOG(INFO, "get current thread name failed, msg:%s", buf);
        return EMPTY_STRING;
    }
}

bool GigJniThreadAttacher::doAttachOrDetach(bool attach) {
    std::string threadIdStr = StringUtil::toString(this_thread::get_id());
    std::string threadName = currrentThreadName();
    JNIEnv *env;
    if (attach) {
        int attached = _jvm->GetEnv((void **)&env, JNI_VERSION_1_4) == JNI_OK;
        if (attached) {
            currentThreadJvmAttatched = true;
            return true;
        }
        if (_jvm->AttachCurrentThread((void **)&env, NULL) == JNI_OK) {
            AUTIL_LOG(INFO, "attach succeed, thread id:%s, name:%s", threadIdStr.c_str(),
                      threadName.c_str());
            currentThreadJvmAttatched = true;
            return true;
        } else {
            AUTIL_LOG(WARN, "GigJni: Can't attach native thread to VM, thread id:%s, name:%s",
                      threadIdStr.c_str(), threadName.c_str());
            return false;
        }
    } else { // detach
        int attached = _jvm->GetEnv((void **)&env, JNI_VERSION_1_4) == JNI_OK;
        if (!attached) {
            currentThreadJvmAttatched = false;
            return true;
        }
        if (_jvm->DetachCurrentThread() == 0) {
            AUTIL_LOG(INFO, "detach succeed, thread id:%s, name:%s", threadIdStr.c_str(),
                      threadName.c_str());
            currentThreadJvmAttatched = false;
            return true;
        } else {
            AUTIL_LOG(WARN, "GigJni: could not detach thread:%s, name:%s", threadIdStr.c_str(),
                      threadName.c_str());
            return false;
        }
    }
}

void GigJniAttachWorkItem::process() {
    autil::ScopedLock lock(_mutex);
    GigJniThreadAttacher::doAttachOrDetach(_attach);
}

void GigJniAttachWorkItem::destroy() {
    delete this;
}

void GigJniAttachWorkItem::drop() {
    std::string threadIdStr = StringUtil::toString(this_thread::get_id());
    AUTIL_LOG(WARN, "drop jni attach for thread:%s", threadIdStr.c_str());
}

} // namespace multi_call
