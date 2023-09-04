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
#ifndef ISEARCH_MULTI_CALL_GIGJNITHREADATTACHER_H
#define ISEARCH_MULTI_CALL_GIGJNITHREADATTACHER_H

#include <jni.h>
#include <thread>

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/java/GigInstance.h"
#include "autil/Lock.h"
#include "autil/LockFreeThreadPool.h"
#include "autil/ThreadPool.h"
#include "autil/WorkItem.h"

namespace multi_call {

class GigJniAttachWorkItem : public autil::WorkItem
{
public:
    GigJniAttachWorkItem(bool attach, autil::ThreadMutex &mutex) : _attach(attach), _mutex(mutex) {
    }

public:
    void process() override;
    void destroy() override;
    void drop() override;

private:
    bool _attach;
    autil::ThreadMutex &_mutex;

private:
    AUTIL_LOG_DECLARE();
};

class GigJniThreadAttacher
{
public:
    static void loadJavaVM(JavaVM *jvm);
    static void unloadJavaVM(JavaVM *jvm);

public:
    static bool attachCurrentThread();
    static bool attachInstance(GigInstance *ins);
    static bool detachInstance(GigInstance *ins);
    static bool doAttachOrDetach(bool attach);

private:
    static bool attachOrDetach(autil::LockFreeThreadPool *threadPool, bool attach);
    static std::string currrentThreadName();

private:
    static autil::ThreadMutex _attachMutex;
    static JavaVM *_jvm;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GigJniThreadAttacher);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGJNITHREADATTACHER_H
