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

#include <stdint.h>
#include <string>
#include <functional>
#include <memory>

#include "autil/Lock.h"
#include "autil/Thread.h"

namespace autil {

class LoopThread;

typedef std::shared_ptr<LoopThread> LoopThreadPtr;

class LoopThread
{
private:
    explicit LoopThread(bool strictMode = false);
public:
    ~LoopThread();
private:
    LoopThread(const LoopThread &);
    LoopThread& operator = (const LoopThread &);
public:
    void stop();
public:
    // if create loop thread fail, return null LoopThreadPtr
    static LoopThreadPtr createLoopThread(const std::function<void ()> &loopFunction,
                                          int64_t loopInterval /*us*/, bool strictMode = false);
    // add this function to avoid the cast from (const char *) to (bool)
    static LoopThreadPtr createLoopThread(const std::function<void ()> &loopFunction, int64_t loopInterval /*us*/,
                                          const char* name, bool strictMode = false);
    static LoopThreadPtr createLoopThread(const std::function<void ()> &loopFunction, int64_t loopInterval /*us*/,
                                          const std::string &name, bool strictMode = false);
    void runOnce();

public:
    static void disableLoopFuncForTest() { _disableLoopFun = true; }
    static void enableLoopFuncForTest() { _disableLoopFun = false; }
private:
    void loop();
private:
    volatile bool _run;
    ThreadCond _cond;
    ThreadPtr _threadPtr;
    std::string _name;
    std::function<void ()> _loopFunction;
    int64_t _loopInterval;
    int64_t _nextTime;
    bool _strictMode;
    static bool _disableLoopFun;
};

}

