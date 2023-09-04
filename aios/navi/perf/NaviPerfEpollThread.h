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

#include "navi/log/NaviLogger.h"
#include <autil/Thread.h>

namespace navi {

class NaviPerfThread;

class NaviPerfEpollThread
{
public:
    NaviPerfEpollThread();
    ~NaviPerfEpollThread();
private:
    NaviPerfEpollThread(const NaviPerfEpollThread &);
    NaviPerfEpollThread &operator=(const NaviPerfEpollThread &);
public:
    bool init(const NaviLoggerPtr &logger);
    bool addPerfThread(NaviPerfThread *perfThread);
    bool start(uint32_t id);
    void stop();
private:
    bool createPipes();
    bool createThread(uint32_t id);
    void workLoop();
private:
    DECLARE_LOGGER();
    int _epollFd;
    int _pipes[2];
    volatile bool _stop;
    autil::ThreadPtr _thread;
};

NAVI_TYPEDEF_PTR(NaviPerfEpollThread);

}
