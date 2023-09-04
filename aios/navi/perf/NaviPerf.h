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
#include "navi/perf/NaviPerfEpollThread.h"
#include "navi/perf/NaviPerfResult.h"
#include <sys/types.h>
#include <unordered_map>

namespace navi {

class NaviPerfThread;

class NaviPerf
{
public:
    NaviPerf();
    ~NaviPerf();
private:
    NaviPerf(const NaviPerf &);
    NaviPerf &operator=(const NaviPerf &);
public:
    void addPid(pid_t pid);
    size_t pidCount() const;
    bool init(const NaviLoggerPtr &logger);
    void stop();
public:
    bool enable(pid_t pid);
    bool disable(pid_t pid);
    bool beginSample(pid_t pid);
    NaviPerfResultPtr endSample(pid_t pid);
private:
    bool initEpollThread();
    bool initPerfThread();
    bool startEpollThread();    
    NaviPerfThread *getNaviPerfThread(pid_t pid);
private:
    DECLARE_LOGGER();
    std::unordered_map<pid_t, NaviPerfThread *> _pidMap;
    std::vector<NaviPerfEpollThreadPtr> _epollThread;
    bool _inited;
};

}

