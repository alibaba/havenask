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

#include "navi/common.h"
#include "navi/log/NaviLogger.h"
#include "navi/perf/NaviPerfResult.h"
#include <arpc/common/LockFreeQueue.h>
#include <linux/perf_event.h>

namespace navi {

typedef arpc::common::LockFreeQueue<NaviPerfResultPtr> NaviPerfResultQueue;

class NaviPerfThread {
public:
    NaviPerfThread(pid_t pid);
    ~NaviPerfThread();
public:
    bool init(const NaviLoggerPtr &logger);
    int getFd() const;
    bool enable();
    bool disable();
    bool beginSample();
    NaviPerfResultPtr endSample();
    void sample();
private:
    void initPerfAttr();
    int perf_event_open(int cpu, int groupFd, unsigned long flags);
    NaviPerfResultPtr getResult() const;
    void setResult(const NaviPerfResultPtr &newResult);
    void pushQueue(NaviPerfResultQueue &queue,
                   const NaviPerfResultPtr &newResult);
    NaviPerfResultPtr popQueue(NaviPerfResultQueue &queue) const;
    void collectSubResult();
    int32_t collectResultEntry(NaviPerfEventEntry *entry);
private:
    void __attribute__ ((noinline)) flushLBR(int32_t depth);
private:
    DECLARE_LOGGER();
    pid_t _pid;
    std::atomic<bool> _enabled;
    perf_event_attr _attr;
    size_t _bufferSize;
    size_t _mmapSize;
    int _fd;
    void *_mem;
    arpc::common::LockFreeQueue<NaviPerfResultPtr> _resultQueue;
    arpc::common::LockFreeQueue<NaviPerfResultPtr> _mainQueue;
    NaviPerfResultPtr _resultQueueTail;
    NaviPerfResultPtr _mainQueueHead;
    NaviPerfResultPtr _resultEndList;
};

}
