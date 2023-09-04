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
#include "navi/perf/NaviPerfEpollThread.h"
#include "navi/perf/NaviPerfThread.h"
#include "aios/network/anet/filecontrol.h"
#include <autil/StringUtil.h>
#include <errno.h>
#include <sys/epoll.h>
#include <unistd.h>

namespace navi {

NaviPerfEpollThread::NaviPerfEpollThread()
    : _epollFd(-1)
    , _stop(false)
{
    _pipes[0] = -1;
    _pipes[1] = -1;
}

NaviPerfEpollThread::~NaviPerfEpollThread() {
    stop();
}

bool NaviPerfEpollThread::init(const NaviLoggerPtr &logger) {
    _logger.object = this;
    _logger.logger = logger;
    _logger.addPrefix("navi perf epoll");
    _epollFd = epoll_create(256);
    if (-1 == _epollFd) {
        NAVI_LOG(ERROR, "open perf epoll fd failed, error [%s]",
                 strerror(errno));
        return false;
    }
    anet::FileControl::setCloseOnExec(_epollFd);
    if (!createPipes()) {
        return false;
    }
    return true;
}

bool NaviPerfEpollThread::addPerfThread(NaviPerfThread *perfThread) {
    struct epoll_event ev;
    ev.data.ptr = perfThread;
    ev.events = 0;
    ev.events |= EPOLLIN;
    int perfFd = perfThread->getFd();
    if (0 != epoll_ctl(_epollFd, EPOLL_CTL_ADD, perfFd, &ev)) {
        NAVI_LOG(ERROR, "add perf fd [%d] to epoll fd [%d] failed, error [%s]",
                 perfFd, _epollFd, strerror(errno));
        return false;
    }
    return true;
}

bool NaviPerfEpollThread::start(uint32_t id) {
    if (!createThread(id)) {
        return false;
    }
    return true;
}

bool NaviPerfEpollThread::createPipes() {
    if (0 != pipe(_pipes)) {
        _pipes[0] = -1;
        _pipes[1] = -1;
        NAVI_LOG(ERROR, "open perf pipe fd failed, error [%s]",
                 strerror(errno));
        return false;
    }
    anet::FileControl::setCloseOnExec(_pipes[0]);
    anet::FileControl::setCloseOnExec(_pipes[1]);
    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLET;
    ev.data.ptr = this;
    if (0 != epoll_ctl(_epollFd, EPOLL_CTL_ADD, _pipes[0], &ev)) {
        NAVI_LOG(ERROR,
                 "add pipe fd [%d] to perf epoll fd [%d] failed, error [%s]",
                 _pipes[1], _epollFd, strerror(errno));
        return false;
    }
    return true;
}

bool NaviPerfEpollThread::createThread(uint32_t id) {
    _thread = autil::Thread::createThread(
        std::bind(&NaviPerfEpollThread::workLoop, this),
        "NaviPerf" + autil::StringUtil::toString(id));
    if (!_thread) {
        NAVI_LOG(ERROR, "create perf sample thread failed");
        return false;
    }
    return true;
}

void NaviPerfEpollThread::stop() {
    _stop = true;
    NAVI_MEMORY_BARRIER();
    if (-1 != _pipes[1]) {
        close(_pipes[1]);
    }
    _pipes[0] = -1;
    _pipes[1] = -1;
    _thread.reset();
    if (-1 != _epollFd) {
        close(_epollFd);
        _epollFd = -1;
    }
    if (-1 != _pipes[0]) {
        close(_pipes[0]);
    }
}

void NaviPerfEpollThread::workLoop() {
    NaviLoggerScope scope(_logger);
    while (!_stop) {
        constexpr int MAX_EVENT = 256;
        struct epoll_event events[MAX_EVENT];
        auto count = epoll_wait(_epollFd, events, MAX_EVENT, 5000);
        if (count < 0) {
            continue;
        }
        for (int i = 0; i < count; i++) {
            if (events[i].data.ptr == this) {
                continue;
            }
            auto perfThread = (NaviPerfThread *)events[i].data.ptr;
            perfThread->sample();
        }
    }
}

}

