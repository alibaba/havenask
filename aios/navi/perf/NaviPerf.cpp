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
#include "navi/perf/NaviPerf.h"
#include "navi/perf/NaviPerfThread.h"

namespace navi {

NaviPerf::NaviPerf()
    : _inited(false)
{
}

NaviPerf::~NaviPerf() {
    stop();
    for (const auto &pair : _pidMap) {
        delete pair.second;
    }
}

void NaviPerf::addPid(pid_t pid) {
    auto thread = new NaviPerfThread(pid);
    _pidMap.emplace(pid, thread);
}

size_t NaviPerf::pidCount() const {
    return _pidMap.size();
}

bool NaviPerf::init(const NaviLoggerPtr &logger) {
    _logger.object = this;
    _logger.logger = logger;
    _logger.addPrefix("navi perf");
    if (!initEpollThread()) {
        return false;
    }
    if (!initPerfThread()) {
        return false;
    }
    if (!startEpollThread()) {
        return false;
    }
    _inited = true;
    return true;
}

bool NaviPerf::initEpollThread() {
    constexpr int32_t epoll_thread_count = 5;
    _epollThread.reserve(epoll_thread_count);
    for (auto i = 0; i < epoll_thread_count; ++i) {
        NaviPerfEpollThreadPtr epollThread(new NaviPerfEpollThread());
        if (!epollThread->init(_logger.logger)) {
            return false;
        }
        _epollThread.emplace_back(std::move(epollThread));
    }
    return true;
}

bool NaviPerf::initPerfThread() {
    auto epollThreadCount = _epollThread.size();
    size_t i = 0;
    for (auto &pair : _pidMap) {
        auto perfThread = pair.second;
        if (!perfThread->init(_logger.logger)) {
            return false;
        }
        const auto &epollThread = _epollThread[i++ % epollThreadCount];
        if (!epollThread->addPerfThread(perfThread)) {
            return false;
        }
    }
    return true;
}

bool NaviPerf::startEpollThread() {
    for (uint32_t i = 0; i < _epollThread.size(); ++i) {
        if (!_epollThread[i]->start(i)) {
            NAVI_LOG(ERROR, "start navi perf epoll thread failed");
            return false;
        }
    }
    return true;
}

bool NaviPerf::enable(pid_t pid) {
    if (!_inited) {
        return false;
    }
    auto perfThread = getNaviPerfThread(pid);
    if (!perfThread) {
        return false;
    }
    return perfThread->enable();
}

bool NaviPerf::disable(pid_t pid) {
    if (!_inited) {
        return false;
    }
    auto perfThread = getNaviPerfThread(pid);
    if (!perfThread) {
        return false;
    }
    return perfThread->disable();
}

bool NaviPerf::beginSample(pid_t pid) {
    if (!_inited) {
        return false;
    }
    auto perfThread = getNaviPerfThread(pid);
    if (!perfThread) {
        return false;
    }
    return perfThread->beginSample();
}

NaviPerfResultPtr NaviPerf::endSample(pid_t pid) {
    if (!_inited) {
        return nullptr;
    }
    auto perfThread = getNaviPerfThread(pid);
    if (!perfThread) {
        return nullptr;
    }
    auto result = perfThread->endSample();
    if (!result) {
        NAVI_KERNEL_LOG(
            ERROR, "can't stop sample thread [%d], sample not started", pid);
        return nullptr;
    }
    return result;
}

NaviPerfThread *NaviPerf::getNaviPerfThread(pid_t pid) {
    auto it = _pidMap.find(pid);
    if (_pidMap.end() == it){
        NAVI_KERNEL_LOG(DEBUG,
                        "can't find sample thread [%d], not in init list", pid);
        return nullptr;
    }
    return it->second;
}

void NaviPerf::stop() {
    for (const auto &epollThread : _epollThread) {
        epollThread->stop();
    }
    _epollThread.clear();
}

}

