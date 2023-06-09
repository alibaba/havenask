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
#include "navi/log/NaviLogger.h"
#include "navi/engine/NaviThreadPool.h"
#include <malloc.h>
#include <unistd.h>
// #include "navi/perf/perf.h"

namespace navi {

thread_local int32_t INLINE_DEPTH_TLS = INVALID_INLINE_DEPTH;
thread_local size_t current_thread_id = 0;
thread_local size_t current_thread_counter = 0;
thread_local size_t current_thread_wait_counter = 0;

NaviThreadPool::NaviThreadPool()
    : _run(false)
    , _accept(false)
    , _threadNum(DEFAULT_THREAD_NUMBER)
    , _autoScale(false)
    , _minThreadNum(DEFAULT_THREAD_NUMBER)
    , _maxThreadNum(DEFAULT_THREAD_NUMBER)
    , _activeThreadNum(DEFAULT_THREAD_NUMBER)
    , _threads(nullptr)
{
    atomic_set(&_workerCount, 0);
    atomic_set(&_runningThread, 0);
    atomic_set(&_processingCount, 0);
    atomic_set(&_wakeIndex, 0);
}

NaviThreadPool::~NaviThreadPool() {
    clear();
    stop();
    delete[] _threads;
}

bool NaviThreadPool::start(const ConcurrencyConfig &config,
                           const NaviLoggerPtr &logger,
                           const std::string &name)
{
    if (_run) {
        return true;
    }
    _logger.object = this;
    _logger.logger = logger;
    _logger.addPrefix("thread pool %s", name.c_str());
    _run = true;
    _accept = true;
    return createThreads(config, name);
}

void NaviThreadPool::stop() {
    waitQueueEmpty();
    waitThreadStop();
    for (size_t i = 0; i < _threadNum; i++) {
        _threads[i].thread.reset();
    }
    int32_t tid = 0;
    while (_idleQueue.Pop(&tid)) {
    }
}

void NaviThreadPool::clear() {
    NaviThreadPoolItemBase *item = nullptr;
    while (_scheduleQueue.Pop(&item)) {
        NAVI_KERNEL_LOG(ERROR, "drop item [%p]", item);
        item->destroy();
    }
}

int32_t NaviThreadPool::getIdleTid() {
    int32_t tid = 0;
    if (_idleQueue.Pop(&tid)) {
        return tid;
    }
    return -1;
}

void NaviThreadPool::signal(int32_t tid) {
    if (tid < 0) {
        tid = getIdleTid();
    }
    if (tid >= 0) {
        NAVI_KERNEL_LOG(SCHEDULE3, "pop idle [%d]", tid);
        auto &thread = _threads[tid];
        auto &cond = thread.cond;
        auto &stat = thread.stat;
        if (0 == cond.trylock()) {
            NAVI_KERNEL_LOG(SCHEDULE3, "wake up [%d]", tid);
            stat = TS_WAKEUP;
            cond.signal();
            cond.unlock();
            return;
        }
        NAVI_KERNEL_LOG(SCHEDULE3, "fail to wakeup idle queue thread: %d", tid);
    }
    while (true) {
        assert(_activeThreadNum > 0);
        int32_t index = atomic_inc_return(&_wakeIndex) % _activeThreadNum;
        auto &thread = _threads[index];
        auto &cond = thread.cond;
        if (0 == cond.trylock()) {
            auto &stat = thread.stat;
            if (TS_WAIT == stat) {
                NAVI_KERNEL_LOG(SCHEDULE3, "wake up [%d]", index);
                stat = TS_WAKEUP;
                cond.signal();
            } else {
                stat = TS_WAKEUP;
            }
            cond.unlock();
            break;
        }
    }
}

bool NaviThreadPool::wait(int32_t tid) {
    if (!_run) {
        return false;
    }
    bool suspend = false;
    auto &cond = _threads[tid].cond;
    if (0 == cond.trylock()) {
        auto &stat = _threads[tid].stat;
        if (stat == TS_WAKEUP) {
            NAVI_KERNEL_LOG(SCHEDULE3, "wait success immediately [%d]", tid);
            stat = TS_RUNNING;
            cond.unlock();
            return true;
        }
        stat = TS_WAIT;
        if (tid < _activeThreadNum) {
            NAVI_KERNEL_LOG(SCHEDULE3, "push to idle [%d]", tid);
            _idleQueue.Push(tid);
        } else {
            suspend = true;
            NAVI_KERNEL_LOG(SCHEDULE3, "thread [%d] suspended", tid);
        }
        current_thread_wait_counter++;
        cond.wait();
        stat = TS_RUNNING;
        cond.unlock();
        if (suspend) {
            NAVI_KERNEL_LOG(SCHEDULE3, "thread [%d] resumed", tid);
        }
    }
    return true;
}

void NaviThreadPool::waitQueueEmpty() {
    _accept = false;
    size_t count = 0;
    size_t sleepTime = 200;
    while (true) {
        for (size_t i = 0; i < _threadNum; i++) {
            auto &cond = _threads[i].cond;
            autil::ScopedLock lock(cond);
            cond.signal();
        }
        if (0ul == _scheduleQueue.Size() &&
            atomic_read(&_workerCount) == 0)
        {
            break;
        }
        if (count++ % 200 == 0) {
             NAVI_KERNEL_LOG(
                 INFO,
                 "thread pool not empty, scheduleQueue size [%lu], workerCount "
                 "[%lld]",
                 _scheduleQueue.Size(), atomic_read(&_workerCount));
        }
        usleep(sleepTime);
        sleepTime += 1000;
        if (sleepTime > 10 * 1000) {
            sleepTime = 10 * 1000;
        }
    }
}

void NaviThreadPool::waitThreadStop() {
    _run = false;
    size_t sleepTime = 200;
    while (true) {
        for (size_t i = 0; i < _threadNum; i++) {
            auto &cond = _threads[i].cond;
            autil::ScopedLock lock(cond);
            cond.signal();
        }
        if (0 == atomic_read(&_runningThread)) {
            break;
        }
        usleep(sleepTime);
        sleepTime += 1000;
        if (sleepTime > 10 * 1000) {
            sleepTime = 10 * 1000;
        }
    }
    if (_backgroundThread) {
        {
            autil::ScopedLock lock(_backgroundCond);
            _backgroundCond.signal();
        }
        _backgroundThread.reset();
    }
}

bool NaviThreadPool::createThreads(const ConcurrencyConfig &config,
                                   const std::string &name)
{
    _configThreadNum = config.threadNum;
    if (_configThreadNum <= 0) { // enable auto scale
        initThreadNumRange(config);
        _threadNum = _maxThreadNum;
        _autoScale = true;
    } else {
        _threadNum = (size_t)_configThreadNum;
    }
    _activeThreadNum = _threadNum;
    _threads = new NaviThread[_threadNum];
    for (size_t i = 0; i < _threadNum; i++) {
        auto thread = autil::Thread::createThread(
            std::bind(&NaviThreadPool::workLoop, this, (int32_t)i), name);
        if (!thread) {
            NAVI_KERNEL_LOG(ERROR, "Create thread fail!");
            _run = false;
            return false;
        }
        _threads[i].thread = thread;
    }
    auto bgThread = autil::Thread::createThread(
            std::bind(&NaviThreadPool::backgroundThread, this), "navi_bg");
    if (!bgThread) {
        NAVI_KERNEL_LOG(ERROR, "Create background thread fail!");
        _run = false;
        return false;
    }
    _backgroundThread = bgThread;
    NAVI_KERNEL_LOG(INFO,
                    "create threads success, autoScale[%d], config[%d],"
                    "threadNum[%lu], minThreadNum[%lu], maxThreadNum[%lu]",
                    _autoScale, _configThreadNum,
                    _threadNum, _minThreadNum, _maxThreadNum);
    return true;
}

void NaviThreadPool::initThreadNumRange(const ConcurrencyConfig &config) {
    _minThreadNum = config.minThreadNum;
    _maxThreadNum = config.maxThreadNum;
    if (DEFAULT_THREAD_NUMBER == _maxThreadNum) {
        _maxThreadNum = DEFAULT_MAX_THREAD_NUMBER;
    } else {
        _maxThreadNum = std::min(_maxThreadNum, DEFAULT_MAX_THREAD_NUMBER);
    }
    _minThreadNum = std::min(_minThreadNum, _maxThreadNum);
}

size_t NaviThreadPool::getCoreNum() {
    errno = 0;
    auto coreNum = sysconf(_SC_NPROCESSORS_ONLN);
    if (errno == 0 && coreNum > 0) {
        return coreNum;
    } else {
        NAVI_KERNEL_LOG(WARN,
                        "auto scale get core num failed, use default[%lu]",
                        DEFAULT_MAX_THREAD_NUMBER);
        return DEFAULT_MAX_THREAD_NUMBER;
    }
}

void NaviThreadPool::backgroundThread() {
    NaviLoggerScope scope(_logger);
    size_t counter = 0;
    while (_run) {
        if (0 == (counter % 128)) {
            updateActiveThreadCount();
        }
        checkTimeout();
        {
            autil::ScopedLock lock(_backgroundCond);
            _backgroundCond.wait(10 * 1000);
        }
        counter++;
    }
}

void NaviThreadPool::updateActiveThreadCount() {
    if (!_autoScale) {
        return;
    }
    assert(_configThreadNum <= 0);
    size_t activeCount = std::max(0, (int)getCoreNum() + _configThreadNum);
    activeCount = std::min(activeCount, _maxThreadNum);
    activeCount = std::max(activeCount, _minThreadNum);
    activeCount = std::max(activeCount, (size_t)1);
    if (activeCount != _activeThreadNum) {
        NAVI_KERNEL_LOG(
            INFO,
            "active thread count changed from [%lu] to [%lu] for auto scale, config [%d]",
            _activeThreadNum, activeCount, _configThreadNum);
        _activeThreadNum = activeCount;
        for (size_t i = 0; i < activeCount; i++) {
            auto &thread = _threads[i];
            auto &cond = thread.cond;
            if (0 == cond.trylock()) {
                auto &stat = thread.stat;
                if (TS_WAIT == stat) {
                    stat = TS_WAKEUP;
                    cond.signal();
                } else {
                    stat = TS_WAKEUP;
                }
                cond.unlock();
                break;
            }
        }
    }
}

void NaviThreadPool::checkTimeout() {
}

bool NaviThreadPool::incWorkerCount() {
    if (_accept) {
        atomic_inc(&_workerCount);
        return true;
    } else {
        return false;
    }
}

void NaviThreadPool::decWorkerCount() {
    atomic_dec(&_workerCount);
}

void NaviThreadPool::push(NaviThreadPoolItemBase *item) {
    if (unlikely(item->syncMode())) {
        NAVI_KERNEL_LOG(SCHEDULE3, "run sync WorkItem [%p]", item);
        item->process();
        item->destroy();
        return;
    }
    auto tid = getIdleTid();
    if (tid >= 0) {
        item->setSignalTid(_threads[tid].tid, getQueueSize());
    }
    NAVI_KERNEL_LOG(SCHEDULE3, "push WorkItem [%p], tid [%d], queueSize [%lu]", item, tid, getQueueSize());
    _scheduleQueue.Push(item);
    signal(tid);
}

int64_t NaviThreadPool::getIdleThreadCount() const {
    int64_t active = _activeThreadNum;
    int64_t idle = active - atomic_read(&_processingCount);
    return std::max((int64_t)0, idle);
}

int64_t NaviThreadPool::getRunningThreadCount() const {
    return atomic_read(&_processingCount);
}

size_t NaviThreadPool::getQueueSize() const {
    return _scheduleQueue.Size();
}

NaviThreadPoolItemBase *NaviThreadPool::pop() {
    NaviThreadPoolItemBase *item = nullptr;
    if (_scheduleQueue.Pop(&item)) {
        return item;
    } else {
        return nullptr;
    }
}

std::vector<pid_t> NaviThreadPool::getPidVec() const {
    std::vector<pid_t> vec;
    while ((int64_t)_threadNum != atomic_read(&_runningThread)) {
        usleep(100);
    }
    for (size_t i = 0; i < _threadNum; i++) {
        vec.push_back(_threads[i].tid);
    }
    return vec;
}

void NaviThreadPool::workLoop(int32_t tid) {
    current_thread_id = (long)syscall(SYS_gettid);
    _threads[tid].tid = current_thread_id;
    NAVI_MEMORY_BARRIER();
    atomic_inc(&_runningThread);
    NaviLoggerScope scope(_logger);
    while (_run) {
        auto item = pop();
        NAVI_KERNEL_LOG(SCHEDULE3, "thread pop [%d] [%p] queueSize [%lu]", tid, item, getQueueSize());
        if (item) {
            atomic_inc(&_processingCount);
            auto dequeueTime = CommonUtil::getTimelineTimeNs();
            item->setScheduleInfo(dequeueTime, atomic_read(&_processingCount),
                                  current_thread_id, current_thread_counter,
                                  current_thread_wait_counter);
            INLINE_DEPTH_TLS = 1;
            NAVI_KERNEL_LOG(SCHEDULE3, "begin process [%d] [%p] queueSize [%lu]", tid, item, getQueueSize());
            item->process();
            item->destroy();
            atomic_dec(&_processingCount);
            current_thread_counter++;
            NAVI_KERNEL_LOG(SCHEDULE3, "end process [%d] [%p] queueSize [%lu]", tid, item, getQueueSize());
            // malloc_trim(0);
        }
        if (unlikely(tid >= _activeThreadNum)) {
            // transfer to active thread
            signal(-1);
            if (!wait(tid)) {
                break;
            }
            NAVI_KERNEL_LOG(SCHEDULE3,
                            "thread continue running after suspend [%d] queueSize [%lu]", tid, getQueueSize());
        } else {
            if (!item) {
                if (!wait(tid)) {
                    break;
                }
                NAVI_KERNEL_LOG(SCHEDULE3, "thread continue running [%d] queueSize [%lu]", tid, getQueueSize());
            }
        }
    }
    atomic_dec(&_runningThread);
    INLINE_DEPTH_TLS = INVALID_INLINE_DEPTH;
}

}
