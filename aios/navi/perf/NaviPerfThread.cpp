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
#include "navi/perf/NaviPerfThread.h"
#include "navi/log/NaviLogger.h"
#include "navi/proto/GraphVis.pb.h"
#include "navi/util/CommonUtil.h"
#include <cxxabi.h>
#include <execinfo.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace navi {

constexpr uint32_t PAGE_SIZE = 4096;
constexpr uint32_t BUFFER_SIZE = 256 * PAGE_SIZE;

#define mb() asm volatile("mfence":::"memory")

NaviPerfThread::NaviPerfThread(pid_t pid)
    : _pid(pid)
    , _bufferSize(0)
    , _mmapSize(0)
    , _fd(-1)
    , _mem(MAP_FAILED)
{
}

NaviPerfThread::~NaviPerfThread() {
    if (-1 != _fd) {
        ::close(_fd);
    }
    if (MAP_FAILED != _mem) {
        munmap(_mem, _mmapSize);
    }
    auto result = _resultEndList;
    while (result) {
        result = result->stealPrev();
    }
}

bool NaviPerfThread::init(const NaviLoggerPtr &logger) {
    _logger.object = this;
    _logger.logger = logger;
    _logger.addPrefix("navi perf thread [%d]", _pid);
    initPerfAttr();
    static_assert(0 == (BUFFER_SIZE & (BUFFER_SIZE - 1)),
                  "buffer size should be power of two");
    _bufferSize = BUFFER_SIZE;
    _mmapSize = _bufferSize + PAGE_SIZE;
    _fd = perf_event_open(-1, -1, PERF_FLAG_FD_CLOEXEC);
    if (-1 == _fd) {
        NAVI_LOG(ERROR, "open perf fd for pid [%d] failed, error [%s]", _pid,
                 strerror(errno));
        return false;
    }
    _mem = mmap(0, _mmapSize, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);
    if (MAP_FAILED == _mem) {
        NAVI_LOG(ERROR,
                 "mmap perf mem for pid [%d] failed, fd [%d], err [%s]", _pid,
                 _fd, strerror(errno));
        return false;
    }
    ioctl(_fd, PERF_EVENT_IOC_RESET, 0);
    return true;
}

int NaviPerfThread::getFd() const {
    return _fd;
}

void NaviPerfThread::initPerfAttr() {
    memset(&_attr, 0, sizeof(perf_event_attr));
    _attr.size = sizeof(perf_event_attr);
    _attr.disabled = 1;
    _attr.type = PERF_TYPE_HARDWARE;
    _attr.config = PERF_COUNT_HW_REF_CPU_CYCLES;
    _attr.sample_period = 60000;
    _attr.sample_type = NaviPerfEvent::SAMPLE_TYPE |
                        NaviPerfBranchEntry::SAMPLE_TYPE |
                        NaviPerfCallchainEntry::SAMPLE_TYPE;
    _attr.branch_sample_type = NaviPerfBranchEntry::BRANCH_SAMPLE_TYPE;
    _attr.wakeup_events = 1;
    _attr.use_clockid = 1;
    _attr.clockid = CLOCK_MONOTONIC;
    _attr.exclude_callchain_kernel = 1;
    _attr.exclude_guest = 1;
    _attr.context_switch = 1;
    _attr.sample_id_all = 1;
}

int NaviPerfThread::perf_event_open(int cpu, int groupFd,
                                    unsigned long flags)
{
    return syscall(__NR_perf_event_open, &_attr, _pid, cpu, groupFd, flags);
}

void NaviPerfThread::flushLBR(int32_t depth) {
    if (++depth < 32) {
        flushLBR(depth);
    }
    NAVI_MEMORY_BARRIER();
}

bool NaviPerfThread::enable() {
    int ret = ioctl(_fd, PERF_EVENT_IOC_ENABLE, 0);
    if (0 != ret) {
        NAVI_LOG(ERROR, "begin thread sample error");
    }
    flushLBR(0);
    return ret;
}

bool NaviPerfThread::disable() {
    return ioctl(_fd, PERF_EVENT_IOC_DISABLE, 0);
}

bool NaviPerfThread::beginSample() {
    NaviPerfResultPtr newResult(new NaviPerfResult());
    pushQueue(_resultQueue, newResult);
    newResult->setPrev(_resultEndList);
    _resultEndList = newResult;
    NAVI_LOG(SCHEDULE2, "begin thread sample");
    return true;
}

NaviPerfResultPtr NaviPerfThread::endSample() {
    if (!_resultEndList) {
        return nullptr;
    }
    auto retResult = _resultEndList;
    _resultEndList = _resultEndList->stealPrev();
    if (retResult) {
        retResult->stop();
    }
    NAVI_LOG(SCHEDULE2, "end thread sample, %lu", retResult->getEventCount());
    return retResult;
}

void NaviPerfThread::sample() {
    perf_event_mmap_page *header = (perf_event_mmap_page *)_mem;
    auto ptr = &header->data_head;
    uint64_t data_head = *(volatile uint64_t *)ptr;
    mb();
    uint64_t data_tail = header->data_tail;
    uint8_t *bufferStart = ((uint8_t *)_mem) + PAGE_SIZE;
    uint8_t *bufferEnd = ((uint8_t *)_mem) + _mmapSize;
    NAVI_LOG(SCHEDULE3, "begin sample loop, pid [%d]", _pid);
    int64_t count = 0;
    while (data_head != data_tail) {
        uint8_t *sampleBase = bufferStart + (data_tail & (_bufferSize - 1));
        auto sample = (perf_event_header *)sampleBase;
        auto sampleSize = sample->size;
        uint32_t naviEventSize = sampleSize - sizeof(perf_event_header);
        uint8_t *copyBase =
            bufferStart +
            ((data_tail + sizeof(perf_event_header)) & (_bufferSize - 1));
        auto entryType = PET_UNKNOWN;
        if (sample->type == PERF_RECORD_SAMPLE) {
            entryType = PET_SAMPLE;
        } else if (sample->type == PERF_RECORD_THROTTLE) {
            entryType = PET_THROTTLE;
        } else if (sample->type == PERF_RECORD_UNTHROTTLE) {
            entryType = PET_UNTHROTTLE;
        } else if (sample->type == PERF_RECORD_SWITCH) {
            if (sample->misc & PERF_RECORD_MISC_SWITCH_OUT) {
                entryType = PET_SWITCH_OUT;
            } else {
                entryType = PET_SWITCH_IN;
            }
        } else {
            NAVI_LOG(ERROR, "unknown sample [%d]", sample->type);
            data_tail += sampleSize;
            continue;
        }
        auto entry = (NaviPerfEventEntry *)malloc(
            naviEventSize + offsetof(NaviPerfEventEntry, data));
        new (entry) NaviPerfEventEntry();
        entry->sampleType = entryType;
        uint8_t *dataStart = sampleBase + sizeof(perf_event_header);
        uint8_t *dataEnd = dataStart + naviEventSize;
        if (dataEnd < bufferEnd || dataStart >= bufferEnd) {
            memcpy(&entry->data, copyBase, naviEventSize);
        } else {
            uint8_t *dest = (uint8_t *)&entry->data;
            size_t len1 = bufferEnd - copyBase;
            memcpy(dest, dataStart, len1);
            size_t len2 = naviEventSize - len1;
            memcpy(dest + len1, bufferStart, len2);
        }
        if (sample->type == PERF_RECORD_SAMPLE) {
            size_t callchainSize = 0;
            if (_attr.sample_type & NaviPerfCallchainEntry::SAMPLE_TYPE) {
                entry->callchainEntry = (NaviPerfCallchainEntry *)(((char *)&entry->data) + sizeof(NaviPerfEvent));
                callchainSize = 8 * (entry->callchainEntry->nr + 1);
            }
            entry->branchEntry
                = (NaviPerfBranchEntry *)(((char *)&entry->data) + sizeof(NaviPerfEvent) + callchainSize);
        }
        NAVI_LOG(SCHEDULE3, "sample time: %lu, clock: %ld, pid: %d",
                 NaviPerfResult::getSampleTime(entry),
                 autil::TimeUtility::currentTime(), _pid);
        collectResultEntry(entry);
        data_tail += sampleSize;
        NAVI_LOG(SCHEDULE3, "sample count: %ld", ++count);
    }
    mb();
    ptr = &header->data_tail;
    *(volatile uint64_t *)ptr = data_tail;
    NAVI_LOG(SCHEDULE3, "end sample loop, pid [%d]", _pid);
}

void NaviPerfThread::pushQueue(
    arpc::common::LockFreeQueue<NaviPerfResultPtr> &queue,
    const NaviPerfResultPtr &newResult)
{
    queue.Push(newResult);
}

NaviPerfResultPtr NaviPerfThread::popQueue(
    arpc::common::LockFreeQueue<NaviPerfResultPtr> &queue) const
{
    NaviPerfResultPtr ret;
    if (queue.Pop(&ret)) {
        return ret;
    } else {
        return nullptr;
    }
}

void NaviPerfThread::collectSubResult() {
    NaviPerfResultPtr current = _resultQueueTail;
    auto next = popQueue(_resultQueue);
    while (next) {
        if (!current || !current->addSubResult(next)) {
            pushQueue(_mainQueue, next);
            current = next;
        }
        next = popQueue(_resultQueue);
    }
    _resultQueueTail = current;
}

int32_t NaviPerfThread::collectResultEntry(NaviPerfEventEntry *entry) {
    int32_t ret = 0;
    collectSubResult();
    NaviPerfResultPtr current;
    if (!_mainQueueHead) {
        current = popQueue(_mainQueue);
    } else {
        current = _mainQueueHead;
    }
    if (!current) {
        free(entry);
        return ret;
    }
    while (current) {
        ret = current->addEntry(entry);
        if (-1 == ret) {
            // entry too old
            NAVI_LOG(SCHEDULE2, "entry dropped, time [%lu]",
                     NaviPerfResult::getSampleTime(entry));
            free(entry);
            entry = nullptr;
            break;
        } else if (0 == ret) {
            // fail
            current = popQueue(_mainQueue);
        } else if (1 == ret) {
            // success
            entry = nullptr;
            break;
        } else {
            assert(false);
            break;
        }
    }
    if (entry) {
        free(entry);
    }
    _mainQueueHead = current;
    return ret;
}

}
