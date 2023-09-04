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
#include "navi/perf/NaviPerfResult.h"
#include "navi/proto/GraphVis.pb.h"
#include "navi/util/CommonUtil.h"

namespace navi {

void NaviPerfEventThrottle::fillProto(PerfEventDef *perfEvent) const {
    perfEvent->set_time(time);
}

void NaviPerfEvent::fillProto(PerfEventDef *perfEvent,
                              NaviPerfCallchainEntry *callchainEntry,
                              NaviPerfBranchEntry *branchEntry,
                              std::unordered_map<uint64_t, uint32_t> &addrMap) const
{
    perfEvent->set_pid(pid);
    perfEvent->set_tid(tid);
    perfEvent->set_time(time);
    perfEvent->set_cpu(cpu);
    int64_t branchNum = branchEntry->bnr;
    int64_t matchIndex = -1;
    int64_t callchainNum = callchainEntry ? callchainEntry->nr : 0;
    if (branchNum == 0) {
        matchIndex = 0;
    } else {
        uint64_t matchIp = branchEntry->lbr[branchNum - 1].from;
        for (int64_t i = 1; i < callchainNum; i++) {
            int64_t diff = matchIp - callchainEntry->ips[i];
            if (abs(diff) <= 5) {
                matchIndex = i;
                break;
            }
        }
        auto ip0 = branchEntry->lbr[0].to;
        fillAddr(ip0, perfEvent, addrMap);
        for (int64_t i = 0; i < branchNum; i++) {
            auto ip = branchEntry->lbr[i].from;
            fillAddr(ip, perfEvent, addrMap);
        }
    }
    if (matchIndex >= 0) {
        for (int64_t i = matchIndex + 1; i < callchainNum; ++i) {
            auto ip = callchainEntry->ips[i];
            fillAddr(ip, perfEvent, addrMap);
        }
    }
}

void NaviPerfEvent::fillAddr(
    uint64_t ip, PerfEventDef *perfEvent,
    std::unordered_map<uint64_t, uint32_t> &addrMap) const
{
    uint32_t index = 0;
    auto it = addrMap.find(ip);
    if (addrMap.end() == it) {
        index = addrMap.size();
        addrMap.emplace(ip, index);
    } else {
        index = it->second;
    }
    perfEvent->add_addr(index);
}

void NaviPerfEventSwitch::fillProto(PerfEventDef *perfEvent) const {
    perfEvent->set_pid(pid);
    perfEvent->set_tid(tid);
    perfEvent->set_time(time);
    perfEvent->set_cpu(cpu);
}

NaviPerfResult::NaviPerfResult()
    : _beginTime(CommonUtil::getTimelineTimeNs())
    , _endTime(_beginTime)
    , _eventCount(0)
    , _head(nullptr)
    , _tail(nullptr)
{
}

NaviPerfResult::~NaviPerfResult() {
    auto entry = _head;
    while (entry) {
        auto next = entry->next;
        free(entry);
        entry = next;
    }
}

void NaviPerfResult::stop() {
    _endTime = CommonUtil::getTimelineTimeNs();
}

void NaviPerfResult::fillProto(KernelPerfDef *perf,
                               std::unordered_map<uint64_t, uint32_t> &addrMap) const
{
    auto entry = _head;
    while (entry) {
        auto perfEvent = perf->add_events();
        switch (entry->sampleType) {
        case PET_SAMPLE:
            entry->normalEvent.fillProto(perfEvent, entry->callchainEntry,
                                         entry->branchEntry, addrMap);
            break;
        case PET_THROTTLE:
        case PET_UNTHROTTLE:
            entry->throttleEvent.fillProto(perfEvent);
            break;
        case PET_SWITCH_IN:
        case PET_SWITCH_OUT:
            entry->switchEvent.fillProto(perfEvent);
            break;
        case PET_UNKNOWN:
        default:
            assert(false);
            break;
        }
        perfEvent->set_sample_type((PerfEventType)entry->sampleType);
        entry = entry->next;
    }
}

bool NaviPerfResult::addSubResult(const NaviPerfResultPtr &sub) {
    if (_sub) {
        return _sub->addSubResult(sub);
    }
    if (_endTime == _beginTime) {
        if (sub->_beginTime > _beginTime) {
            _sub = sub;
            return true;
        }
    } else {
        if (sub->_beginTime > _beginTime && sub->_endTime < _endTime) {
            _sub = sub;
            return true;
        }
    }
    return false;
}

int NaviPerfResult::addEntry(NaviPerfEventEntry *entry) {
    if (_sub) {
        auto ret = _sub->addEntry(entry);
        switch (ret) {
        case -1:
            // entry too old
            break;
        case 0:
            // fail
            _sub.reset();
            break;
        case 1:
            // success
            return 1;
        default:
            assert(false);
            break;
        }
    }
    auto sampleTime = getSampleTime(entry);
    if (sampleTime < _beginTime) {
        return -1;
    } else if (_beginTime == _endTime || sampleTime < _endTime) {
        addToEntryList(entry);
        return 1;
    } else {
        return 0;
    }
}

void NaviPerfResult::addToEntryList(NaviPerfEventEntry *entry) {
    if (!_head) {
        _head = entry;
        _tail  = entry;
    } else {
        _tail->next = entry;
        _tail = entry;
    }
    _eventCount++;
}

void NaviPerfResult::setPrev(const NaviPerfResultPtr &prev) {
    _prev = prev;
}

NaviPerfResultPtr NaviPerfResult::stealPrev() {
    return std::move(_prev);
}

size_t NaviPerfResult::getEventCount() const {
    return _eventCount;
}

uint64_t NaviPerfResult::getSampleTime(NaviPerfEventEntry *entry) {
    switch (entry->sampleType) {
    case PET_SAMPLE:
        return entry->normalEvent.time;
    case PET_THROTTLE:
    case PET_UNTHROTTLE:
        return entry->throttleEvent.time;
    case PET_SWITCH_IN:
    case PET_SWITCH_OUT:
        return entry->switchEvent.time;
    case PET_UNKNOWN:
    default:
        assert(false);
        return 0;
    }
}

}

