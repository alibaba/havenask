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
#include <linux/perf_event.h>
#include <unordered_map>

namespace navi {

class PerfEventDef;
class KernelPerfDef;
struct NaviPerfCallchainEntry;
struct NaviPerfBranchEntry;

// see man perf_event_open
struct NaviPerfEvent {
public:
    void fillProto(PerfEventDef *perfEvent,
                   NaviPerfCallchainEntry *callchainEntry,
                   NaviPerfBranchEntry *branchEntry,
                   std::unordered_map<uint64_t, uint32_t> &addrMap) const;
private:
    void fillAddr(uint64_t ip, PerfEventDef *perfEvent,
                  std::unordered_map<uint64_t, uint32_t> &addrMap) const;
public:
    uint64_t ip;       /* if PERF_SAMPLE_IP */
    uint32_t pid, tid; /* if PERF_SAMPLE_TID */
    uint64_t time;     /* if PERF_SAMPLE_TIME */
    uint32_t cpu, res; /* if PERF_SAMPLE_CPU */
public:
    static constexpr uint32_t SAMPLE_TYPE =
        PERF_SAMPLE_IP | PERF_SAMPLE_TID | PERF_SAMPLE_TIME | PERF_SAMPLE_CPU;
};

struct NaviPerfCallchainEntry {
    uint64_t nr;
    uint64_t ips[0];
public:
    static constexpr uint64_t SAMPLE_TYPE = PERF_SAMPLE_CALLCHAIN;
};

struct NaviPerfBranchEntry {
    uint64_t bnr;                    /* if PERF_SAMPLE_BRANCH_STACK */
    struct perf_branch_entry lbr[0]; /* if PERF_SAMPLE_BRANCH_STACK */
public:
    static constexpr uint64_t SAMPLE_TYPE = PERF_SAMPLE_BRANCH_STACK;
    static constexpr uint64_t BRANCH_SAMPLE_TYPE =
        PERF_SAMPLE_BRANCH_CALL_STACK | PERF_SAMPLE_BRANCH_USER |
        PERF_SAMPLE_BRANCH_NO_FLAGS | PERF_SAMPLE_BRANCH_NO_CYCLES;
};

struct NaviPerfEventThrottle {
public:
    void fillProto(PerfEventDef *perfEvent) const;
public:
    uint64_t time;
    uint64_t id;
    uint64_t stream_id;
};

struct NaviPerfEventSwitch {
public:
    void fillProto(PerfEventDef *perfEvent) const;
public:
    uint32_t pid, tid; /* if PERF_SAMPLE_TID */
    uint64_t time;     /* if PERF_SAMPLE_TIME */
    uint32_t cpu, res; /* if PERF_SAMPLE_CPU */
};

struct NaviPerfEventEntry {
public:
    NaviPerfEventEntry()
        : next(nullptr)
        , sampleType(0)
        , branchEntry(nullptr)
        , callchainEntry(nullptr)
    {
    }
public:
    NaviPerfEventEntry *next;
    int32_t sampleType;
    NaviPerfBranchEntry *branchEntry;
    NaviPerfCallchainEntry *callchainEntry;
    union {
        char data[0];
        NaviPerfEvent normalEvent;
        NaviPerfEventThrottle throttleEvent;
        NaviPerfEventSwitch switchEvent;
    };
};

class NaviPerfThread;
class NaviPerfResult;
NAVI_TYPEDEF_PTR(NaviPerfResult);

class NaviPerfResult
{
public:
    NaviPerfResult();
    ~NaviPerfResult();
private:
    NaviPerfResult(const NaviPerfResult &);
    NaviPerfResult &operator=(const NaviPerfResult &);
public:
    void setPrev(const NaviPerfResultPtr &prev);
    NaviPerfResultPtr stealPrev();
    bool addSubResult(const NaviPerfResultPtr &sub);
    int addEntry(NaviPerfEventEntry *entry);
    void stop();
    size_t getEventCount() const;
    void fillProto(KernelPerfDef *perf,
                   std::unordered_map<uint64_t, uint32_t> &addrMap) const;
public:
    static uint64_t getSampleTime(NaviPerfEventEntry *entry);
private:
    void addToEntryList(NaviPerfEventEntry *entry);    
private:
    friend class NaviPerfThread;
private:
    int64_t _beginTime;
    int64_t _endTime;
    NaviPerfResultPtr _sub;
    NaviPerfResultPtr _prev;
    size_t _eventCount;
    NaviPerfEventEntry *_head;
    NaviPerfEventEntry *_tail;
};

}

