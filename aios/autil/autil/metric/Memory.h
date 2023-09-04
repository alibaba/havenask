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

#include <memory>
#include <stdint.h>
#include <string>

namespace autil {
namespace metric {

struct MemStat {
    uint64_t memTotal;
    uint64_t memFree;
    uint64_t buffers;
    uint64_t cached;
    uint64_t swapCached;
    uint64_t swapTotal;
    uint64_t swapFree;
    uint64_t active;
    uint64_t inActive;
    uint64_t dirty;
    uint64_t writeBack;
    uint64_t anonPages;
    uint64_t mapped;
    uint64_t activeAnon;
    uint64_t inActiveAnon;
    uint64_t activeFile;
    uint64_t inActiveFile;
    uint64_t unevictable;
    uint64_t mlocked;
    uint64_t shmem;
    uint64_t slab;
    uint64_t sReclaimable;
    uint64_t sUnreclaim;

    MemStat() { reset(); }

    void reset() {
        memTotal = 0;
        memFree = 0;
        buffers = 0;
        cached = 0;
        swapCached = 0;
        swapTotal = 0;
        swapFree = 0;
        active = 0;
        inActive = 0;
        dirty = 0;
        writeBack = 0;
        anonPages = 0;
        mapped = 0;
        activeAnon = 0;
        inActiveAnon = 0;
        activeFile = 0;
        inActiveFile = 0;
        unevictable = 0;
        mlocked = 0;
        shmem = 0;
        slab = 0;
        sReclaimable = 0;
        sUnreclaim = 0;
    }
};

class Memory {
public:
    Memory();
    ~Memory();

private:
    Memory(const Memory &);
    Memory &operator=(const Memory &);

private:
    static const std::string MEM_PROC_STAT;

public:
    void update();
    uint64_t getMemTotal() const { return _curStat.memTotal; }
    uint64_t getMemFree() const { return _curStat.memFree; }
    uint64_t getBuffers() const { return _curStat.buffers; }
    uint64_t getCached() const { return _curStat.cached; }
    uint64_t getSwapCached() const { return _curStat.swapCached; }
    uint64_t getSwapTotal() const { return _curStat.swapTotal; }
    uint64_t getSwapFree() const { return _curStat.swapFree; }
    uint64_t getActive() const { return _curStat.active; }
    uint64_t getInActive() const { return _curStat.inActive; }
    uint64_t getDirty() const { return _curStat.dirty; }
    uint64_t getWriteBack() const { return _curStat.writeBack; }
    uint64_t getAnonPages() const { return _curStat.anonPages; }
    uint64_t getMapped() const { return _curStat.mapped; }
    uint64_t getActiveAnon() const { return _curStat.activeAnon; }
    uint64_t getInActiveAnon() const { return _curStat.inActiveAnon; }
    uint64_t getActiveFile() const { return _curStat.activeFile; }
    uint64_t getInActiveFile() const { return _curStat.inActiveFile; }
    uint64_t getUnevictable() const { return _curStat.unevictable; }
    uint64_t getMlocked() const { return _curStat.mlocked; }
    uint64_t getShmem() const { return _curStat.shmem; }
    uint64_t getSlab() const { return _curStat.slab; }
    uint64_t getSReclaimable() const { return _curStat.sReclaimable; }
    uint64_t getSUnreclaim() const { return _curStat.sUnreclaim; }

private:
    void parseMemInfoLine(const char *str, MemStat &stat);
    void setMemInfoFile(const std::string &file);
    friend class MemoryTest;

private:
    MemStat _curStat;
    std::string _memInfoFile;
};

typedef std::shared_ptr<Memory> MemoryPtr;

} // namespace metric
} // namespace autil
