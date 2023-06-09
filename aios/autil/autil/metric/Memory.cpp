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
#include "autil/metric/Memory.h"

#include <string.h>
#include <stdlib.h>
#include <fstream>

#include "autil/metric/MetricUtil.h"

using namespace std;

namespace autil {
namespace metric {

const string Memory::MEM_PROC_STAT("/proc/meminfo");

Memory::Memory() { 
    _memInfoFile = MEM_PROC_STAT;
}

Memory::~Memory() { 
}

void Memory::update() {
    ifstream fin(_memInfoFile.c_str());
    
    string line;
    while (std::getline(fin, line)) {
        parseMemInfoLine(line.c_str(), _curStat);
    }
}

void Memory::parseMemInfoLine(const char *str, MemStat &stat) {
#define EXTRACT_FIELD(field, value) {                   \
        if (!strncmp(str, field, strlen(field))) {      \
            const char *p = MetricUtil::skipToken(str); \
            value = strtol(p, NULL, 10);                \
            return;                                     \
        }                                               \
    }
    EXTRACT_FIELD("MemTotal", stat.memTotal);
    EXTRACT_FIELD("MemFree", stat.memFree);
    EXTRACT_FIELD("Buffers", stat.buffers);
    EXTRACT_FIELD("Cached", stat.cached);
    EXTRACT_FIELD("SwapCached", stat.swapCached);
    EXTRACT_FIELD("SwapTotal", stat.swapTotal);
    EXTRACT_FIELD("SwapFree", stat.swapFree);
    EXTRACT_FIELD("Dirty", stat.dirty);
    EXTRACT_FIELD("Writeback", stat.writeBack);
    EXTRACT_FIELD("AnonPages", stat.anonPages);
    EXTRACT_FIELD("Mapped", stat.mapped);
    //if a is substr of b, so a should extract after b 
    EXTRACT_FIELD("Active(anon)", stat.activeAnon);
    EXTRACT_FIELD("Active(file)", stat.activeFile);
    EXTRACT_FIELD("Active", stat.active);
    EXTRACT_FIELD("Inactive(anon)", stat.inActiveAnon);
    EXTRACT_FIELD("Inactive(file)", stat.inActiveFile);
    EXTRACT_FIELD("Inactive", stat.inActive);

    EXTRACT_FIELD("Unevictable", stat.unevictable);
    EXTRACT_FIELD("Mlocked", stat.mlocked);
    EXTRACT_FIELD("Shmem", stat.shmem);
    EXTRACT_FIELD("Slab", stat.slab);
    EXTRACT_FIELD("SReclaimable", stat.sReclaimable);
    EXTRACT_FIELD("SUnreclaim", stat.sUnreclaim);

#undef EXTRACT_FIELD
}

void Memory::setMemInfoFile(const string &file) {
    _memInfoFile = file;
}

}
}

