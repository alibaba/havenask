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
#include "autil/metric/ProcessMemory.h"

#include <fstream> // IWYU pragma: keep
#include <stdlib.h>
#include <string.h>
#include <string>
#include <unistd.h>

#include "autil/metric/MetricUtil.h"

using namespace std;

namespace autil {
namespace metric {

ProcessMemory::ProcessMemory() {
    _machineMemSize = 0;
    _memSize = 0;
    _memRss = 0;
    _memSwap = 0;
}

ProcessMemory::~ProcessMemory() {}

void ProcessMemory::update() {
    ifstream memInfoIn("/proc/meminfo");
    if (memInfoIn) {
        parseMachineMemSize(memInfoIn);
    }

    stringstream ss;
    pid_t pid = getpid();
    ss << "/proc/" << pid << "/status";
    string filePath = ss.str();
    ifstream memStatsIn(filePath.c_str());
    if (memStatsIn) {
        parseProcessMemStatus(memStatsIn);
    }
}

void ProcessMemory::parseProcessMemStatus(istream &in) {
    string line;
    int32_t flag = 3;
    const char *token1 = "VmSize";
    const char *token2 = "VmRSS";
    const char *token3 = "VmSwap";
    while (flag > 0 && std::getline(in, line)) {
        const char *str = line.c_str();
        str = MetricUtil::skipWhite(str);
        if (str == NULL) {
            continue;
        }

        if (!strncmp(str, token1, strlen(token1))) {
            const char *p = MetricUtil::skipToken(str);
            _memSize = strtoul(p, NULL, 10);
            flag--;
            continue;
        }

        if (!strncmp(str, token2, strlen(token2))) {
            const char *p = MetricUtil::skipToken(str);
            _memRss = strtoul(p, NULL, 10);
            flag--;
            continue;
        }

        if (!strncmp(str, token3, strlen(token3))) {
            const char *p = MetricUtil::skipToken(str);
            _memSwap = strtoul(p, NULL, 10);
            flag--;
            continue;
        }
    }
}

void ProcessMemory::parseMachineMemSize(istream &in) {
    string line;
    const char *token = "MemTotal";
    while (std::getline(in, line)) {
        const char *str = line.c_str();
        str = MetricUtil::skipWhite(str);
        if (str == NULL) {
            continue;
        }
        if (!strncmp(str, token, strlen(token))) {
            const char *p = MetricUtil::skipToken(str);
            _machineMemSize = strtoul(p, NULL, 10);
            break;
        }
    }
}

} // namespace metric
} // namespace autil
