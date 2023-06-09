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
#include "autil/metric/ProcessCpu.h"

#include <unistd.h>
#include <stdint.h>
#include <stdio.h>
#include <fstream> // IWYU pragma: keep
#include <sstream> // IWYU pragma: keep

using namespace std;

namespace autil {
namespace metric {

ProcessCpu::ProcessCpu() { 
    _curProcessCpuTotTime = 0;
    _preProcessCpuTotTime = 0;
}

ProcessCpu::~ProcessCpu() { 
}

double ProcessCpu::getUsage() {
    _cpu.update();
    _preProcessCpuTotTime = _curProcessCpuTotTime;
    getProcessCpuStat();
    jiffies_t totalTime = _cpu.getTotalDiff();
    jiffies_t processTime = getProcessTime();
    if (processTime >= totalTime) {
        return 100.0;
    }
    return processTime / (double)totalTime * 100.0;
}

void ProcessCpu::getProcessCpuStat() {
    char filePath[1024];
    pid_t pid = getpid();
    snprintf(filePath, sizeof(filePath), "/proc/%d/stat", pid);

    ifstream in(filePath);
    if (!in) {
        return;
    }

    string line;
    std::getline(in, line);
    return parseProcCPUStat(line, _curProcessCpuTotTime);
}

void ProcessCpu::parseProcCPUStat(string line, 
                                  jiffies_t &processCpuTotTime)
{
    /*
     * the line string in /proc/[pid]/stat is:
     * pid %d, comm %s, state %c, ppid %d, pgrp %d, session %d,
     * tty_nr %d, tpgid %d, flags %u, minflt %lu, cminflt %lu,
     * majflt %lu, cmajflt %lu,
     * utime %lu, stime %lu, cutime %ld, cstime %ld,
     * ...
     */
    struct ProcessCpuStat processCpuStat;
    size_t pos = 0;
    for (int32_t i = 0; i < 13 && pos != string::npos; i++) {
        pos = line.find(" ", pos) + 1;
    }

    if (pos == string::npos) {
        return;
    }

    line = line.substr(pos);

    istringstream iss(line);
    iss >> processCpuStat.utime >> processCpuStat.stime
        >> processCpuStat.cutime >> processCpuStat.cstime;
    processCpuTotTime = processCpuStat.utime + processCpuStat.stime +
                        processCpuStat.cutime + processCpuStat.cstime;
}

jiffies_t ProcessCpu::getProcessTime() {
    if (_curProcessCpuTotTime <= _preProcessCpuTotTime) {
        return 0;
    }
    return _curProcessCpuTotTime - _preProcessCpuTotTime;
}

}
}

