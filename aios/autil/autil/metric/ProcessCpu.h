/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2019-07-24 10:47
 * Author Name: yixuan.ly
 * Author Email: yixuan.ly@alibaba-inc.com
 */

#pragma once

#include <memory>
#include <string>

#include "autil/metric/Cpu.h"

namespace autil {
namespace metric {

struct ProcessCpuStat {
    jiffies_t utime;
    jiffies_t stime;
    jiffies_t cutime;
    jiffies_t cstime;

    ProcessCpuStat() { reset(); }
    void reset() {
        utime = 0;
        stime = 0;
        cutime = 0;
        cstime = 0;
    }
};

class ProcessCpu {
public:
    ProcessCpu();
    ~ProcessCpu();

private:
    ProcessCpu(const ProcessCpu &);
    ProcessCpu &operator=(const ProcessCpu &);

public:
    double getUsage();

private:
    jiffies_t getProcessTime();
    void parseProcCPUStat(std::string line, jiffies_t &processCpuTotTime);
    void getProcessCpuStat();
    friend class ProcessCpuTest;

private:
    Cpu _cpu;
    jiffies_t _curProcessCpuTotTime;
    jiffies_t _preProcessCpuTotTime;
};

typedef std::shared_ptr<ProcessCpu> ProcessCpuPtr;

} // namespace metric
} // namespace autil
