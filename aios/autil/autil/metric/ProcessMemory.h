/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2019-07-24 10:47
 * Author Name: yixuan.ly
 * Author Email: yixuan.ly@alibaba-inc.com
 */

#pragma once

#include <stdint.h>
#include <memory>
#include <sstream>

namespace autil {
namespace metric {

class ProcessMemory
{
public:
    ProcessMemory();
    ~ProcessMemory();
private:
    ProcessMemory(const ProcessMemory &);
    ProcessMemory& operator = (const ProcessMemory &);
public:
    void update();
    uint64_t getMemSize() {return _memSize;}
    uint64_t getMemRss() {return _memRss;}
    double getMemRssRatio() {
        double ratio = 0;
        if (_machineMemSize != 0) {
            ratio = 100.0 * getMemRss() / _machineMemSize;
        }
        return ratio;
    }
    uint64_t getMemSwap() {return _memSwap;}
    uint64_t getMemUsed() {return _memRss + _memSwap;}
    double getMemUsedRatio() {
        double ratio = 0;
        if (_machineMemSize != 0) {
            ratio = 100.0 * getMemUsed() / _machineMemSize;
        }
        return ratio;
    }

private:
    void parseProcessMemStatus(std::istream &in);
    void parseMachineMemSize(std::istream &in);

private:
    // 单位都是KB
    uint64_t _machineMemSize;
    uint64_t _memSize;
    uint64_t _memRss;
    uint64_t _memSwap;
};

typedef std::shared_ptr<ProcessMemory> ProcessMemoryPtr;

}
}
