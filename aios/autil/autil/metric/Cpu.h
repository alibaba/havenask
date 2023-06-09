/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2019-07-24 10:47
 * Author Name: yixuan.ly
 * Author Email: yixuan.ly@alibaba-inc.com
 */
#pragma once

#include <stdint.h>
#include <memory>
#include <string>

namespace autil {
namespace metric {

typedef uint64_t jiffies_t;

struct CpuStat {
    jiffies_t user;
    jiffies_t nice;
    jiffies_t sys;
    jiffies_t idle;
    jiffies_t iow;
    jiffies_t irq;
    jiffies_t sirq;
    jiffies_t steal;
    jiffies_t guest;

    CpuStat() {
        reset();
    }
    void reset() {
        user = 0;
        nice = 0;
        sys = 0;
        idle = 0;
        iow = 0;
        irq = 0;
        sirq = 0;
        steal = 0;
        guest = 0;
    }
};

class Cpu
{
public:
    Cpu();
    ~Cpu();
private:
    Cpu(const Cpu &);
    Cpu& operator = (const Cpu &);
private:
    static const std::string CPU_PROC_STAT;
public:
    void update();
    jiffies_t getTotalDiff() {return _diffTotal;}
    inline double getUser() const;
    inline double getNice() const;
    inline double getSys() const;
    inline double getIdle() const;
    double getIoWait() const;
    inline double getIrq() const;
    inline double getSoftIrq() const;
private:
    void parseCpuStat(const char *statStr, CpuStat &cpuStat);
    void setProcStatFile(const std::string &file);
    friend class CpuTest;
private:
    CpuStat _prevStat;
    CpuStat _curStat;
    jiffies_t _diffTotal;
    std::string _procStatFile;
};

inline double Cpu::getUser() const {
    return (_curStat.user - _prevStat.user) / (double)_diffTotal * 100;
}
inline double Cpu::getNice() const {
    return (_curStat.nice - _prevStat.nice) / (double)_diffTotal * 100;
}
inline double Cpu::getSys() const {
    return (_curStat.sys - _prevStat.sys +
            _curStat.irq - _prevStat.irq +
            _curStat.sirq - _prevStat.sirq) / (double)_diffTotal * 100;
}
inline double Cpu::getIdle() const {
    return (_curStat.idle - _prevStat.idle) / (double)_diffTotal * 100;
}
inline double Cpu::getIrq() const {
    return (_curStat.irq - _prevStat.irq) / (double)_diffTotal * 100;
}
inline double Cpu::getSoftIrq() const {
    return (_curStat.sirq - _prevStat.sirq) / (double)_diffTotal * 100;
}

typedef std::shared_ptr<Cpu> CpuPtr;

}
}
