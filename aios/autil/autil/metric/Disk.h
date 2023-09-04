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

struct DiskStat {
    double reads;
    double readsMgd;
    double readsSects;
    double writes;
    double writesMgd;
    double writesSects;
    double current;
    uint64_t updateTime;

    DiskStat() { reset(); }
    void reset() {
        reads = 0.0;
        readsMgd = 0.0;
        readsSects = 0.0;
        writes = 0.0;
        writesMgd = 0.0;
        writesSects = 0.0;
        current = 0.0;
        updateTime = 0;
    }
};

class Disk {
public:
    Disk();
    ~Disk();

private:
    static const std::string DISK_PROC_STAT;

private:
    Disk(const Disk &);
    Disk &operator=(const Disk &);

public:
    void update();
    inline double getReads() const;
    inline double getReadsMgd() const;
    inline double getReadsKb() const;
    inline double getWrites() const;
    inline double getWritesMgd() const;
    inline double getWritesKb() const;
    inline double getCurrentIO() const;

private:
    void adjust();
    void setDiskStatsFile(const std::string &file);
    void parseDiskStat(const char *str, DiskStat &diskStat);
    const char *skipAndFilter(
        const char *str, const char *pattern, int32_t filterReverseIdx, char filterLeftRange, char filterRightRange);

    // for test
    void setTimeDiff(double diff) { _timeDiff = diff; }
    friend class DiskTest;

private:
    DiskStat _prevStat;
    DiskStat _curStat;
    double _timeDiff; // s
    std::string _statsFile;
};

inline double Disk::getReads() const { return (_curStat.reads - _prevStat.reads) / _timeDiff; }
inline double Disk::getReadsMgd() const { return (_curStat.readsMgd - _prevStat.readsMgd) / _timeDiff; }
inline double Disk::getReadsKb() const { return (_curStat.readsSects - _prevStat.readsSects) * 0.5 / _timeDiff; }
inline double Disk::getWrites() const { return (_curStat.writes - _prevStat.writes) / _timeDiff; }
inline double Disk::getWritesMgd() const { return (_curStat.writesMgd - _prevStat.writesMgd) / _timeDiff; }
inline double Disk::getWritesKb() const { return (_curStat.writesSects - _prevStat.writesSects) * 0.5 / _timeDiff; }
inline double Disk::getCurrentIO() const { return _curStat.current; }

typedef std::shared_ptr<Disk> DiskPtr;

} // namespace metric
} // namespace autil
