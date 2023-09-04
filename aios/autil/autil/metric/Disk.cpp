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
#include "autil/metric/Disk.h"

#include <fstream>
#include <stdlib.h>
#include <string.h>

#include "autil/TimeUtility.h"
#include "autil/metric/MetricUtil.h"

#define DELTA 0.000001

using namespace std;

namespace autil {
namespace metric {

const string Disk::DISK_PROC_STAT("/proc/diskstats");
Disk::Disk() {
    _statsFile = DISK_PROC_STAT;
    _timeDiff = 0;
}

Disk::~Disk() {}

void Disk::update() {
    _prevStat = _curStat;
    _curStat.reset();
    _curStat.updateTime = TimeUtility::currentTime();
    ifstream fin(_statsFile.c_str());
    string line;
    while (std::getline(fin, line)) {
        parseDiskStat(line.c_str(), _curStat);
    }
    _timeDiff = (_curStat.updateTime - _prevStat.updateTime) / (double)1000000.0;

    adjust();
}

void Disk::adjust() {
    if (_timeDiff < DELTA) {
        _timeDiff = DELTA;
    }
    if (_curStat.reads < _prevStat.reads) {
        _curStat.reads = _prevStat.reads;
    }
    if (_curStat.readsMgd < _prevStat.readsMgd) {
        _curStat.readsMgd = _prevStat.readsMgd;
    }
    if (_curStat.readsSects < _prevStat.readsSects) {
        _curStat.readsSects = _prevStat.readsSects;
    }
    if (_curStat.writes < _prevStat.writes) {
        _curStat.writes = _prevStat.writes;
    }
    if (_curStat.writesMgd < _prevStat.writesMgd) {
        _curStat.writesMgd = _prevStat.writesMgd;
    }
    if (_curStat.writesSects < _prevStat.writesSects) {
        _curStat.writesSects = _prevStat.writesSects;
    }
    if (_curStat.current < _prevStat.current) {
        _curStat.current = _prevStat.current;
    }
}

/*
  /proc/diststats
  8    0 sda 102565 18526 2736611 1407028 13504206 62919269 611496896 1542334754 0 49788906 1543822002

  Field  1 -- # of reads completed
  Field  2 -- # of reads merged,
  Field  3 -- # of sectors read
  Field  4 -- # of milliseconds spent reading
  Field  5 -- # of writes completed
  Field  6 -- # of writes merged
  Field  7 -- # of sectors written
  Field  8 -- # of milliseconds spent writing
  Field  9 -- # of I/Os currently in progress
  Field 10 -- # of milliseconds spent doing I/Os
  Field 11 -- weighted # of milliseconds spent doing I/Os
*/

const char *Disk::skipAndFilter(
    const char *str, const char *pattern, int32_t filterReverseIdx, char filterLeftRange, char filterRightRange) {
    const char *p = strstr(str, pattern);
    if (p == NULL) {
        return NULL;
    }
    // skip disk name
    p = MetricUtil::skipToken(p);
    char tmp = *(p - filterReverseIdx);
    if (tmp >= filterLeftRange && tmp <= filterRightRange) {
        // filter partition
        return NULL;
    }
    return p;
}

void Disk::parseDiskStat(const char *str, DiskStat &diskStat) {
    const char *p = skipAndFilter(str, "sd", 1, '0', '9');
    if (p == NULL) {
        p = skipAndFilter(str, "df", 1, '0', '9');
    }
    if (p == NULL) {
        p = skipAndFilter(str, "nvme", 2, 'p', 'p');
    }
    if (p == NULL) {
        p = skipAndFilter(str, "cciss", 2, 'p', 'p');
    }
    if (p == NULL) {
        return;
    }

#define EXTRACT_FILED(field)                                                                                           \
    {                                                                                                                  \
        diskStat.field += strtoul(p, NULL, 10);                                                                        \
        p = MetricUtil::skipToken(p);                                                                                  \
    }

    EXTRACT_FILED(reads);
    EXTRACT_FILED(readsMgd);
    EXTRACT_FILED(readsSects);
    p = MetricUtil::skipToken(p); // skip filed #4
    EXTRACT_FILED(writes);
    EXTRACT_FILED(writesMgd);
    EXTRACT_FILED(writesSects);
    p = MetricUtil::skipToken(p); // skip filed #8
    EXTRACT_FILED(current);
#undef EXTRACT_FILED
}

void Disk::setDiskStatsFile(const string &file) { _statsFile = file; }

} // namespace metric
} // namespace autil
