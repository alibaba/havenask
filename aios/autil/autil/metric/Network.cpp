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
#include "autil/metric/Network.h"

#include <string.h>
#include <stdlib.h>
#include <fstream>

#include "autil/metric/MetricUtil.h"
#include "autil/TimeUtility.h"

#define DELTA 0.000001

using namespace std;

namespace autil {
namespace metric {

const string Network::NET_PROC_STAT("/proc/net/dev");

Network::Network() { 
    _netStatFile = NET_PROC_STAT;
    _timeDiff = 0;
}

Network::~Network() { 
}

void Network::update() {
    _prevStat = _curStat;
    _curStat.reset();
    _curStat.updateTime = TimeUtility::currentTime();
    ifstream fin(_netStatFile.c_str());
    
    string line;
    while (std::getline(fin, line)) {
        parseNetStatLine(line.c_str(), _curStat);
    }
    _timeDiff = (_curStat.updateTime - _prevStat.updateTime) / (double)1000000;
    adjust();
}

void Network::adjust() {
    if (_timeDiff < DELTA) {
        _timeDiff = DELTA;
    }
    if (_curStat.pksin < _prevStat.pksin) {
        _curStat.pksin = _prevStat.pksin;
    }
    if (_curStat.bytesin < _prevStat.bytesin) {
        _curStat.bytesin = _prevStat.bytesin;
    }
    if (_curStat.pksout < _prevStat.pksout) {
        _curStat.pksout = _prevStat.pksout;
    }
    if (_curStat.bytesout < _prevStat.bytesout) {
        _curStat.bytesout = _prevStat.bytesout;
    }
}

/*
  /proc/net/dev

  Inter-|   Receive                                                |  Transmit
  face  |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
  eth0:32962464219 121148329    0    0    0     0          0       988 6042136006 75411586    0    0    0     0       0          0
*/
void Network::parseNetStatLine(const char *str, NetStat &netStat) {
    if (strstr(str, "lo") || strstr(str, "bond")
        || strstr(str, "docker") || strstr(str, "veth")
        || strstr(str, "vlan")) 
    {
        return;
    }
    
    const char *p = strstr(str, ":");
    if (p == NULL) {
        return;
    }
    
    p ++; // skip the ":"
    
    netStat.bytesin += strtod(p, NULL);
    p = MetricUtil::skipToken(p);

    netStat.pksin += strtod(p, NULL);
    p = MetricUtil::skipToken(p);

    // skip to output infos
    for (int i = 0; i < 6; i++) {
        p = MetricUtil::skipToken(p);
    }
    
    netStat.bytesout += strtod(p, NULL);
    p = MetricUtil::skipToken(p);

    netStat.pksout += strtod(p, NULL);
}

void Network::setNetStatFile(const string &statFile) {
    _netStatFile = statFile;
}
void Network::setTimeDiff(double diff) {
    _timeDiff = diff;
}

}
}

