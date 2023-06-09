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

#include <stdint.h>
#include <memory>
#include <string>

namespace autil {
namespace metric {

struct NetStat {
    double pksin;
    double bytesin;
    double pksout;
    double bytesout;
    uint64_t updateTime;
    
    NetStat() {
        reset();
    }
    void reset() {
        pksin = 0.0;
        bytesin = 0.0;
        pksout = 0.0;
        bytesout = 0.0;
        updateTime = 0;
    }
};

class Network
{
public:
    Network();
    ~Network();
private:
    static const std::string NET_PROC_STAT;
private:
    Network(const Network &);
    Network& operator = (const Network &);
public:
    void update();
    inline double getPksin() const;
    inline double getBytesin() const;
    inline double getPksout() const;
    inline double getBytesout() const;
private:
    void adjust();
    void parseNetStatLine(const char *str, NetStat &netStat);
    void setNetStatFile(const std::string &statFile);
    void setTimeDiff(double diff);
    friend class NetworkTest;
private:
    NetStat _prevStat;
    NetStat _curStat;
    double _timeDiff; // s
    std::string _netStatFile;
};

inline double Network::getPksin() const {
    return (_curStat.pksin - _prevStat.pksin) / _timeDiff;
}

inline double  Network::getBytesin() const {
    return (_curStat.bytesin - _prevStat.bytesin) / (double)1024 / _timeDiff;
}

inline double Network::getPksout() const {
    return (_curStat.pksout - _prevStat.pksout) / _timeDiff;
}

inline double Network::getBytesout() const {
    return (_curStat.bytesout - _prevStat.bytesout) / (double)1024 / _timeDiff;
}

typedef std::shared_ptr<Network> NetworkPtr;

}
}
