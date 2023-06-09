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
#include "build_service/common/NetworkTrafficEstimater.h"

#include <autil/LoopThread.h>
#include <autil/StringUtil.h>
#include <iostream>

#include "build_service/util/Monitor.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace autil;
using namespace build_service::util;

namespace build_service { namespace common {
BS_LOG_SETUP(common, NetworkTrafficEstimater);

const char NetworkTrafficEstimater::PROC_NET_DEV[] = "/proc/net/dev";
const int64_t NetworkTrafficEstimater::STAT_INTERVAL_IN_SECONDS = 30;

NetworkTrafficEstimater::NetworkTrafficEstimater(indexlib::util::MetricProviderPtr metricProvider)
    : _isStarted(false)
    , _byteInSpeed(0)
    , _byteOutSpeed(0)
    , _pktInSpeed(0)
    , _pktOutSpeed(0)
    , _pktErrSpeed(0)
    , _pktDropSpeed(0)
    , _metricProvider(metricProvider)
{
    _byteInSpeedMetric =
        DECLARE_METRIC(_metricProvider, "estimater/network_traffic.bytein_speed", kmonitor::STATUS, "count");
    _byteOutSpeedMetric =
        DECLARE_METRIC(_metricProvider, "estimater/network_traffic.byteout_speed", kmonitor::STATUS, "count");
    _pktInSpeedMetric =
        DECLARE_METRIC(_metricProvider, "estimater/network_traffic.pktin_speed", kmonitor::STATUS, "count");
    _pktOutSpeedMetric =
        DECLARE_METRIC(_metricProvider, "estimater/network_traffic.pktout_speed", kmonitor::STATUS, "count");
    _pktErrSpeedMetric =
        DECLARE_METRIC(_metricProvider, "estimater/network_traffic.pkterr_speed", kmonitor::STATUS, "count");
    _pktDropSpeedMetric =
        DECLARE_METRIC(_metricProvider, "estimater/network_traffic.pktdrop_speed", kmonitor::STATUS, "count");
}

bool NetworkTrafficEstimater::Start()
{
    _loopThreadPtr =
        LoopThread::createLoopThread(std::bind(&NetworkTrafficEstimater::WorkLoop, this),
                                     STAT_INTERVAL_IN_SECONDS * 2 * 1000 * 1000, "network_traffic_estimater_workloop");
    _isStarted = (_loopThreadPtr.get() != nullptr) ? true : false;
    return _isStarted;
}

bool NetworkTrafficEstimater::CollectNetworkTraffic(struct NetworkTrafficInfo* totalTraffic) const
{
    std::string s;
    if (!fslib::util::FileUtil::readFile(PROC_NET_DEV, s)) {
        return false;
    }
    struct NetworkTrafficInfo currentTraffic;
    memset(static_cast<void*>(&currentTraffic), 0, sizeof(struct NetworkTrafficInfo));

    string::size_type pos = 0;
    string::size_type tmp = s.find("\n", pos);
    while (tmp != s.npos) {
        string line = s.substr(pos, tmp - pos);
        if (line.find("eth") != line.npos || line.find("em") != line.npos || line.find("venet") != line.npos) {
            memset(static_cast<void*>(&currentTraffic), 0, sizeof(struct NetworkTrafficInfo));
            string::size_type startPos = line.find(":");
            string str = line.substr(startPos + 1);
            sscanf(str.c_str(),
                   "%lu %lu %lu %lu %*u %*u %*u %*u"
                   "%lu %lu %lu %lu %*u %*u %*u %*u",
                   &currentTraffic.byteIn, &currentTraffic.pktIn, &currentTraffic.pktErrIn, &currentTraffic.pktDropIn,
                   &currentTraffic.byteOut, &currentTraffic.pktOut, &currentTraffic.pktErrOut,
                   &currentTraffic.pktDropOut);
            *totalTraffic = *totalTraffic + currentTraffic;
        }
        pos = tmp + 1;
        tmp = s.find("\n", pos);
    }
    return true;
}

void NetworkTrafficEstimater::WorkLoop()
{
    int64_t beginTs = autil::TimeUtility::currentTimeInMicroSeconds();
    struct NetworkTrafficInfo totalTraffic1;
    CollectNetworkTraffic(&totalTraffic1);
    sleep(STAT_INTERVAL_IN_SECONDS);
    struct NetworkTrafficInfo totalTraffic2;
    CollectNetworkTraffic(&totalTraffic2);
    totalTraffic2 = totalTraffic2 - totalTraffic1;

    _byteInSpeed = totalTraffic2.byteIn / STAT_INTERVAL_IN_SECONDS;
    _byteOutSpeed = totalTraffic2.byteOut / STAT_INTERVAL_IN_SECONDS;
    _pktInSpeed = totalTraffic2.pktIn / STAT_INTERVAL_IN_SECONDS;
    _pktOutSpeed = totalTraffic2.pktOut / STAT_INTERVAL_IN_SECONDS;
    _pktErrSpeed = (totalTraffic2.pktErrIn + totalTraffic2.pktErrOut) / STAT_INTERVAL_IN_SECONDS;
    _pktDropSpeed = (totalTraffic2.pktDropIn + totalTraffic2.pktDropOut) / STAT_INTERVAL_IN_SECONDS;

    if (_metricProvider) {
        REPORT_METRIC(_byteInSpeedMetric, _byteInSpeed);
        REPORT_METRIC(_byteOutSpeedMetric, _byteOutSpeed);
        REPORT_METRIC(_pktInSpeedMetric, _pktInSpeed);
        REPORT_METRIC(_pktOutSpeedMetric, _pktOutSpeed);
        REPORT_METRIC(_pktErrSpeedMetric, _pktErrSpeed);
        REPORT_METRIC(_pktDropSpeedMetric, _pktErrSpeed);
    }
    int64_t currentTs = autil::TimeUtility::currentTimeInMicroSeconds();
    BS_LOG(INFO,
           "network traffic: bytin[%lu], bytout[%lu], pktin[%lu], pktout[%lu], pkterr[%lu], pktdrp[%lu],"
           " workloop took %.1f ms.",
           _byteInSpeed, _byteOutSpeed, _pktInSpeed, _pktOutSpeed, _pktErrSpeed, _pktDropSpeed,
           float(currentTs - beginTs) / 1000);
    return;
}

}} // namespace build_service::common
