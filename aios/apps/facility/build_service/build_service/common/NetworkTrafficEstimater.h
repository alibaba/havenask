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
#ifndef ISEARCH_BS_NETWORKTRAFFICESTIMATER_H
#define ISEARCH_BS_NETWORKTRAFFICESTIMATER_H

#include <autil/LoopThread.h>
#include <indexlib/util/metrics/MetricProvider.h>

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace common {

class NetworkTrafficEstimater
{
public:
    static const char PROC_NET_DEV[];
    static const int64_t STAT_INTERVAL_IN_SECONDS;

    struct NetworkTrafficInfo {
        NetworkTrafficInfo()
            : byteIn(0)
            , byteOut(0)
            , pktIn(0)
            , pktOut(0)
            , pktErrIn(0)
            , pktDropIn(0)
            , pktErrOut(0)
            , pktDropOut(0)
        {
        }

        NetworkTrafficInfo& operator+(const struct NetworkTrafficInfo& other)
        {
            byteIn += other.byteIn;
            byteOut += other.byteOut;
            pktIn += other.pktIn;
            pktOut += other.pktOut;
            pktErrIn += other.pktErrIn;
            pktDropIn += other.pktDropIn;
            pktErrOut += other.pktErrOut;
            pktDropOut += other.pktDropOut;
            return *this;
        }

        NetworkTrafficInfo& operator-(const struct NetworkTrafficInfo& other)
        {
            auto Sub = [](uint64_t& val, uint64_t otherVal) -> void {
                if (unlikely(val < otherVal)) {
                    val = 0;
                } else {
                    val -= otherVal;
                }
            };
            Sub(byteIn, other.byteIn);
            Sub(byteOut, other.byteOut);
            Sub(pktIn, other.pktIn);
            Sub(pktOut, other.pktOut);
            Sub(pktErrIn, other.pktErrIn);
            Sub(pktErrOut, other.pktErrOut);
            Sub(pktDropIn, other.pktDropIn);
            Sub(pktDropOut, other.pktDropOut);
            return *this;
        }

        uint64_t byteIn;
        uint64_t byteOut;
        uint64_t pktIn;
        uint64_t pktOut;
        uint64_t pktErrIn;
        uint64_t pktDropIn;
        uint64_t pktErrOut;
        uint64_t pktDropOut;
    };

    explicit NetworkTrafficEstimater(indexlib::util::MetricProviderPtr metricProvider);
    ~NetworkTrafficEstimater() { Stop(); }

    bool CollectNetworkTraffic(struct NetworkTrafficInfo* totalTraffic) const;
    bool Start();
    void Stop()
    {
        _isStarted = false;
        _loopThreadPtr.reset();
    }
    bool IsStarted() const { return _isStarted; }

    uint64_t GetByteInSpeed() const { return _byteInSpeed; }
    uint64_t GetByteOutSpeed() const { return _byteOutSpeed; }
    uint64_t GetPktInSpeed() const { return _pktInSpeed; }
    uint64_t GetPktOutSpeed() const { return _pktOutSpeed; }
    uint64_t GetPktErrSpeed() const { return _pktErrSpeed; }
    uint64_t GetPktDropSpeed() const { return _pktDropSpeed; }

private:
    void WorkLoop();
    void ReportMetrics();
    autil::LoopThreadPtr _loopThreadPtr;
    bool _isStarted;

    uint64_t _byteInSpeed;
    uint64_t _byteOutSpeed;
    uint64_t _pktInSpeed;
    uint64_t _pktOutSpeed;
    uint64_t _pktErrSpeed;
    uint64_t _pktDropSpeed;

    indexlib::util::MetricProviderPtr _metricProvider;
    indexlib::util::MetricPtr _byteInSpeedMetric;
    indexlib::util::MetricPtr _byteOutSpeedMetric;
    indexlib::util::MetricPtr _pktInSpeedMetric;
    indexlib::util::MetricPtr _pktOutSpeedMetric;
    indexlib::util::MetricPtr _pktErrSpeedMetric;
    indexlib::util::MetricPtr _pktDropSpeedMetric;

    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(NetworkTrafficEstimater);

}} // namespace build_service::common

#endif // ISEARCH_BS_NETWORKTRAFFICESTIMATER_H
