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

#include "aios/network/arpc/arpc/metric/ClientMetricReporter.h"
#include "aios/network/arpc/arpc/proto/rpc_extensions.pb.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"

namespace arpc {

// 保存一次 RPC 调用过程中的统计信息，作为 client 端发出的 RPC 调用可以使用该类型进行统计。
class ClientRPCStats {
public:
    ClientRPCStats(std::shared_ptr<ClientMetricReporter> metricReporter) : _metricReporter(metricReporter) {
        _iovSizes.reserve(4);
    }
    ~ClientRPCStats();

public:
    void markRequestBegin() { _beginTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    void markRequestDone(arpc::ErrorCode error);
    int64_t totalCostUs() const { return nonZeroSubtract(_doneTimeUs, _beginTimeUs); }

    void markRequestPendingBegin() { _requestPendingBeginTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    void markRequestPendingDone() { _requestPendingDoneTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    int64_t requestPendingCostUs() const {
        return nonZeroSubtract(_requestPendingDoneTimeUs, _requestPendingBeginTimeUs);
    }

    void markRequestPackBegin() { _requestPackBeginTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    void markRequestPackDone() { _requestPackDoneTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    int64_t requestPackCostUs() const { return nonZeroSubtract(_requestPackDoneTimeUs, _requestPackBeginTimeUs); }

    void markRequestSendBegin() { _requestSendBeginTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    void markRequestSendDone() { _requestSendDoneTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    int64_t requestSendCostUs() const { return nonZeroSubtract(_requestSendDoneTimeUs, _requestSendBeginTimeUs); }

    void markResponsePopBegin(int64_t timeUs = 0) {
        if (timeUs != 0) {
            _responsePopBeginTimeUs = timeUs;
            return;
        }
        _responsePopBeginTimeUs = autil::TimeUtility::currentTimeInMicroSeconds();
    }
    void markResponsePopDone() { _responsePopDoneTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    int64_t responsePopCostUs() const { return nonZeroSubtract(_responsePopDoneTimeUs, _responsePopBeginTimeUs); }

    // 从请求发送完毕，到接收到响应之间的耗时认为是等待响应的耗时
    int64_t responseWaitCostUs() const { return nonZeroSubtract(_responsePopBeginTimeUs, _requestSendDoneTimeUs); }

    void markResponseUnpackBegin() { _responseUnpackBeginTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    void markResponseUnpackDone() { _responseUnpackDoneTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    int64_t responseUnpackCostUs() const {
        return nonZeroSubtract(_responseUnpackDoneTimeUs, _responseUnpackBeginTimeUs);
    }

    struct IOVDesc {
        IOVDesc(const std::string &d, const std::string &rd, size_t s) : device(d), remoteDevice(rd), size(s) {}
        std::string device;
        std::string remoteDevice;
        size_t size;
    };
    void markRequestIovSize(const std::string &d, const std::string &rd, size_t s) { _iovSizes.emplace_back(d, rd, s); }
    const std::vector<IOVDesc> &iovSizes() const { return _iovSizes; };

public:
    void setRequestId(uint32_t requestId) { _requestId = requestId; }
    uint32_t requestId() const { return _requestId; }
    arpc::ErrorCode error() const { return _error; }
    bool success() const { return _error == arpc::ARPC_ERROR_NONE; }

private:
    int64_t nonZeroSubtract(int64_t lhs, int64_t rhs) const {
        return (lhs > 0 && rhs > 0 && lhs > rhs) ? lhs - rhs : 0L;
    }

private:
    // 从发起请求，到接收到响应之间的完整耗时
    int64_t _beginTimeUs{0};
    int64_t _doneTimeUs{0};

    // 阶段耗时，请求发起时如果连接没有建立，会先暂存起来，这两个时间点记录了请求被暂存的时间
    int64_t _requestPendingBeginTimeUs{0};
    int64_t _requestPendingDoneTimeUs{0};

    // 阶段耗时，将请求编码为通信库所需的格式
    int64_t _requestPackBeginTimeUs{0};
    int64_t _requestPackDoneTimeUs{0};

    // 阶段耗时，将编码后的请求通过通信库发送出去
    int64_t _requestSendBeginTimeUs{0};
    int64_t _requestSendDoneTimeUs{0};

    // 阶段耗时，接收到响应后查找出相应请求的耗时
    int64_t _responsePopBeginTimeUs{0};
    int64_t _responsePopDoneTimeUs{0};

    // 阶段耗时，将响应解码为用户所知的格式
    int64_t _responseUnpackBeginTimeUs{0};
    int64_t _responseUnpackDoneTimeUs{0};

    // iovSize, 被处理的请求iov大小
    std::vector<IOVDesc> _iovSizes;

    uint32_t _requestId{0};
    arpc::ErrorCode _error{ARPC_ERROR_NONE};

private:
    std::weak_ptr<ClientMetricReporter> _metricReporter;

    AUTIL_LOG_DECLARE();
};

} // namespace arpc