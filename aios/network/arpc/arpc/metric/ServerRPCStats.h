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

#include "aios/network/arpc/arpc/metric/ServerMetricReporter.h"
#include "aios/network/arpc/arpc/proto/rpc_extensions.pb.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"

namespace arpc {

// 保存一次 RPC 调用过程中的统计信息，作为 server 端收到的 RPC 调用可以使用该类型进行统计。
class ServerRPCStats {
public:
    ServerRPCStats(std::shared_ptr<ServerMetricReporter> metricReporter) : _metricReporter(metricReporter) {
        _iovSizes.reserve(4);
    }
    ~ServerRPCStats();

public:
    void markRequestBegin() { _beginTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    void markRequestDone(arpc::ErrorCode error);
    int64_t totalCostUs() const { return nonZeroSubtract(_doneTimeUs, _beginTimeUs); }

    void markRequestUnpackBegin() { _requestUnpackBeginTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    void markRequestUnpackDone() { _requestUnpackDoneTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    int64_t requestUnpackCostUs() const { return nonZeroSubtract(_requestUnpackDoneTimeUs, _requestUnpackBeginTimeUs); }

    void markRequestPendingBegin() { _requestPendingBeginTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    void markRequestPendingDone() { _requestPendingDoneTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    int64_t requestPendingCostUs() const {
        return nonZeroSubtract(_requestPendingDoneTimeUs, _requestPendingBeginTimeUs);
    }

    void markRequestCallBegin() { _requestCallBeginTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    void markRequestCallDone() { _requestCallDoneTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    int64_t requestCallCostUs() const { return nonZeroSubtract(_requestCallDoneTimeUs, _requestCallBeginTimeUs); }

    void markResponsePackBegin() { _responsePackBeginTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    void markResponsePackDone() { _responsePackDoneTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    int64_t responsePackCostUs() const { return nonZeroSubtract(_responsePackDoneTimeUs, _responsePackBeginTimeUs); }

    void markResponseSendBegin() { _responseSendBeginTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    void markResponseSendDone() { _responseSendDoneTimeUs = autil::TimeUtility::currentTimeInMicroSeconds(); }
    int64_t responseSendCostUs() const { return nonZeroSubtract(_responseSendDoneTimeUs, _responseSendBeginTimeUs); }

    // 记录response的消息大小
    struct IOVDesc {
        IOVDesc(const std::string &d, const std::string &rd, size_t s) : device(d), remoteDevice(rd), size(s) {}
        std::string device;
        std::string remoteDevice;
        size_t size;
    };
    inline void markResponseIovSize(const std::string &device, const std::string &remoteDevice, size_t size) {
        _iovSizes.emplace_back(device, remoteDevice, size);
    }
    const std::vector<IOVDesc> &iovSizes() const { return _iovSizes; };

    inline void markThreadPoolItemCount(size_t itemCount) { _threadPoolItemCount = itemCount; }
    size_t threadPoolItemCount() const { return _threadPoolItemCount; }

public:
    void setRequestId(uint32_t requestId) { _requestId = requestId; }
    uint32_t requestId() const { return _requestId; }
    arpc::ErrorCode error() const { return _error; }
    bool success() const { return _error == arpc::ARPC_ERROR_NONE; }

private:
    // 从接收到请求，到返回响应之间的完整耗时
    int64_t _beginTimeUs{0};
    int64_t _doneTimeUs{0};

    // 将收到的请求解码为用户所知的格式
    int64_t _requestUnpackBeginTimeUs{0};
    int64_t _requestUnpackDoneTimeUs{0};

    // 收到请求后将投递到一个线程池中等待处理，这两个时间点记录了在线程池中暂存的时间
    int64_t _requestPendingBeginTimeUs{0};
    int64_t _requestPendingDoneTimeUs{0};

    // 调用用户提供的方法
    int64_t _requestCallBeginTimeUs{0};
    int64_t _requestCallDoneTimeUs{0};

    // 将用户返回的响应编码为通信库所需的格式
    int64_t _responsePackBeginTimeUs{0};
    int64_t _responsePackDoneTimeUs{0};

    // 将响应发送给对端
    int64_t _responseSendBeginTimeUs{0};
    int64_t _responseSendDoneTimeUs{0};

    // iovSize, 返回的响应iov大小
    std::vector<IOVDesc> _iovSizes;

    // thread pool in queue item count
    size_t _threadPoolItemCount{0};

    uint32_t _requestId{0};
    arpc::ErrorCode _error{ARPC_ERROR_NONE};

private:
    int64_t nonZeroSubtract(int64_t lhs, int64_t rhs) const {
        return (lhs > 0 && rhs > 0 && lhs > rhs) ? lhs - rhs : 0L;
    }

private:
    std::weak_ptr<ServerMetricReporter> _metricReporter;

    AUTIL_LOG_DECLARE();
};

} // namespace arpc