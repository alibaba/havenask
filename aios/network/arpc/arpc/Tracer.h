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
#ifndef ARPC_TRACER_H
#define ARPC_TRACER_H
#include <stdint.h>
#include <string>

#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/metric/ClientRPCStats.h"
#include "aios/network/arpc/arpc/metric/ServerRPCStats.h"
#include "aios/network/arpc/arpc/proto/rpc_extensions.pb.h"
#include "aios/network/arpc/arpc/util/Log.h"
#include "autil/TimeUtility.h"

ARPC_BEGIN_NAMESPACE(arpc);

class Tracer {
public:
    Tracer(bool enableTrace = false);
    ~Tracer();

public:
    void SetTraceFlag(bool flag) { _enableTrace = flag; }
    bool GetTraceFlag() const { return _enableTrace; }

    inline void SetClientTimeout(int32_t clientTimeout);
    int32_t GetClientTimeout() const { return _clientTimeout; }

    inline void SetServerQueueSize(int32_t queueSize);

    inline void setUserPayload(const std::string &userPayload);
    inline const std::string &getUserPayload() const;

    TraceInfo *ExtractClientTraceInfo(google::protobuf::Arena *arena) const;
    TraceInfo *ExtractServerTraceInfo(google::protobuf::Arena *arena) const;

public:
    int64_t GetBeginHandleRequest() const { return _beginHandleRequest; }
    int64_t GetBeginWorkItemProcess() const { return _beginWorkItemProcess; }
    int64_t GetBeginPostResponse() const { return _beginPostResponse; }
    /*
     * trace info on client
     */
    inline void BeginCallMethod();
    inline void EndEncodeRequest();
    inline void BeginPostRequest();
    inline void EndPostRequest();
    inline void BeginHandleResponse();
    inline void EndCallMethod(ErrorCode errCode);
    inline void setClientRPCStats(const std::shared_ptr<ClientRPCStats> &clientStats) { _clientStats = clientStats; }
    const std::shared_ptr<ClientRPCStats> &getClientRPCStats() const { return _clientStats; }

    /*
     * trace info on server
     */
    inline void SetBeginHandleRequest(int64_t beginHandleRequest);
    inline void BeginHandleRequest();
    inline void EndDecodeRequest();
    inline void BeginWorkItemProcess();
    inline void BeginPostResponse();
    inline void EndEncodeResponse();
    inline void EndHandleRequest(ErrorCode errCode);
    inline void setServerRPCStats(const std::shared_ptr<ServerRPCStats> &serverStats) { _serverStats = serverStats; }
    const std::shared_ptr<ServerRPCStats> &getServerRPCStats() const { return _serverStats; }

private:
    inline int64_t calcTime(int64_t begin, int64_t end) const {
        int64_t ret = end - begin;
        return (ret < 0 ? 0 : ret);
    }
    friend class TracerTest;

private:
    bool _enableTrace;
    int32_t _clientTimeout;
    int32_t _serverQueueSize;
    std::string _userPayload;

    // time unit is: us
    // trace info on client
    int64_t _beginPostRequest;
    int64_t _beginHandleResponse;
    std::shared_ptr<ClientRPCStats> _clientStats{nullptr};

    // trace info on server
    int64_t _beginHandleRequest;
    int64_t _endHandleRequest;
    int64_t _beginPostResponse;
    int64_t _beginWorkItemProcess;
    std::shared_ptr<ServerRPCStats> _serverStats{nullptr};
};

inline void Tracer::SetServerQueueSize(int32_t queueSize) { _serverQueueSize = queueSize; }

inline void Tracer::SetClientTimeout(int32_t clientTimeout) { _clientTimeout = clientTimeout; }

inline void Tracer::setUserPayload(const std::string &userPayload) { _userPayload = userPayload; }

inline const std::string &Tracer::getUserPayload() const { return _userPayload; }

/*
 * trace info on client
 */
inline void Tracer::BeginCallMethod() {
    if (_clientStats != nullptr) {
        _clientStats->markRequestBegin();
        _clientStats->markRequestPackBegin();
    }
}

inline void Tracer::EndEncodeRequest() {
    if (_clientStats != nullptr) {
        _clientStats->markRequestPackDone();
    }
}

inline void Tracer::BeginPostRequest() {
    _beginPostRequest = autil::TimeUtility::currentTime();
    if (_clientStats != nullptr) {
        _clientStats->markRequestSendBegin();
    }
}

inline void Tracer::EndPostRequest() {
    if (_clientStats != nullptr) {
        _clientStats->markRequestSendDone();
    }
}

inline void Tracer::BeginHandleResponse() {
    _beginHandleResponse = autil::TimeUtility::currentTime();
    if (_clientStats != nullptr) {
        _clientStats->markResponsePopBegin();
        _clientStats->markResponseUnpackBegin();
    }
}

inline void Tracer::EndCallMethod(ErrorCode errCode) {
    if (_clientStats != nullptr) {
        if (errCode == ARPC_ERROR_NONE) {
            _clientStats->markResponseUnpackDone();
        }
        _clientStats->markRequestDone(errCode);
    }
}

inline void Tracer::SetBeginHandleRequest(int64_t beginHandleRequest) {
    _beginHandleRequest = beginHandleRequest;
    if (_serverStats != nullptr) {
        _serverStats->markRequestBegin();
        _serverStats->markRequestUnpackBegin();
    }
}

inline void Tracer::BeginHandleRequest() { SetBeginHandleRequest(autil::TimeUtility::currentTime()); }

inline void Tracer::EndDecodeRequest() {
    _endHandleRequest = autil::TimeUtility::currentTime();
    if (_serverStats != nullptr) {
        _serverStats->markRequestUnpackDone();
        _serverStats->markRequestPendingBegin();
    }
}

inline void Tracer::BeginWorkItemProcess() {
    _beginWorkItemProcess = autil::TimeUtility::currentTime();
    if (_serverStats != nullptr) {
        _serverStats->markRequestPendingDone();
        _serverStats->markRequestCallBegin();
    }
}

inline void Tracer::BeginPostResponse() {
    _beginPostResponse = autil::TimeUtility::currentTime();
    if (_serverStats != nullptr) {
        _serverStats->markRequestCallDone();
        _serverStats->markResponsePackBegin();
    }
}

inline void Tracer::EndEncodeResponse() {
    if (_serverStats != nullptr) {
        _serverStats->markResponsePackDone();
        _serverStats->markResponseSendBegin();
    }
}

inline void Tracer::EndHandleRequest(ErrorCode errCode) {
    if (_serverStats != nullptr) {
        if (errCode == ARPC_ERROR_NONE) {
            _serverStats->markResponseSendDone();
        }
        _serverStats->markRequestDone(errCode);
    }
}

TYPEDEF_PTR(Tracer);
ARPC_END_NAMESPACE(arpc);

#endif // ARPC_TRACER_H
