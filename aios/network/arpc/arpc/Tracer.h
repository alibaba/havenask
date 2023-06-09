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
#include "aios/network/arpc/arpc/util/Log.h"
#include "aios/network/arpc/arpc/proto/rpc_extensions.pb.h"
#include "autil/TimeUtility.h"

ARPC_BEGIN_NAMESPACE(arpc);

class Tracer
{
public:
    Tracer(bool enableTrace = false);
    ~Tracer();
public:
    void SetTraceFlag(bool flag)
    {
        _enableTrace = flag;
    }
    bool GetTraceFlag() const
    {
        return _enableTrace;
    }
    int64_t GetBeginHandleRequest() const
    {
        return _beginHandleRequest;
    }
    int64_t GetBeginWorkItemProcess() const
    {
        return _beginWorkItemProcess; 
    }
    int32_t GetClientTimeout() const
    {
        return _clientTimeout;
    }
    int64_t GetBeginPostResponse() const
    {
        return _beginPostResponse;
    }
    TraceInfo *ExtractClientTraceInfo(google::protobuf::Arena *arena) const;
    TraceInfo *ExtractServerTraceInfo(google::protobuf::Arena *arena) const;
    /*
     * trace info on client
     */
    inline void BeginPostRequest();
    inline void BeginHandleResponse();

    /*
     * trace info on server
     */
    inline void SetServerQueueSize(int32_t queueSize);
    inline void BeginHandleRequest();
    inline void EndHandleRequest();
    inline void BeginWorkItemProcess();
    inline void BeginPostResponse();
    inline void SetBeginHandleRequest(int64_t beginHandleRequest);
    inline void SetClientTimeout(int32_t clientTimeout);

    inline void setUserPayload(const std::string &userPayload);
    inline const std::string &getUserPayload() const;

  private:
    inline int64_t calcTime(int64_t begin, int64_t end) const
    {
        int64_t ret = end - begin;
        return (ret < 0 ? 0 : ret);
    }
    friend class TracerTest;
private:
    bool _enableTrace;
    int32_t _clientTimeout;

    // time unit is: us
    // trace info on client
    int64_t _beginPostRequest;
    int64_t _beginHandleResponse;

    // trace info on server
    int32_t _serverQueueSize;
    int64_t _beginHandleRequest;
    int64_t _endHandleRequest;
    int64_t _beginPostResponse;
    int64_t _beginWorkItemProcess;
    std::string _userPayload;
};

inline void Tracer::BeginPostRequest()
{
    _beginPostRequest = autil::TimeUtility::currentTime();
}

inline void Tracer::BeginHandleResponse()
{
    _beginHandleResponse = autil::TimeUtility::currentTime();
}

inline void Tracer::SetServerQueueSize(int32_t queueSize)
{
    _serverQueueSize = queueSize;
}

inline void Tracer::BeginHandleRequest()
{
    _beginHandleRequest = autil::TimeUtility::currentTime();
}

inline void Tracer::EndHandleRequest()
{
    _endHandleRequest = autil::TimeUtility::currentTime();
}

inline void Tracer::BeginPostResponse()
{
    _beginPostResponse = autil::TimeUtility::currentTime();
}

inline void Tracer::BeginWorkItemProcess()
{
    _beginWorkItemProcess = autil::TimeUtility::currentTime();
}

inline void Tracer::SetBeginHandleRequest(int64_t beginHandleRequest)
{
    _beginHandleRequest = beginHandleRequest;
}

inline void Tracer::SetClientTimeout(int32_t clientTimeout)
{
    _clientTimeout = clientTimeout;
}

inline void Tracer::setUserPayload(const std::string &userPayload)
{
    _userPayload = userPayload;
}

inline const std::string &Tracer::getUserPayload() const
{
    return _userPayload;
}

TYPEDEF_PTR(Tracer);
ARPC_END_NAMESPACE(arpc);

#endif //ARPC_TRACER_H
