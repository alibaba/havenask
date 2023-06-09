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
#ifndef ISEARCH_MULTI_CALL_GIGCLOSURE_H
#define ISEARCH_MULTI_CALL_GIGCLOSURE_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/rpc/GigRpcController.h"
#include <functional>

namespace multi_call {

struct GrpcCallData;
class QuerySession;
class QueryInfo;
MULTI_CALL_TYPEDEF_PTR(QuerySession);
MULTI_CALL_TYPEDEF_PTR(QueryInfo);

class GigClosure : public google::protobuf::Closure {
public:
    GigClosure();
    ~GigClosure();

public:
    const QuerySessionPtr &getQuerySession() const;
    void setQuerySession(const QuerySessionPtr &session);

    const QueryInfoPtr &getQueryInfo() const;
    void setQueryInfo(const QueryInfoPtr &queryInfo);

    virtual int64_t getStartTime() const;
    GigRpcController &getController();
    float degradeLevel(float level = 1.0) const;
    MultiCallErrorCode getErrorCode() const;
    void setErrorCode(MultiCallErrorCode ec);
    void setTargetWeight(WeightTy targetWeight);
    WeightTy getTargetWeight() const;
    RequestType getRequestType() const;
    virtual ProtocolType getProtocolType() = 0;

    void startTrace();
    void endTrace();

protected:
    void setCompatibleFieldInfo(const CompatibleFieldInfo *info);
    void fillCompatibleInfo(google::protobuf::Message *message,
                            MultiCallErrorCode ec,
                            const std::string &responseInfo) const;
    void setNeedFinishQueryInfo(bool needFinish);

private:
    friend struct GrpcCallData;

protected:
    const CompatibleFieldInfo *_compatibleInfo;
    GigRpcController _controller;
    QueryInfoPtr _queryInfo;
    bool _needFinishQueryInfo;
    WeightTy _targetWeight;
    int64_t _startTime;

    QuerySessionPtr _querySession;
};

typedef std::function<void(google::protobuf::RpcController *controller,
                           google::protobuf::Message *request,
                           google::protobuf::Message *response,
                           GigClosure *closure)>
    GigRpcMethod;

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGCLOSURE_H
