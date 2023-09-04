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
#ifndef ISEARCH_MULTI_CALL_RESPONSE_H
#define ISEARCH_MULTI_CALL_RESPONSE_H

#include "aios/network/gig/multi_call/common/QueryResultStatistic.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/opentelemetry/core/Span.h"

namespace multi_call {

class GigResponseInfo;
struct LackResponseInfo {
    std::string bizName;
    VersionTy version = -1;
    PartIdTy partId = -1;
    PartIdTy partCount = -1;
};

class Response
{
public:
    Response()
        : _partCount(INVALID_PART_COUNT)
        , _partId(INVALID_PART_ID)
        , _version(INVALID_VERSION_ID)
        , _agentInfo(nullptr)
        , _isReturned(false)
        , _isRetried(false) {
    }
    virtual ~Response();

public:
    // response should always free data
    virtual void init(void *data) = 0;
    virtual bool deserializeApp() {
        return true;
    }
    virtual size_t size() const = 0;
    virtual void fillSpan() {};

public:
    const std::string &getBizName() const {
        return _bizName;
    }
    void setBizName(const std::string &bizName) {
        _bizName = bizName;
    }
    void setRequestId(const std::string &requestId) {
        _requestId = requestId;
    }
    const std::string &getRequestId() const {
        return _requestId;
    }
    const std::string &getSpecStr() const {
        return _specStr;
    }
    void setSpecStr(const std::string &specStr) {
        _specStr = specStr;
    }
    const std::string &getNodeId() const {
        return _nodeId;
    }
    void setNodeId(const std::string &nodeId) {
        _nodeId = nodeId;
    }
    PartIdTy getPartCount() const {
        return _partCount;
    }
    void setPartCount(PartIdTy partCount) {
        _partCount = partCount;
    }
    PartIdTy getPartId() const {
        return _partId;
    }
    void setPartId(PartIdTy partId) {
        _partId = partId;
    }
    VersionTy getVersion() const {
        return _version;
    }
    void setVersion(VersionTy version) {
        _version = version;
    }
    WeightTy getWeight() const {
        return _stat.targetWeight;
    }
    void setWeight(WeightTy weight) {
        _stat.setTargetWeight(weight);
    }
    void setAgentInfo(const std::string &info) {
        initAgentInfo(info);
        if (hasStatInAgentInfo()) {
            _stat.setAgentInfo(_agentInfo);
        }
    }
    const GigResponseInfo *getAgentInfo() {
        return _agentInfo;
    }
    void setCallBegTime(int64_t beginTime) {
        _stat.setCallBegTime(beginTime);
    }
    void rpcBegin() {
        _stat.rpcBegin();
    }
    void callEnd() {
        _stat.callEnd();
    }
    int64_t getCallBegTime() const {
        return _stat.callBegTime;
    }
    int64_t getCallEndTime() {
        return _stat.callEndTime;
    }
    int64_t callUsedTime() {
        return _stat.getLatency();
    }
    int64_t rpcUsedTime() {
        return _stat.getRpcLatency();
    }
    float netLatency() {
        return _stat.getNetLatency();
    }
    void setLatencyMs(float latencyMs) {
        _stat.callEndTime = _stat.callBegTime + (int64_t)(latencyMs * FACTOR_US_TO_MS);
    }
    MultiCallErrorCode getErrorCode() const {
        return _stat.ec;
    }
    void setErrorCode(MultiCallErrorCode ec, const std::string &errorString = "") {
        _stat.ec = ec;
        _stat.errorString = errorString;
    }
    void setReturned() {
        _isReturned = true;
    }
    bool isReturned() const {
        return _isReturned;
    }
    void setRetried() {
        _isRetried = true;
    }
    bool isRetried() const {
        return _isRetried;
    }
    bool isFailed() const {
        return _stat.isFailed();
    }
    std::string errorString() const {
        return _stat.errorString;
    }
    QueryResultStatistic &getStat() {
        return _stat;
    }
    virtual std::string toString() const {
        return "bizName: " + _bizName + ", requestId: " + _requestId + ", spec: " + _specStr +
               ", part: " + std::to_string(_partId) + "/" + std::to_string(_partCount);
    }
    void setProtobufArena(const std::shared_ptr<google::protobuf::Arena> &arena) {
        _arena = arena;
    }
    void setSpan(const opentelemetry::SpanPtr &span) {
        _span = span;
    }
    const opentelemetry::SpanPtr &getSpan() {
        return _span;
    }
    void setProtocolType(ProtocolType type) {
        _type = type;
    }
    ProtocolType getProtocolType() const {
        return _type;
    }

protected:
    void initAgentInfo(const std::string &agentInfoStr);
    bool hasStatInAgentInfo();

private:
    std::string _bizName;
    std::string _requestId;
    std::string _specStr;
    std::string _nodeId;
    PartIdTy _partCount;
    PartIdTy _partId;
    VersionTy _version;
    QueryResultStatistic _stat;
    std::shared_ptr<google::protobuf::Arena> _arena;
    GigResponseInfo *_agentInfo;
    volatile bool _isReturned;
    volatile bool _isRetried;
    opentelemetry::SpanPtr _span;
    ProtocolType _type;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(Response);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_RESPONSE_H
