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
#ifndef ISEARCH_MULTI_CALL_REPLYINFOCOLLECTOR_H
#define ISEARCH_MULTI_CALL_REPLYINFOCOLLECTOR_H

#include <map>
#include <set>

#include "aios/network/gig/multi_call/common/MetaEnv.h"
#include "aios/network/gig/multi_call/common/common.h"

namespace multi_call {
struct EtInfo {
    EtInfo() : isET(false), latency(0) {
    }
    bool isET;
    double latency;
};

struct RetryInfo {
    RetryInfo() : isRetry(false), latency(0), retryCallNum(0), retrySuccNum(0) {
    }
    bool isRetry;
    double latency;
    uint32_t retryCallNum;
    uint32_t retrySuccNum;
};

struct BizSizeInfo {
    BizSizeInfo() : num(0), sumSize(0) {
    }
    uint32_t num;
    size_t sumSize;
};

struct ReplyBizInfo {
    ReplyBizInfo()
        : requestCount(0)
        , errorRequestCount(0)
        , timeoutRequestCount(0)
        , expectProviderCount(0)
        , returnCount(0)
        , probeCallNum(0)
        , copyCallNum(0)
        , latency(0)
        , rpcLatency(0)
        , netLatency(0.0f) {
    }
    RetryInfo retryInfo;
    EtInfo etInfo;
    uint32_t requestCount;
    uint32_t errorRequestCount;
    uint32_t timeoutRequestCount;
    uint32_t expectProviderCount;
    uint32_t returnCount;
    uint32_t probeCallNum;
    uint32_t copyCallNum;
    int64_t latency;
    int64_t rpcLatency;
    float netLatency;
    std::set<MultiCallErrorCode> errorCodes;
    BizSizeInfo requestSize;
    BizSizeInfo responseSize;
};

typedef std::map<std::string, ReplyBizInfo> ReplyBizInfoMap;

class ReplyInfoCollector
{
public:
    ReplyInfoCollector(const std::vector<std::string> &bizNameVec);
    ~ReplyInfoCollector();

private:
    ReplyInfoCollector(const ReplyInfoCollector &);
    ReplyInfoCollector &operator=(const ReplyInfoCollector &);

public:
    void setEtInfo(const std::string &bizName, const EtInfo &etInfo);
    void setRetryInfo(const std::string &bizName, const RetryInfo &retryInfo);
    void addRequestCount(const std::string &bizName, uint32_t count);
    void addExpectProviderCount(const std::string &bizName, uint32_t count);
    void addProbeCallNum(const std::string &bizName, uint32_t count);
    void addCopyCallNum(const std::string &bizName, uint32_t count);
    void addRetryCallNum(const std::string &bizName);
    void addRetrySuccessNum(const std::string &bizName);
    void setBizLatency(const std::string &bizName, int64_t latency, int64_t rpcLatency,
                       float netLatency);
    void addFailRequestCount(const std::string &bizName, uint32_t errorCount,
                             uint32_t timeoutCount);
    void addErrorCode(const std::string &bizName, MultiCallErrorCode ec);
    void fillErrorCodes(std::set<MultiCallErrorCode> &errorCodes);
    const ReplyBizInfoMap &getReplyBizInfoMap() const;
    ReplyBizInfo getReplyBizInfo(const std::string &bizName);
    void setSessionBiz(const std::string &biz);
    void setSessionSrc(const std::string &src);
    void setSessionSrcAb(const std::string &srcAb);
    void setStressTest(const std::string &stressTest);
    const std::string &getSessionBiz() const;
    const std::string &getStressTest() const;
    const std::string &getSessionSrc() const;
    const std::string &getSessionSrcAb() const;
    void addRequestSize(const std::string &bizName, size_t size);
    void addResponseSize(const std::string &bizName, size_t size);

private:
    mutable ReplyBizInfoMap _replyBizInfoMap;
    std::string _sessionSrc;
    std::string _sessionSrcAb;
    std::string _sessionBiz;
    std::string _stressTest;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(ReplyInfoCollector);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_REPLYINFOCOLLECTOR_H
