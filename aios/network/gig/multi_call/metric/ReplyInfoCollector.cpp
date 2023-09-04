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
#include "aios/network/gig/multi_call/metric/ReplyInfoCollector.h"

#include "aios/network/gig/multi_call/common/ControllerParam.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, ReplyInfoCollector);

ReplyInfoCollector::ReplyInfoCollector(const vector<string> &bizNameVec) {
    for (const auto &name : bizNameVec) {
        _replyBizInfoMap[name] = ReplyBizInfo();
    }
}

ReplyInfoCollector::~ReplyInfoCollector() {
}

void ReplyInfoCollector::setEtInfo(const string &bizName, const EtInfo &etInfo) {
    ReplyBizInfoMap::iterator iter = _replyBizInfoMap.find(bizName);
    if (iter != _replyBizInfoMap.end()) {
        iter->second.etInfo = etInfo;
    } else {
        AUTIL_LOG(WARN, "unknown biz name [%s]", bizName.c_str());
    }
}

ReplyBizInfo ReplyInfoCollector::getReplyBizInfo(const std::string &bizName) {
    ReplyBizInfoMap::iterator iter = _replyBizInfoMap.find(bizName);
    if (iter != _replyBizInfoMap.end()) {
        return iter->second;
    } else {
        return ReplyBizInfo();
    }
}

const ReplyBizInfoMap &ReplyInfoCollector::getReplyBizInfoMap() const {
    return _replyBizInfoMap;
}

void ReplyInfoCollector::setRetryInfo(const string &bizName, const RetryInfo &retryInfo) {
    ReplyBizInfoMap::iterator iter = _replyBizInfoMap.find(bizName);
    if (iter != _replyBizInfoMap.end()) {
        iter->second.retryInfo = retryInfo;
    } else {
        AUTIL_LOG(WARN, "unknown biz name [%s]", bizName.c_str());
    }
}

void ReplyInfoCollector::addRequestCount(const std::string &bizName, uint32_t count) {
    ReplyBizInfoMap::iterator iter = _replyBizInfoMap.find(bizName);
    if (iter != _replyBizInfoMap.end()) {
        iter->second.requestCount += count;
    } else {
        AUTIL_LOG(WARN, "unknown biz name [%s]", bizName.c_str());
    }
}

void ReplyInfoCollector::addExpectProviderCount(const std::string &bizName, uint32_t count) {
    ReplyBizInfoMap::iterator iter = _replyBizInfoMap.find(bizName);
    if (iter != _replyBizInfoMap.end()) {
        iter->second.expectProviderCount += count;
    } else {
        AUTIL_LOG(WARN, "unknown biz name [%s]", bizName.c_str());
    }
}

void ReplyInfoCollector::addProbeCallNum(const string &bizName, uint32_t count) {
    ReplyBizInfoMap::iterator iter = _replyBizInfoMap.find(bizName);
    if (iter != _replyBizInfoMap.end()) {
        iter->second.probeCallNum += count;
    } else {
        AUTIL_LOG(WARN, "unknown biz name [%s]", bizName.c_str());
    }
}

void ReplyInfoCollector::addCopyCallNum(const std::string &bizName, uint32_t count) {
    ReplyBizInfoMap::iterator iter = _replyBizInfoMap.find(bizName);
    if (iter != _replyBizInfoMap.end()) {
        iter->second.copyCallNum += count;
    } else {
        AUTIL_LOG(WARN, "unknown biz name [%s]", bizName.c_str());
    }
}

void ReplyInfoCollector::addRetryCallNum(const string &bizName) {
    ReplyBizInfoMap::iterator iter = _replyBizInfoMap.find(bizName);
    if (iter != _replyBizInfoMap.end()) {
        iter->second.retryInfo.retryCallNum++;
    } else {
        AUTIL_LOG(WARN, "unknown biz name [%s]", bizName.c_str());
    }
}

void ReplyInfoCollector::addRetrySuccessNum(const string &bizName) {
    ReplyBizInfoMap::iterator iter = _replyBizInfoMap.find(bizName);
    if (iter != _replyBizInfoMap.end()) {
        iter->second.retryInfo.retrySuccNum++;
    } else {
        AUTIL_LOG(WARN, "unknown biz name [%s]", bizName.c_str());
    }
}

void ReplyInfoCollector::setBizLatency(const string &bizName, int64_t latency, int64_t rpcLatency,
                                       float netLatency) {
    ReplyBizInfoMap::iterator iter = _replyBizInfoMap.find(bizName);
    if (iter != _replyBizInfoMap.end()) {
        auto &bizInfo = iter->second;
        bizInfo.returnCount++;
        if (bizInfo.latency < latency) {
            bizInfo.latency = latency;
        }
        if (bizInfo.rpcLatency < rpcLatency) {
            bizInfo.rpcLatency = rpcLatency;
        }
        if (netLatency != INVALID_FLOAT_OUTPUT_VALUE && bizInfo.netLatency < netLatency) {
            bizInfo.netLatency = netLatency;
        }
    } else {
        AUTIL_LOG(WARN, "unknown biz name [%s]", bizName.c_str());
    }
}

void ReplyInfoCollector::addFailRequestCount(const std::string &bizName, uint32_t errorCount,
                                             uint32_t timeoutCount) {
    ReplyBizInfoMap::iterator iter = _replyBizInfoMap.find(bizName);
    if (iter != _replyBizInfoMap.end()) {
        auto &bizInfo = iter->second;
        bizInfo.errorRequestCount += errorCount;
        bizInfo.timeoutRequestCount += timeoutCount;
    } else {
        AUTIL_LOG(WARN, "unknown biz name [%s]", bizName.c_str());
    }
}

void ReplyInfoCollector::addErrorCode(const std::string &bizName, MultiCallErrorCode ec) {
    ReplyBizInfoMap::iterator iter = _replyBizInfoMap.find(bizName);
    if (iter != _replyBizInfoMap.end()) {
        iter->second.errorCodes.insert(ec);
    } else {
        AUTIL_LOG(WARN, "unknown biz name [%s]", bizName.c_str());
    }
}

void ReplyInfoCollector::fillErrorCodes(std::set<MultiCallErrorCode> &errorCodes) {
    ReplyBizInfoMap::iterator iter = _replyBizInfoMap.begin();
    for (; iter != _replyBizInfoMap.end(); iter++) {
        errorCodes.insert(iter->second.errorCodes.begin(), iter->second.errorCodes.end());
    }
}

void ReplyInfoCollector::setSessionSrc(const std::string &sessionSrc) {
    if (sessionSrc.empty()) {
        _sessionSrc = GIG_UNKNOWN;
    } else {
        _sessionSrc = sessionSrc;
    }
}

void ReplyInfoCollector::setSessionSrcAb(const std::string &sessionSrcAb) {
    if (sessionSrcAb.empty()) {
        _sessionSrcAb = GIG_UNKNOWN;
    } else {
        _sessionSrcAb = sessionSrcAb;
    }
}

void ReplyInfoCollector::setStressTest(const std::string &stressTest) {
    if (stressTest.empty()) {
        _stressTest = GIG_UNKNOWN;
    } else {
        _stressTest = stressTest;
    }
}

void ReplyInfoCollector::setSessionBiz(const std::string &sessionBiz) {
    if (sessionBiz.empty()) {
        _sessionBiz = GIG_UNKNOWN;
    } else {
        _sessionBiz = sessionBiz;
    }
}

const std::string &ReplyInfoCollector::getSessionSrc() const {
    return _sessionSrc;
}

const std::string &ReplyInfoCollector::getSessionSrcAb() const {
    return _sessionSrcAb;
}

const std::string &ReplyInfoCollector::getSessionBiz() const {
    return _sessionBiz;
}

const std::string &ReplyInfoCollector::getStressTest() const {
    return _stressTest;
}

void ReplyInfoCollector::addRequestSize(const std::string &bizName, size_t size) {
    ReplyBizInfoMap::iterator iter = _replyBizInfoMap.find(bizName);
    if (iter != _replyBizInfoMap.end()) {
        BizSizeInfo &info = iter->second.requestSize;
        info.num++;
        info.sumSize += size;
    } else {
        AUTIL_LOG(WARN, "unknown biz name [%s]", bizName.c_str());
    }
}

void ReplyInfoCollector::addResponseSize(const std::string &bizName, size_t size) {
    ReplyBizInfoMap::iterator iter = _replyBizInfoMap.find(bizName);
    if (iter != _replyBizInfoMap.end()) {
        BizSizeInfo &info = iter->second.responseSize;
        info.num++;
        info.sumSize += size;
    } else {
        AUTIL_LOG(WARN, "unknown biz name [%s]", bizName.c_str());
    }
}

} // namespace multi_call
