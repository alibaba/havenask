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
#ifndef ISEARCH_MULTI_CALL_QUERYRESULTSTATISTIC_H
#define ISEARCH_MULTI_CALL_QUERYRESULTSTATISTIC_H

#include "aios/network/gig/multi_call/common/ControllerParam.h"
#include "aios/network/gig/multi_call/common/common.h"
#include <assert.h>

namespace multi_call {

class GigResponseInfo;
class PropagationStatDef;

struct QueryResultStatistic {
public:
    QueryResultStatistic()
        : statIndex(-1), targetWeight(MAX_WEIGHT), callBegTime(-1),
          rpcBeginTime(-1), callEndTime(-1), ec(MULTI_CALL_ERROR_NO_RESPONSE),
          agentInfo(NULL), netLatency(INVALID_FLOAT_OUTPUT_VALUE), heartbeatPropagationStat(nullptr) {}
    ~QueryResultStatistic();
    QueryResultStatistic(const QueryResultStatistic &) = delete;
    QueryResultStatistic &operator=(const QueryResultStatistic &) = delete;

    void rpcBegin() { rpcBeginTime = autil::TimeUtility::currentTime(); }
    void callEnd() {
        if (callEndTime <= callBegTime) {
            callEndTime = autil::TimeUtility::currentTime();
        }
    }
    void setCallBegTime(int64_t beginTime) { callBegTime = beginTime; }
    int64_t getLatency() const {
        assert(callEndTime >= callBegTime);
        return callEndTime - callBegTime;
    }
    int64_t getRpcLatency() const {
        if (rpcBeginTime > 0) {
            assert(callEndTime >= rpcBeginTime);
            return callEndTime - rpcBeginTime;
        } else {
            return getLatency();
        }
    }
    bool isDecWeight() const { return MULTI_CALL_ERROR_DEC_WEIGHT == ec; }
    bool isFailed() const { return ec > MULTI_CALL_ERROR_DEC_WEIGHT; }
    bool isEmptyAgentInfo() const { return agentInfo == NULL; }
    void setTargetWeight(WeightTy weight) {
        if (weight > MAX_WEIGHT) {
            weight = MAX_WEIGHT;
        } else if (weight < MIN_WEIGHT) {
            weight = MIN_WEIGHT;
        }
        targetWeight = weight;
    }
    void setNetLatency(float netLatency_) { netLatency = netLatency_; }
    float getNetLatency() const { return netLatency; }
    void setAgentInfo(GigResponseInfo *pbInfo);
    bool validateMirror() const;
    const PropagationStatDef &getMirrorStat() const;

private:
    // for ut
    bool operator==(const QueryResultStatistic &rhs) const;

public:
    int32_t statIndex;
    WeightTy targetWeight;
    int64_t callBegTime;
    int64_t rpcBeginTime;
    int64_t callEndTime;
    MultiCallErrorCode ec;
    std::string errorString;
    GigResponseInfo *agentInfo;
    float netLatency;
    const PropagationStatDef *heartbeatPropagationStat;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_QUERYRESULTSTATISTIC_H
