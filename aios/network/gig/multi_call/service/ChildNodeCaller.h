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
#ifndef ISEARCH_MULTI_CALL_CHILDNODECALLER_H
#define ISEARCH_MULTI_CALL_CHILDNODECALLER_H

#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "aios/network/gig/multi_call/interface/Reply.h"
#include "aios/network/gig/multi_call/interface/SearchService.h"
#include "aios/network/gig/multi_call/service/CallDelegationThread.h"
#include "aios/network/gig/multi_call/service/ChildNodeReply.h"
#include "aios/network/gig/multi_call/service/FlowConfigSnapshot.h"
#include "aios/network/gig/multi_call/service/LatencyTimeSnapshot.h"
#include "aios/network/gig/multi_call/service/RetryLimitChecker.h"

namespace multi_call {

class ChildNodeCaller
{
public:
    ChildNodeCaller(const SearchServiceSnapshotPtr &snapshot,
                    const FlowConfigSnapshotPtr &flowConfigSnapshot,
                    const CallDelegationThreadPtr &callDelegationThread,
                    const RequestGeneratorVec &generatorVec,
                    const RetryLimitCheckerPtr &retryLimitChecker,
                    const LatencyTimeSnapshotPtr &latencyTimeSnapshot,
                    const QuerySessionPtr &querySession);
    ~ChildNodeCaller();

private:
    ChildNodeCaller(const ChildNodeCaller &);
    ChildNodeCaller &operator=(const ChildNodeCaller &);

public:
    ChildNodeReplyPtr call();
    ChildNodeReplyPtr call(SearchServiceResourceVector &resourceVec);
    void afterCall(Closure *closure);
    bool isDetectionOn() const;
    bool isRetryOn() const;
    const CallerPtr &getCaller() const;

public:
    // for java client
    bool initResource(SearchServiceResourceVector &resourceVec);
    const ReplyInfoCollectorPtr &getReplyInfoCollector() const {
        return _replyInfoCollector;
    }

private:
    void initCaller();
    void prepareSearchResource(SearchServiceResourceVector &resourceVec);
    void doCall(const SearchServiceResourcePtr &resource);
    void fillSourceId();

private:
    static uint64_t getMaxTimeout(const SearchServiceResourceVector &resourceVec);

private:
    int64_t _callBeginTime;
    SearchServiceSnapshotPtr _snapshot;
    FlowConfigSnapshotPtr _flowConfigSnapshot;
    CallDelegationThreadPtr _callDelegationThread;
    RequestGeneratorVec _generatorVec;
    RetryLimitCheckerPtr _retryLimitChecker;
    LatencyTimeSnapshotPtr _latencyTimeSnapshot;
    QuerySessionPtr _querySession;
    ChildNodeReplyPtr _reply;
    ReplyInfoCollectorPtr _replyInfoCollector;
    CallerPtr _caller;
    bool _earlyTerminationEnabled;
    bool _retryEnabled;
    bool _singleRetryEnable;
    bool _canRetry;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(ChildNodeCaller);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_CHILDNODECALLER_H
