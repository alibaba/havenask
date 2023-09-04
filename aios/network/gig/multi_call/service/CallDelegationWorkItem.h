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
#ifndef ISEARCH_CALLTHREADWORKITEM_H
#define ISEARCH_CALLTHREADWORKITEM_H

#include "aios/network/gig/multi_call/service/Caller.h"
#include "aios/network/gig/multi_call/service/ChildNodeReply.h"
#include "aios/network/gig/multi_call/service/LoadBalancerContext.h"
#include "aios/network/gig/multi_call/service/SearchServiceSnapshotInVersion.h"

namespace multi_call {

class CallDelegationWorkItem
{
public:
    CallDelegationWorkItem(const CallerPtr &caller,
                           const LoadBalancerContextPtr &loadBalancerContext);
    virtual ~CallDelegationWorkItem();

private:
    CallDelegationWorkItem(const CallDelegationWorkItem &);
    CallDelegationWorkItem &operator=(const CallDelegationWorkItem &);

public:
    // return if this query is really terminated
    virtual bool process(int64_t currentTime, const SearchServiceSnapshotPtr &snapshot);

private:
    bool retry(const std::map<std::string, int32_t> &retryBizInfos,
               const SearchServiceSnapshotPtr &snapshot);

private:
    CallerPtr _caller;
    LoadBalancerContextPtr _loadBalancerContext;
    bool _isRetryQueryReported;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(CallDelegationWorkItem);

} // namespace multi_call

#endif // ISEARCH_CALLTHREADWORKITEM_H
