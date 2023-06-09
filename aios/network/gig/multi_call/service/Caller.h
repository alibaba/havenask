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
#ifndef MULTI_CALL_CALLER_H
#define MULTI_CALL_CALLER_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/QuerySessionContext.h"
#include "aios/network/gig/multi_call/interface/Closure.h"
#include "aios/network/gig/multi_call/service/SearchServiceResource.h"
#include "aios/network/gig/multi_call/util/TerminateNotifier.h"
#include "autil/Lock.h"

namespace multi_call {

class ChildNodeReply;
typedef std::shared_ptr<ChildNodeReply> ChildNodeReplyPtr;

class Caller : public std::enable_shared_from_this<Caller> {
public:
    /**
     * @func: called whenever a response arrived
     */
    Caller(const ChildNodeReplyPtr &reply, const std::shared_ptr<QuerySessionContext> &sessionContext);
    virtual ~Caller();

private:
    Caller(const Caller &);
    Caller &operator=(const Caller &);

public:
    void call(const SearchServiceResourcePtr &resource, bool isRetry);
    void earlyTerminate();
    bool canTerminate() const {
        if (_hasClosure) {
            return _notifier.canTerminate();
        } else {
            return _stopped;
        }
    }
    void afterCall(Closure *closure) {
        _hasClosure = true;
        _notifier.setClosure(closure);
    }
    bool hasClosure() const {
        return _hasClosure;
    }
    void stop() {
        _stopped = true;
    }
    Closure *stealClosure() {
        return _notifier.stealClosure();
    }
    void decNotifier(PartIdTy partIdIndex) {
        _notifier.dec(partIdIndex);
    }
    const ChildNodeReplyPtr &getReply() const {
        return _reply;
    }
protected:
    bool _hasClosure : 1;
    bool _stopped : 1;
    TerminateNotifier _notifier;
    ChildNodeReplyPtr _reply;
    std::shared_ptr<QuerySessionContext> _sessionContext;
    std::map<PartIdTy, PartIdTy> _partIdToIndexMap;
private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(Caller);

} // namespace multi_call

#endif // MULTI_CALL_CALLER_H
