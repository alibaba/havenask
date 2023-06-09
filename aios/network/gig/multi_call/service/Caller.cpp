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
#include "aios/network/gig/multi_call/service/Caller.h"
#include "aios/network/gig/multi_call/service/CallBack.h"
#include "aios/network/gig/multi_call/service/SearchServiceProvider.h"
#include "aios/network/gig/multi_call/service/SearchServiceResource.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, Caller);

Caller::Caller(const ChildNodeReplyPtr &reply,
               const std::shared_ptr<QuerySessionContext> &sessionContext)
    : _hasClosure(false)
    , _stopped(false)
    , _reply(reply)
    , _sessionContext(sessionContext)
{
}

Caller::~Caller() {}

void Caller::call(const SearchServiceResourcePtr &resource, bool isRetry) {
    resource->fillRequestQueryInfo(isRetry, _sessionContext->getTracer(),
                                   _sessionContext->getTraceServerSpan(),
                                   _sessionContext->sessionRequestType());
    SearchServiceProviderPtr provider = resource->getProvider(isRetry);
    RequestPtr request = resource->getRequest();
    PartIdTy partIdIndex = -1;
    PartIdTy partId = resource->getPartId();
    if (_partIdToIndexMap.count(partId) == 0) {
        partIdIndex = _partIdToIndexMap.size();
        _partIdToIndexMap[partId] = partIdIndex;
    } else {
        partIdIndex = _partIdToIndexMap[partId];
    }
    resource->setPartIdIndex(partIdIndex);
    if (!isRetry && resource->isNormalRequest()) {
        _notifier.inc(partIdIndex);
    }
    CallBackPtr callBack(new CallBack(resource, shared_from_this(), isRetry));
    callBack->setProtobufArena(request->getProtobufArena());
    auto ret = provider->post(request, callBack);
    resource->setHasError(!ret);
}

void Caller::earlyTerminate() {
    Closure *closure = _notifier.stealClosure();
    if (closure) {
        closure->Run();
    }
}

} // namespace multi_call
