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
#include "aios/network/gig/multi_call/interface/QuerySession.h"

#include "aios/network/gig/multi_call/proto/GigAgent.pb.h"
#include "aios/network/gig/multi_call/rpc/GigClosure.h"
#include "aios/network/gig/multi_call/service/LoadBalancerContext.h"
#include "aios/network/opentelemetry/core/eagleeye/EagleeyePropagator.h"
#include "aios/network/opentelemetry/core/eagleeye/EagleeyeUtil.h"

using namespace std;
using namespace autil;

namespace multi_call {

AUTIL_LOG_SETUP(multi_call, QuerySession);

QuerySession::QuerySession(const SearchServicePtr &searchService,
                           const QueryInfoPtr &queryInfo,
                           const opentelemetry::SpanContextPtr &parent,
                           opentelemetry::SpanKind kind)
    : _searchService(searchService)
    , _sessionContext(new QuerySessionContext(queryInfo, kind))
{
    if (_searchService) {
        _loadBalancerContext.reset(new LoadBalancerContext(_searchService));
    }
    _sessionContext->initTrace(_searchService, parent);
}

QuerySession::QuerySession(const SearchServicePtr &searchService,
                           const std::shared_ptr<QuerySessionContext> &context)
    : _searchService(searchService)
    , _sessionContext(context)
{
    if (_searchService) {
        _loadBalancerContext.reset(new LoadBalancerContext(_searchService));
    }
    if (!_sessionContext) {
        _sessionContext.reset(new QuerySessionContext());
        _sessionContext->initTrace(_searchService);
    }
}

QuerySession::~QuerySession() {
}

QuerySessionPtr QuerySession::get(google::protobuf::Closure *doneClosure) {
    GigClosure *gigClosure = dynamic_cast<GigClosure *>(doneClosure);
    if (gigClosure) {
        return gigClosure->getQuerySession();
    } else {
        return QuerySessionPtr();
    }
}

QuerySessionPtr QuerySession::createWithOrigin(const SearchServicePtr &searchService,
        const QuerySessionPtr &origin) {
    return std::make_shared<QuerySession>(searchService,
            origin ? origin->getSessionContext() : nullptr);
}

void QuerySession::selectBiz(const RequestGeneratorPtr &generator,
                             BizInfo &bizInfo) {
    if (!generator) {
        AUTIL_LOG(ERROR, "request generator is null when select biz!");
        bizInfo.set_status(GIG_NULL_GENERATOR);
        return;
    }
    if (!_searchService || !_loadBalancerContext) {
        AUTIL_LOG(ERROR, "_searchService or _loadBalancerContext is null, "
                         "select biz failed!");
        bizInfo.set_status(GIG_ERROR_UNKNOWN);
        return;
    }

    std::string bizName = generator->getBizName();
    bizInfo.set_biz_name(bizName);
    bizInfo.set_cluster_name(generator->getClusterName());
    auto versionSelector = _loadBalancerContext->getVersionSelector(bizName);
    if (!versionSelector) {
        AUTIL_LOG(ERROR, "create version selector failed, bizName:[%s]",
                  bizName.c_str());
        bizInfo.set_status(GIG_REPLY_ERROR_VERSION_NOT_EXIST);
    } else {
        auto cachedGenerator = std::make_shared<CachedRequestGenerator>(generator);
        versionSelector->select(cachedGenerator,
                                _searchService->getSearchServiceSnapshot());
        const SearchServiceSnapshotInVersionPtr &versionSnapshot =
            versionSelector->getBizVersionSnapshot();
        if (versionSnapshot != nullptr) {
            bizInfo.set_status(GIG_ERROR_NONE);
            bizInfo.set_part_count(versionSnapshot->getPartCount());
            bizInfo.set_version(versionSnapshot->getVersion());
            bizInfo.set_is_complete(versionSnapshot->isComplete());
        } else {
            bizInfo.set_status(GIG_NULL_SNAPSHOT);
        }
    }
}

void QuerySession::call(CallParam &callParam, ReplyPtr &reply) {
    if (!_searchService || !_loadBalancerContext) {
        AUTIL_LOG(ERROR, "_searchService or _loadBalancerContext is null, call "
                         "with query session failed!");
        if (callParam.closure) { // callback
            reply.reset();
            callParam.closure->Run();
        }
        return;
    }
    RequestType sType = sessionRequestType();
    if (sType != RT_NORMAL) {
        for (auto &generator : callParam.generatorVec) {
            generator->setUserRequestType(sType);
        }
    }
    _searchService->search(callParam, reply, this->shared_from_this());
}

bool QuerySession::bind(const GigClientStreamPtr &stream) {
    if (!_searchService || !_loadBalancerContext) {
        AUTIL_LOG(ERROR,
                  "bind client stream [%p] failed, searchService [%p] or loadBalancerContext [%p] "
                  "is null",
                  stream.get(), _searchService.get(), _loadBalancerContext.get());
        return false;
    }
    return _searchService->bind(stream, this->shared_from_this());
}

} // namespace multi_call
