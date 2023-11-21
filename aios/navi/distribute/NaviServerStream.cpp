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
#include "navi/distribute/NaviServerStream.h"
#include "navi/distribute/NaviServerStreamCreator.h"
#include "navi/engine/NaviSnapshot.h"
#include "navi/engine/RemoteSubGraphBase.h"

namespace navi {

NaviServerStream::NaviServerStream(NaviServerStreamCreator *creator,
                                   Navi *navi)
    : NaviStreamBase(nullptr)
    , _createTime(autil::TimeUtility::currentTime())
    , _creator(creator)
    , _snapshot(navi ? navi->getSnapshot() : nullptr)
{
    _logger.addPrefix("server");
    initThreadPool();
}

NaviServerStream::~NaviServerStream() {
    _creator->decQueryCount();
}

void NaviServerStream::initThreadPool() {
    if (_snapshot) {
        setThreadPool(_snapshot->getDestructThreadPool());
    }
}

google::protobuf::Message *NaviServerStream::newReceiveMessage(
        google::protobuf::Arena *arena) const
{
    return NaviStreamBase::newReceiveMessageBase(arena);
}

bool NaviServerStream::receive(const multi_call::GigStreamMessage &message) {
    auto naviRequest = dynamic_cast<NaviMessage *>(message.message);
    if (!naviRequest) {
        NAVI_LOG(ERROR, "invalid message type");
        return false;
    }
    if (!naviRequest->domain_graph().sub_graph().empty()) {
        _needGraphDef = false;
        if (!runGraph(naviRequest, message.arena)) {
            return false;
        }
    } else if (_needGraphDef) {
        NAVI_LOG(ERROR, "lack graph def");
        return false;
    }
    NAVI_LOG(SCHEDULE2, "receive msg, handler [%p]", (void *)message.handlerId);
    if (naviRequest->border_datas_size() > 0) {
        return NaviStreamBase::receiveBase(message);
    }
    return true;
}

void NaviServerStream::receiveCancel(const multi_call::GigStreamMessage &message,
                                     multi_call::MultiCallErrorCode ec)
{
    NaviStreamBase::receiveCancelBase(message, ec);
}

void NaviServerStream::notifyIdle(multi_call::PartIdTy partId) {
    NaviStreamBase::notifyIdleBase(partId);
}

bool NaviServerStream::notifyInitReceive(multi_call::PartIdTy partId) {
    multi_call::GigStreamMessage message;
    multi_call::MultiCallErrorCode ec = multi_call::MULTI_CALL_ERROR_NONE;
    if (!peekMessage(partId, message, ec)) {
        return false;
    }
    if (multi_call::MULTI_CALL_ERROR_NONE != ec) {
        return false;
    }
    if (!message.message) {
        return false;
    }
    auto naviRequest = dynamic_cast<NaviMessage *>(message.message);
    if (!naviRequest) {
        NAVI_LOG(ERROR, "invalid message type");
        return false;
    }
    if (!_snapshot) {
        NAVI_LOG(ERROR, "navi snapshot is null");
        return false;
    }
    auto &pbParams = naviRequest->params();
    const auto &taskQueueName = pbParams.task_queue_name();
    SessionId sessionId;
    sessionId.instance = pbParams.id().instance();
    sessionId.queryId = pbParams.id().query_id();
    auto item = new NaviStreamReceiveItem(shared_from_this(), partId);
    {
        NaviLoggerScope _(nullptr);
        _snapshot->runStreamSession(taskQueueName, sessionId, item, _logger.logger);
    }
    return true;
}

bool NaviServerStream::notifyReceive(multi_call::PartIdTy partId) {
    if (_initMessage && isAsyncMode()) {
        _initMessage = false;
        return notifyInitReceive(partId);
    } else {
        _initMessage = false;
        return NaviStreamBase::notifyReceiveBase(shared_from_this(), partId);
    }
}

bool NaviServerStream::runGraph(NaviMessage *request, const ArenaPtr &arena) {
    if (initialized()) {
        NAVI_LOG(ERROR, "graph is running");
        return false;
    }
    assert(_snapshot && "snapshot must not be nullptr");
    {
        NaviLoggerScope _(_logger);
        if (!_snapshot->doRunStreamSession(request, arena, request->params(), shared_from_this())) {
            NAVI_LOG(WARN, "snapshot run graph failed");
            return false;
        }
    }
    if (!initialized()) {
        NAVI_LOG(ERROR, "run graph failed");
        return false;
    }
    NAVI_LOG(SCHEDULE1, "end run graph");
    return true;
}

}
