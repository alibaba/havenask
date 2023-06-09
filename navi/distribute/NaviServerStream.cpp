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
    , _creator(creator)
    , _navi(navi)
{
    _logger.addPrefix("server");
    initThreadPool();
}

NaviServerStream::~NaviServerStream() {
    _creator->decQueryCount();
}

void NaviServerStream::initThreadPool() {
    auto snapshot = getSnapshot();
    if (snapshot) {
        setThreadPool(snapshot->getDestructThreadPool());
    }
}

std::shared_ptr<NaviSnapshot> NaviServerStream::getSnapshot() const {
    auto navi = getNavi();
    if (!navi) {
        NAVI_LOG(ERROR, "navi is null");
        return nullptr;
    }
    return navi->getSnapshot();
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
    if (naviRequest->has_graph()) {
        if (!runGraph(naviRequest, message.arena)) {
            return false;
        }
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

bool NaviServerStream::notifyReceive(multi_call::PartIdTy partId) {
    return NaviStreamBase::notifyReceiveBase(shared_from_this(), partId);
}

bool NaviServerStream::runGraph(NaviMessage *request, const ArenaPtr &arena) {
    if (initialized()) {
        NAVI_LOG(ERROR, "graph is running");
        return false;
    }
    auto snapshot = getSnapshot();
    if (!snapshot) {
        NAVI_LOG(ERROR, "navi snapshot is null");
        return false;
    }
    // ATTENTION:
    // trace log will not be collected before runGraph return
    auto oldLogger = NAVI_TLS_LOGGER;
    NAVI_TLS_LOGGER = nullptr;
    if (!snapshot->runGraph(request, arena, request->params(),
                            shared_from_this(), _logger.logger))
    {
        NAVI_LOG(WARN, "snapshot run graph failed");
        return false;
    }
    NAVI_TLS_LOGGER = oldLogger;
    if (!initialized()) {
        NAVI_LOG(ERROR, "run graph failed");
        return false;
    }
    NAVI_LOG(SCHEDULE1, "end run graph");
    return true;
}

void NaviServerStream::doFinish() {
    _navi = nullptr;
}

Navi *NaviServerStream::getNavi() const {
    return _navi;
}

}
