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
#include "navi/distribute/NaviStreamBase.h"

namespace navi {

NaviStreamBase::NaviStreamBase(const NaviLoggerPtr &logger)
    : _logger(this, "stream", logger)
    , _initialized(false)
{
    NAVI_LOG(SCHEDULE1, "stream base constructed");
}

NaviStreamBase::~NaviStreamBase() {
}

google::protobuf::Message *NaviStreamBase::newReceiveMessageBase(
        google::protobuf::Arena *arena) const
{
    return google::protobuf::Arena::CreateMessage<NaviMessage>(arena);
}

bool NaviStreamBase::receiveBase(const multi_call::GigStreamMessage &message) {
    DomainHolderScope<GraphDomainRemoteBase> scope(_domainHolder);
    auto domain = scope.getDomain();
    if (!domain) {
        std::string msg;
        if (message.message) {
            msg = message.message->DebugString();
        }
        NAVI_LOG(SCHEDULE1,
                 "receive failed, null domain, partId: %d, handler: [%p], msg: %s",
                 message.partId,
                 (void *)message.handlerId,
                 msg.c_str());
        return false;
    }
    if (!domain->receive(message)) {
        NAVI_LOG(ERROR, "receive failed, partId: %d, handler: [%p]", message.partId, (void *)message.handlerId);
        return false;
    }
    return true;
}

void NaviStreamBase::receiveCancelBase(const multi_call::GigStreamMessage &message,
                                       multi_call::MultiCallErrorCode ec)
{
    DomainHolderScope<GraphDomainRemoteBase> scope(_domainHolder);
    auto domain = scope.getDomain();
    if (!domain) {
        NAVI_LOG(SCHEDULE1,
                 "receive cancel failed, null domain, partId: %d, handler: [%p]",
                 message.partId,
                 (void *)message.handlerId);
        return;
    }
    domain->receiveCancel(message, ec);
}

void NaviStreamBase::notifyIdleBase(multi_call::PartIdTy partId) {
    DomainHolderScope<GraphDomainRemoteBase> scope(_domainHolder);
    auto domain = scope.getDomain();
    if (!domain) {
        return;
    }
    domain->notifyIdle(partId);
}

bool NaviStreamBase::notifyReceiveBase(const multi_call::GigStreamBasePtr &stream, multi_call::PartIdTy partId) {
    if (!_threadPool) {
        NAVI_LOG(DEBUG, "no receive thread pool, partId [%d], use sync receive", partId);
        return stream->asyncReceive(partId);
    }
    auto item = new NaviStreamReceiveItem(stream, partId);
    _threadPool->push(item);
    return true;
}

bool NaviStreamBase::setDomain(const GraphDomainRemoteBasePtr &domain) {
    NAVI_LOG(SCHEDULE1, "set domain [%p]", domain.get());
    auto ret = _domainHolder.init(domain);
    _initialized = true;
    _domainGdb = domain.get();
    return ret;
}

void NaviStreamBase::setThreadPool(const std::shared_ptr<NaviThreadPool> &threadPool) {
    _threadPool = threadPool;
}

bool NaviStreamBase::initialized() {
    return _initialized;
}

void NaviStreamBase::finish() {
    NAVI_LOG(SCHEDULE1, "finish");
    doFinish();
    _domainHolder.terminate();
}

}
