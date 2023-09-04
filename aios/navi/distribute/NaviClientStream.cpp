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
#include "navi/distribute/NaviClientStream.h"
#include "navi/engine/RemoteSubGraphBase.h"

namespace navi {

NaviClientStream::NaviClientStream(const std::string &bizName,
                                   const NaviLoggerPtr &logger)
    : GigClientStream(bizName, NAVI_RPC_METHOD_NAME)
    , NaviStreamBase(logger)
{
    _logger.addPrefix("client: [%s]", bizName.c_str());
    NAVI_LOG(SCHEDULE1, "constructed");
}

NaviClientStream::~NaviClientStream() {
    NAVI_LOG(SCHEDULE1, "destructed");
}

google::protobuf::Message *NaviClientStream::newReceiveMessage(
        google::protobuf::Arena *arena) const
{
    return NaviStreamBase::newReceiveMessageBase(arena);
}

bool NaviClientStream::receive(const multi_call::GigStreamMessage &message) {
    return NaviStreamBase::receiveBase(message);
}

void NaviClientStream::receiveCancel(const multi_call::GigStreamMessage &message,
                                     multi_call::MultiCallErrorCode ec)
{
    NaviStreamBase::receiveCancelBase(message, ec);
}

bool NaviClientStream::notifyReceive(multi_call::PartIdTy partId) {
    return NaviStreamBase::notifyReceiveBase(shared_from_this(), partId);
}

void NaviClientStream::notifyIdle(multi_call::PartIdTy partId) {
    NaviStreamBase::notifyIdleBase(partId);
}

}
