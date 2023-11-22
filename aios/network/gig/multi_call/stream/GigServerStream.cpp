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
#include "aios/network/gig/multi_call/stream/GigServerStream.h"

#include "aios/network/gig/multi_call/grpc/server/GrpcServerStreamHandler.h"

using namespace std;

namespace multi_call {

AUTIL_DECLARE_AND_SETUP_LOGGER(multi_call, GigServerStream);

GigServerStream::GigServerStream() {
}

GigServerStream::~GigServerStream() {
}

void GigServerStream::setHandler(const GigStreamHandlerBasePtr &handler) {
    _handler = handler;
}

std::shared_ptr<QueryInfo> GigServerStream::getQueryInfo() const {
    auto serverHandler = dynamic_cast<GrpcServerStreamHandler *>(_handler.get());
    if (!serverHandler) {
        return nullptr;
    }
    return serverHandler->getQueryInfo();
}

std::string GigServerStream::getPeer() const {
    auto serverHandler = dynamic_cast<GrpcServerStreamHandler *>(_handler.get());
    if (!serverHandler) {
        return std::string();
    }
    return serverHandler->getPeer();
}

bool GigServerStream::send(PartIdTy partId, bool eof, google::protobuf::Message *message) {
    if (!_handler) {
        return false;
    }
    auto sendMessage = GigStreamHandlerBase::createSendBufferMessage(eof, false, message);
    if (eof) {
        _handler->endReceive();
    }
    return _handler->send(sendMessage);
}

void GigServerStream::sendCancel(PartIdTy partId, google::protobuf::Message *message) {
    if (!_handler) {
        return;
    }
    auto sendMessage = GigStreamHandlerBase::createSendBufferMessage(true, true, message);
    _handler->send(sendMessage);
}

} // namespace multi_call
