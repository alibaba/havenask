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
#include "swift/client/trace/ReadMessageTracer.h"

#include <algorithm>
#include <functional>
#include <iosfwd>
#include <memory>
#include <stdint.h>

#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/TraceMessage.pb.h"

using namespace std;

namespace swift {
namespace client {

AUTIL_LOG_SETUP(swift, TraceMsgContainer);
AUTIL_LOG_SETUP(swift, ReadMessageTracer);
static const int TRACE_THRESHOLD = 1024;

void TraceMsgContainer::tracingMsg(const protocol::Message &msg) {
    auto *traceMessage = writeMessage.add_tracemessages();
    traceMessage->set_msgid(msg.msgid());
    traceMessage->set_timestampinsecond((uint32_t)(msg.timestamp() / 1000000));
    traceMessage->set_requestuuid(msg.requestuuid());
    traceMessage->set_uint16payload(msg.uint16payload());
    traceMessage->set_uint8maskpayload(msg.uint8maskpayload());
    traceMessage->set_datalen(msg.data().size());
}

bool TraceMsgContainer::isFull() const { return writeMessage.tracemessages_size() >= TRACE_THRESHOLD; }

void TraceMsgContainer::swap(TraceMsgContainer &otherContainer) { writeMessage.Swap(&otherContainer.writeMessage); }

std::string TraceMsgContainer::toString() const {
    string content;
    if (!writeMessage.SerializeToString(&content)) {
        AUTIL_LOG(ERROR, "serialize to string failed, messgae count [%d]", writeMessage.tracemessages_size());
    }
    return content;
}

bool TraceMsgContainer::fromString(const std::string &str) {
    if (!writeMessage.ParseFromString(str)) {
        AUTIL_LOG(ERROR, "parse from string failed, string len [%lu]", str.size());
        return false;
    }
    return true;
}

ReadMessageTracer::ReadMessageTracer(vector<TraceLoggerPtr> traceLoggerVec) : _traceLoggerVec(traceLoggerVec) {
    _sendTraceThread =
        autil::LoopThread::createLoopThread(std::bind(&ReadMessageTracer::sendTrace, this), 10 * 1000ll, "send_trace");
}

ReadMessageTracer::~ReadMessageTracer() {
    _sendTraceThread.reset();
    flush();
}

void ReadMessageTracer::tracingMsg(const protocol::Message &msg) {
    _curContainer.tracingMsg(msg);
    if (_curContainer.isFull()) {
        TraceMsgContainer tmpContainer;
        _curContainer.swap(tmpContainer);
        addContainerToSend(tmpContainer);
    }
}

void ReadMessageTracer::tracingMsg(const protocol::Messages &msgs) {
    int size = msgs.msgs_size();
    for (int i = 0; i < size; i++) {
        tracingMsg(msgs.msgs(i));
    }
}

void ReadMessageTracer::addContainerToSend(TraceMsgContainer &container) {
    *container.writeMessage.mutable_readerinfo() = _readerInfo;
    autil::ScopedLock lock(_mutex);
    _toSendContainerVec.emplace_back(container);
}

void ReadMessageTracer::flush() {
    TraceMsgContainer tmpContainer;
    _curContainer.swap(tmpContainer);
    addContainerToSend(tmpContainer);
    sendTrace();
    for (const auto &logger : _traceLoggerVec) {
        logger->flush();
    }
}

void ReadMessageTracer::sendTrace() {
    {
        autil::ScopedLock lock(_mutex);
        if (_toSendContainerVec.empty()) {
            return;
        }
    }
    vector<TraceMsgContainer> containerVec;
    {
        autil::ScopedLock lock(_mutex);
        containerVec.swap(_toSendContainerVec);
    }
    for (const auto &container : containerVec) {
        const string &content = container.toString();
        for (const auto &logger : _traceLoggerVec) {
            logger->write(content);
        }
    }
}

} // namespace client
} // namespace swift
