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
#include "swift/client/trace/TraceSwiftLogger.h"

#include "swift/client/MessageInfo.h"
#include "swift/client/SwiftWriter.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace client {

AUTIL_LOG_SETUP(swift, TraceSwiftLogger);

TraceSwiftLogger::TraceSwiftLogger(const MessageMeta &meta, SwiftWriter *tracingWriter)
    : _msgMeta(meta), _tracingWriter(tracingWriter) {}

TraceSwiftLogger::~TraceSwiftLogger() { flush(); }

bool TraceSwiftLogger::doWrite(const std::string &content) {
    if (_tracingWriter == nullptr) {
        return false;
    }
    MessageInfo msgInfo;
    msgInfo.uint8Payload = _msgMeta.mask;
    msgInfo.uint16Payload = _msgMeta.hashVal;
    msgInfo.data = content;
    auto ec = _tracingWriter->write(msgInfo);
    if (ec != protocol::ERROR_NONE) {
        AUTIL_LOG(INFO, "topic [%s] write msg failed", _msgMeta.topicName.c_str());
        return false;
    }
    return true;
}

bool TraceSwiftLogger::flush() {
    if (_tracingWriter == nullptr) {
        return false;
    }
    _tracingWriter->waitSent();
    return true;
}

} // end namespace client
} // end namespace swift
