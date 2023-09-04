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
#pragma once

#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "swift/client/trace/TraceLogger.h"
#include "swift/common/Common.h"
#include "swift/protocol/TraceMessage.pb.h"

namespace swift {
namespace protocol {
class Message;
class Messages;
} // namespace protocol

namespace client {

class TraceMsgContainer {
public:
    void tracingMsg(const protocol::Message &msg);
    bool isFull() const;
    void swap(TraceMsgContainer &otherContainer);
    std::string toString() const;
    bool fromString(const std::string &str);

public:
    protocol::WriteTraceMessage writeMessage;

private:
    AUTIL_LOG_DECLARE();
};

class ReadMessageTracer {
public:
    ReadMessageTracer(std::vector<TraceLoggerPtr> traceLoggerVec);
    ~ReadMessageTracer();

private:
    ReadMessageTracer(const ReadMessageTracer &);
    ReadMessageTracer &operator=(const ReadMessageTracer &);

public:
    void tracingMsg(const protocol::Message &msg);
    void tracingMsg(const protocol::Messages &msgs);
    void setReaderInfo(const protocol::ReaderInfo &readerInfo) { _readerInfo = readerInfo; }

public:
    void flush();

private:
    void addContainerToSend(TraceMsgContainer &curContainer);
    void sendTrace();

private:
    std::vector<TraceLoggerPtr> _traceLoggerVec;
    protocol::ReaderInfo _readerInfo;
    TraceMsgContainer _curContainer;
    std::vector<TraceMsgContainer> _toSendContainerVec;
    autil::ThreadMutex _mutex;
    autil::LoopThreadPtr _sendTraceThread;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(ReadMessageTracer);

} // namespace client
} // namespace swift
