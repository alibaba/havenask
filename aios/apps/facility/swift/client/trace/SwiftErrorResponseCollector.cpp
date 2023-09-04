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
#include "swift/client/trace/SwiftErrorResponseCollector.h"

#include <iosfwd>

#include "alog/Appender.h"
#include "alog/Layout.h"
#include "autil/TimeUtility.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/SwiftWriter.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/TraceMessage.pb.h"

using namespace std;
using namespace alog;

namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, SwiftErrorResponseCollector);
static const string loggerName = "swift.SwiftErrorResponseCollector";

SwiftErrorResponseCollector::SwiftErrorResponseCollector(const std::string &logFileName) {
    initFileLogger(logFileName);
}

SwiftErrorResponseCollector::~SwiftErrorResponseCollector() {
    Logger *logger = Logger::getLogger(loggerName.c_str());
    if (logger) {
        logger->flush();
    }
}

bool SwiftErrorResponseCollector::logResponse(protocol::ReaderInfo &readerInfo,
                                              ErrorResponseType type,
                                              const std::string &content) {
    protocol::WriteErrorResponse errResponse;
    *errResponse.mutable_readerinfo() = readerInfo;
    errResponse.set_type(type);
    errResponse.set_content(content);
    string str;
    errResponse.SerializeToString(&str);
    logInFile(str);
    if (_swiftWriter) {
        MessageInfo msgInfo;
        msgInfo.uint8Payload = 251;
        msgInfo.uint16Payload = 65531;
        msgInfo.data = str;
        auto ec = _swiftWriter->write(msgInfo);
        if (ec != protocol::ERROR_NONE) {
            return false;
        }
    }
    return true;
}

void SwiftErrorResponseCollector::setSwiftWriter(SwiftWriter *writer) { _swiftWriter = writer; }

void SwiftErrorResponseCollector::resetSwiftWriter() { _swiftWriter = nullptr; }

void SwiftErrorResponseCollector::logInFile(const std::string &content) {
    ErrorResponseLogHeader header;
    header.logLen = content.size();
    header.timestamp = autil::TimeUtility::currentTime();
    AUTIL_LOG_BINARY(INFO, std::string((char *)&header, sizeof(header)) + content);
}

void SwiftErrorResponseCollector::initFileLogger(const std::string &logFileName) {
    Logger *logger = Logger::getLogger(loggerName.c_str());
    if (logger == nullptr) {
        return;
    }
    if (logger->getInheritFlag()) {
        // reset trace appender for binary logging
        Appender *appender = FileAppender::getAppender(logFileName.c_str());
        appender->setAutoFlush(true);
        FileAppender *fileAppender = (FileAppender *)appender;
        fileAppender->setMaxSize(1024);
        fileAppender->setCacheLimit(64);
        fileAppender->setHistoryLogKeepCount(20);
        fileAppender->setAsyncFlush(true);
        fileAppender->setFlushIntervalInMS(1000);
        fileAppender->setFlushThreshold(1024 * 1024);
        fileAppender->setCompress(true);
        fileAppender->setLayout(new BinaryLayout());
        logger->setAppender(appender);
        logger->setInheritFlag(false);
        logger->setLevel(LOG_LEVEL_INFO);
    }
}

} // end namespace client
} // end namespace swift
