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
#include "swift/client/trace/TraceFileLogger.h"

#include <iosfwd>

#include "alog/Appender.h"
#include "alog/Layout.h"
#include "autil/Log.h"

using namespace alog;
using namespace std;

namespace swift {
namespace client {
static const string traceFileLoggerName = "swift.TraceFileLogger";

AUTIL_LOG_SETUP(swift, TraceFileLogger);

TraceFileLogger::TraceFileLogger() {}

TraceFileLogger::~TraceFileLogger() { flush(); }

bool TraceFileLogger::initLogger(const std::string &logFile) {

    Logger *logger = Logger::getLogger(traceFileLoggerName.c_str());
    if (logger == nullptr) {
        return false;
    }
    if (logger->getInheritFlag()) {
        // reset trace appender for binary logging
        string defaultFileName = logFile;
        Appender *appender = FileAppender::getAppender(defaultFileName.c_str());
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
    return true;
}
bool TraceFileLogger::doWrite(const std::string &content) {
    AUTIL_LOG_BINARY(INFO, content);
    return true;
}

bool TraceFileLogger::flush() {
    Logger *logger = Logger::getLogger(traceFileLoggerName.c_str());
    if (logger) {
        logger->flush();
    }
    return true;
}
} // end namespace client
} // end namespace swift
