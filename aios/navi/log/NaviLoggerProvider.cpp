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
#include "navi/log/NaviLogger.h"
#include "navi/log/NaviLogManager.h"
#include "navi/log/TraceAppender.h"
#include "navi/log/ConsoleAppender.h"

namespace navi {

NaviLoggerProvider::NaviLoggerProvider(const std::string &logLevel,
                                       const std::string &prefix)
    : _logManager(new NaviLogManager(0))
{
    LogConfig config;
    config.maxMessageLength = 1024 * 1024;
    _logManager->init(config);
    auto logger = _logManager->createLogger();
    logger->setTraceAppender(std::make_shared<TraceAppender>(logLevel));
    logger->addAppender(std::make_shared<ConsoleAppender>(logLevel));
    _logger = new NaviObjectLogger(this, prefix.c_str(), logger);
    _scope = new NaviLoggerScope(*_logger);
}

NaviLoggerProvider::~NaviLoggerProvider() {
    delete _scope;
    delete _logger;
}

std::string NaviLoggerProvider::getErrorMessage() const {
    auto errorEvent = _logger->logger->firstErrorEvent();
    if (errorEvent) {
        return errorEvent->message;
    } else {
        return std::string();
    }
}

std::vector<std::string> NaviLoggerProvider::getTrace(
        const std::string &filter,
        const std::string &formatPattern) const
{
    TraceCollector collector;
    collector.setFormatPattern(formatPattern);
    _logger->logger->collectTrace(collector);
    std::vector<std::string> traceVec;
    collector.format(traceVec);
    std::vector<std::string> ret;
    ret.reserve(traceVec.size());
    for (const auto &msg : traceVec) {
        if (!filter.empty()) {
            if (msg.find(filter) != std::string::npos) {
                ret.push_back(msg);
            }
        } else {
            ret.push_back(msg);
        }
    }
    return ret;
}
}
