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
#include "navi/log/TraceAppender.h"
#include "navi/log/LoggingEvent.h"
#include "navi/log/NaviLogManager.h"
#include <iostream>

namespace navi {

TraceAppender::TraceAppender(const std::string &levelStr)
    : Appender(levelStr, nullptr)
    , _logManager(nullptr)
{
    auto patternLayout = new PatternLayout();
    patternLayout->setLogPattern(DEFAULT_LOG_PATTERN);
    setLayout(patternLayout);
}

TraceAppender::~TraceAppender() {
}

void TraceAppender::addBtFilter(const LogBtFilterParam &param) {
    _btFilter.addParam(param);
}

void TraceAppender::setLogManager(const NaviLogManager *logManager) {
    _logManager = logManager;
}

int TraceAppender::append(LoggingEvent& event) {
    auto traceEvent = event;
    if (unlikely(_btFilter.pass(traceEvent))) {
        traceEvent.bt = _logManager->getCurrentBtString();
    }
    autil::ScopedLock lock(_lock);
    _traceCollector.append(std::move(traceEvent));
    return 0;
}

void TraceAppender::collectTrace(TraceCollector &collector) {
    autil::ScopedLock lock(_lock);
    collector.merge(_traceCollector);
}

}

