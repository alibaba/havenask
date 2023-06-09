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
#include "navi/log/NaviLogger.h"
#include <iostream>

namespace navi {

void TraceBtFilterMap::addParam(const TraceBtFilterParam &param) {
    _filterMap[param.line].insert(param.file);
}

bool TraceBtFilterMap::pass(const LoggingEvent &event) {
    auto lineIt = _filterMap.find(event.line);
    if (_filterMap.end() == lineIt) {
        return false;
    }
    const auto &fileSet = lineIt->second;
    return fileSet.end() != fileSet.find(event.file);
}

TraceAppender::TraceAppender(const std::string &levelStr)
    : Appender(levelStr, nullptr)
{
    auto patternLayout = new PatternLayout();
    patternLayout->setLogPattern(DEFAULT_LOG_PATTERN);
    setLayout(patternLayout);
}

TraceAppender::~TraceAppender() {
}

void TraceAppender::addBtFilter(const TraceBtFilterParam &param) {
    _filterMap.addParam(param);
}

int TraceAppender::append(LoggingEvent& event) {
    auto traceEvent = event;
    if (_filterMap.pass(traceEvent)) {
        traceEvent.bt = NaviLogger::getCurrentBtString();
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

