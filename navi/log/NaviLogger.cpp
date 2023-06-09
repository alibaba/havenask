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
#include "navi/log/AlogAppender.h"
#include "navi/log/LoggingEvent.h"
#include "navi/log/NaviLogManager.h"
#include "navi/log/TraceAppender.h"
#include "navi/util/CommonUtil.h"
#include "aios/network/arpc/arpc/common/Exception.h"
#include <stdarg.h>

namespace navi {

thread_local const NaviObjectLogger *NAVI_TLS_LOGGER = nullptr;

NaviLogger::NaviLogger(size_t maxMessageLength,
                       const std::shared_ptr<NaviLogManager> &logManager)
    : _level(LOG_LEVEL_DISABLE)
    , _stopped(false)
    , _maxMessageLength(maxMessageLength)
    , _traceAppender(nullptr)
    , _logManager(logManager)
    , _hasError(false)
{
}

NaviLogger::~NaviLogger() {
    auto _logger = this;
    NAVI_LOG(SCHEDULE2, "destruct: %p", this);
    for (auto appender : _ownAppenders) {
        delete appender;
    }
}

void NaviLogger::init(SessionId id, const std::vector<FileAppender *> &appenders) {
    setLoggerId(id);
    for (auto appender : appenders) {
        _appenders.push_back((Appender *)appender);
    }
    updateLogLevel();
    auto _logger = this;
    NAVI_LOG(SCHEDULE2, "construct: %p", this);
}

void NaviLogger::stop() {
    bool expect = false;
    _stopped.compare_exchange_weak(expect, true);
}

bool NaviLogger::stopped() {
    return _stopped.load(std::memory_order_relaxed);
}

void NaviLogger::setLoggerId(SessionId id) {
    _id = id;
    updateName();
}

SessionId NaviLogger::getLoggerId() const {
    return _id;
}

const std::string &NaviLogger::getName() const {
    return _name;
}

void NaviLogger::addAppender(Appender *appender) {
    _appenders.push_back(appender);
    _ownAppenders.push_back(appender);
    updateLogLevel();
}

LogLevel NaviLogger::getTraceLevel() const {
    return _traceAppender ? _traceAppender->getLevel() : LOG_LEVEL_DISABLE;
}

void NaviLogger::setTraceAppender(TraceAppender *appender) {
    _traceAppender = appender;
    addAppender(appender);
}

void NaviLogger::collectTrace(TraceCollector &collector) {
    if (_traceAppender) {
        _traceAppender->collectTrace(collector);
    }
}

void NaviLogger::updateLogLevel() {
    LogLevel maxLevel = LOG_LEVEL_DISABLE;
    for (auto appender : _appenders) {
        maxLevel = std::max(maxLevel, appender->getLevel());
    }
    _level = maxLevel;
}

void NaviLogger::updateName() {
    _name = CommonUtil::formatSessionId(_id, _logManager->getInstanceId());
}

void NaviLogger::log(LogLevel level,
                     void *object,
                     const std::string &prefix,
                     const char *file,
                     int line,
                     const char *func,
                     const char *fmt,
                     va_list ap)
{
    if (__builtin_expect(stopped(), 0)) {
        return;
    }
    char buffer[_maxMessageLength];
    vsnprintf(buffer, _maxMessageLength, fmt, ap);
    LoggingEvent event(_name, object, prefix, buffer, level, file, line, func);
    _log(event);
}

void NaviLogger::log(LogLevel level,
                     void *object,
                     const std::string &prefix,
                     const char *fmt,
                     va_list ap)
{
    if (__builtin_expect(stopped(), 0)) {
        return;
    }
    static thread_local size_t bufferSize = _maxMessageLength;
    static thread_local std::unique_ptr<char[]> bufferStorage(new char[bufferSize]);
    if (unlikely(bufferSize != _maxMessageLength)) {
        bufferSize = _maxMessageLength;
        bufferStorage.reset(new char[bufferSize]);
    }

    char *buffer = bufferStorage.get();
    vsnprintf(buffer, _maxMessageLength, fmt, ap);
    LoggingEvent event(_name, object, prefix, buffer, level, "", 0, "");
    _log(event);
}

void NaviLogger::_log(LoggingEvent& event)
{
    if (event.level <= LOG_LEVEL_ERROR && !_hasError) {
        std::string btStr;
        if (_traceAppender) {
            btStr = getCurrentBtString();
        }
        autil::ScopedLock lock(_lock);
        if (!_hasError) {
            _hasError = true;
            _firstErrorEvent = event;
            _firstErrorBackTrace = std::move(btStr);
        }
    }
    if (event.name.empty()) {
        event.name = _name;
    }
    {
        for (const auto &appender : _appenders) {
            if (event.level <= appender->getLevel()) {
                appender->append(event);
            }
        }
    }
}

std::string NaviLogger::getCurrentBtString() {
    arpc::common::Exception e;
    e.Init("", "", 0);
    return e.ToString();
}

LoggingEvent NaviLogger::firstErrorEvent() {
    autil::ScopedLock lock(_lock);
    return _firstErrorEvent;
}

std::string NaviLogger::firstErrorBackTrace() {
    autil::ScopedLock lock(_lock);
    return _firstErrorBackTrace;
}

}
