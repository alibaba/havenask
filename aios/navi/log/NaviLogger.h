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
#ifndef NAVI_NAVILOGGER_H
#define NAVI_NAVILOGGER_H

/*
we copy design and code from alog for navi's log
*/

#include "navi/common.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/log/TraceCollector.h"
#include "autil/Lock.h"
#include <vector>
#include <list>
#include <set>

namespace navi {

class LoggingEvent;
class Appender;
class FileAppender;
class TraceAppender;
class NaviLogManager;

class NaviLogger
{
public:
    NaviLogger(size_t maxMessageLength,
               const std::shared_ptr<NaviLogManager> &logManager);
    ~NaviLogger();
public:
    void init(SessionId id, const std::vector<FileAppender *> &appenders);
    void stop();
    bool stopped();
    void setLoggerId(SessionId id);
    SessionId getLoggerId() const;
    const std::string &getName() const;
public:
    void log(LogLevel level, void *object, const std::string &prefix,
             const char *format, va_list ap);
    void log(LogLevel level, void *object, const std::string &prefix,
             const char *filename, int line, const char *function,
             const char *format, va_list ap);
    inline bool isLevelEnabled(LogLevel level) {
        return (level <= _level)? true : false;
    }
    LogLevel getTraceLevel() const;
    void addAppender(Appender *appender);
    bool hasTraceAppender() const;
    void setTraceAppender(TraceAppender *appender);
    void collectTrace(TraceCollector &collector);
    LoggingEvent firstErrorEvent();
    std::string firstErrorBackTrace();
    std::string getCurrentBtString() const;
    std::string getExceptionMessage(const autil::legacy::ExceptionBase &e) const;
    void updateTraceLevel(const std::string &levelStr);
private:
    void updateLogLevel();
    void updateName();
    void _log(LoggingEvent& event);
private:
    SessionId _id;
    std::string _name;
    LogLevel _level;
    std::atomic<bool> _stopped;
    size_t _maxMessageLength;
    std::vector<Appender *> _appenders;
    std::vector<Appender *> _ownAppenders;
    TraceAppender *_traceAppender;
    std::shared_ptr<NaviLogManager> _logManager;
    autil::ThreadMutex _lock;
    bool _hasError;
    LoggingEvent _firstErrorEvent;
    std::string _firstErrorBackTrace;
};

NAVI_TYPEDEF_PTR(NaviLogger);

class NaviObjectLogger {
public:
    NaviObjectLogger()
        : object(nullptr)
    {
    }
    explicit NaviObjectLogger(void *object_, const char *prefix_,
                              const NaviLoggerPtr &logger_)
        : object(object_)
        , prefix(prefix_)
        , logger(logger_)
    {
    }
    ~NaviObjectLogger() {
    }
public:
    void addPrefix(const char *fmt, ...) __attribute__((format(printf, 2, 3)));
public:
    void *object;
    std::string prefix;
    NaviLoggerPtr logger;
};

inline void NaviObjectLogger::addPrefix(const char *fmt, ...) {
    constexpr int32_t MAX_PREFIX_LENGTH = 256;
    char buffer[MAX_PREFIX_LENGTH];
    va_list ap;
    va_start(ap, fmt);
    auto n = vsnprintf(buffer, MAX_PREFIX_LENGTH, fmt, ap);
    va_end(ap);
    n = std::max(0, std::min(n, MAX_PREFIX_LENGTH - 1));
    if (!prefix.empty()) {
        prefix.append(" ", 1);
    }
    prefix.append(buffer, n);
}

thread_local extern const NaviObjectLogger *NAVI_TLS_LOGGER;

class NaviLoggerScope {
public:
    NaviLoggerScope(const NaviObjectLogger &logger) : _oldLogger(NAVI_TLS_LOGGER) { NAVI_TLS_LOGGER = &logger; }
    NaviLoggerScope(const NaviObjectLogger *logger) : _oldLogger(NAVI_TLS_LOGGER) { NAVI_TLS_LOGGER = logger; }
    ~NaviLoggerScope() {
        NAVI_TLS_LOGGER = _oldLogger;
    }
private:
    const NaviObjectLogger *_oldLogger;
};

#define DECLARE_LOGGER() navi::NaviObjectLogger _logger;

inline void NAVI_EVENT_LOG(const NaviObjectLogger &objectLogger,
                           LogLevel level,
                           const char *file,
                           int line,
                           const char *func,
                           const char *fmt, ...) __attribute__((format(printf, 6, 7)));
inline void NAVI_EVENT_LOG(const NaviObjectLogger *objectLogger,
                           LogLevel level,
                           const char *file,
                           int line,
                           const char *func,
                           const char *fmt, ...) __attribute__((format(printf, 6, 7)));
inline void NAVI_EVENT_LOG(const NaviLoggerPtr &logger,
                           LogLevel level,
                           const char *file,
                           int line,
                           const char *func,
                           const char *fmt, ...) __attribute__((format(printf, 6, 7)));
inline void NAVI_EVENT_LOG(NaviLogger *logger,
                           LogLevel level,
                           const char *file,
                           int line,
                           const char *func,
                           const char *fmt, ...) __attribute__((format(printf, 6, 7)));

inline bool logEnable(const NaviObjectLogger &objectLogger, LogLevel level) {
    auto logger = objectLogger.logger.get();
    return logger && logger->isLevelEnabled(level);
}

inline void NAVI_EVENT_LOG(const NaviObjectLogger &objectLogger,
                           LogLevel level,
                           const char *file,
                           int line,
                           const char *func,
                           const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    objectLogger.logger->log(level, objectLogger.object, objectLogger.prefix,
                             file, line, func, fmt, ap);
    va_end(ap);
}

inline bool logEnable(const NaviObjectLogger *objectLogger, LogLevel level) {
    if (!objectLogger) {
        return false;
    }
    auto logger = objectLogger->logger.get();
    return logger && logger->isLevelEnabled(level);
}

inline void NAVI_EVENT_LOG(const NaviObjectLogger *objectLogger,
                           LogLevel level,
                           const char *file,
                           int line,
                           const char *func,
                           const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    objectLogger->logger->log(level, objectLogger->object, objectLogger->prefix,
                              file, line, func, fmt, ap);
    va_end(ap);
}

inline bool logEnable(const NaviLoggerPtr &logger, LogLevel level) {
    return logger && logger->isLevelEnabled(level);
}

inline void NAVI_EVENT_LOG(const NaviLoggerPtr &logger,
                           LogLevel level,
                           const char *file,
                           int line,
                           const char *func,
                           const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    logger->log(level, nullptr, std::string(), file, line, func, fmt, ap);
    va_end(ap);
}

inline bool logEnable(NaviLogger *logger, LogLevel level) {
    return logger && logger->isLevelEnabled(level);
}

inline void NAVI_EVENT_LOG(NaviLogger *logger,
                           LogLevel level,
                           const char *file,
                           int line,
                           const char *func,
                           const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    logger->log(level, nullptr, std::string(), file, line, func, fmt, ap);
    va_end(ap);
}

#define NAVI_LOG_INTERNAL(logger, level, format, args...)                      \
    {                                                                          \
        if (__builtin_expect(logEnable(logger, navi::LOG_LEVEL_##level), 0)) {  \
            NAVI_EVENT_LOG(logger, navi::LOG_LEVEL_##level, __FILE__, __LINE__, \
                           __FUNCTION__, format, ##args);                      \
        }                                                                      \
    }

#define NAVI_LOG(level, format, args...)                                       \
    NAVI_LOG_INTERNAL(_logger, level, format, ##args)

#define NAVI_KERNEL_LOG(level, format, args...)                                \
    NAVI_LOG_INTERNAL(navi::NAVI_TLS_LOGGER, level, format, ##args)

#define NAVI_TLS_LOG(level, format, args...)                                   \
    NAVI_LOG_INTERNAL(navi::NAVI_TLS_LOGGER, level, format, ##args)

#define NAVI_INTERVAL_LOG(logInterval, level, format, args...)                                                         \
    do {                                                                                                               \
        static int logCounter;                                                                                         \
        if (0 == logCounter) {                                                                                         \
            NAVI_LOG_INTERNAL(_logger, level, format, ##args);                                                         \
            logCounter = logInterval;                                                                                  \
        }                                                                                                              \
        logCounter--;                                                                                                  \
    } while (0)
}

#endif
