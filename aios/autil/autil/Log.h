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

#include <iostream>

#include "alog/Logger.h" // IWYU pragma: export
#include "alog/LogStream.h" // IWYU pragma: export
#include "alog/Configurator.h"
#include "autil/TimeUtility.h"

#define AUTIL_ROOT_LOG_CONFIG() alog::Configurator::configureRootLogger()
#define AUTIL_LOG_CONFIG(filename) do {                         \
        try {                                                   \
            alog::Configurator::configureLogger(filename);      \
        } catch(std::exception &e) {                            \
            std::cerr << "WARN! Failed to configure logger!"    \
                      << e.what() << ",use default log conf."   \
                      << std::endl;                             \
            AUTIL_ROOT_LOG_CONFIG();                            \
        }                                                       \
    }while(0)

#define AUTIL_LOG_CONFIG_FROM_STRING(content) do {                  \
        try {                                                       \
            alog::Configurator::configureLoggerFromString(content); \
        } catch(std::exception &e) {                                \
            std::cerr << "configure logger use ["                   \
                      << content<< "] failed"                       \
                      << std::endl;                                 \
            AUTIL_ROOT_LOG_CONFIG();                                \
        }                                                           \
    }while(0)


#define AUTIL_ROOT_LOG_SETLEVEL(level)                                  \
    alog::Logger::getRootLogger()->setLevel(alog::LOG_LEVEL_##level)

#define AUTIL_LOG_DECLARE() static alog::Logger *_logger

#define AUTIL_LOG_SETUP(n, c) alog::Logger *c::_logger  \
    = alog::Logger::getLogger(#n "." #c)

#define AUTIL_LOG_SETUP_TEMPLATE(n, c, T) template <typename T> \
    alog::Logger *c<T>::_logger                                 \
    = alog::Logger::getLogger(#n "." #c)

#define AUTIL_LOG_SETUP_TEMPLATE_2(n, c, T1, T2) template <typename T1, typename T2> \
    alog::Logger *c<T1,T2>::_logger                                     \
    = alog::Logger::getLogger(#n "." #c)

#define AUTIL_LOG_SETUP_TEMPLATE_3(n,c, T1, T2, T3)              \
    template <typename T1, typename T2, typename T3>             \
    alog::Logger *c<T1, T2, T3>::_logger                         \
    = alog::Logger::getLogger(#n "."  #c)

#define AUTIL_LOG_SETUP_TEMPLATE_WITH_TYPENAME(n,c, typename, T) template <typename T> \
    alog::Logger *c<T>::_logger                                 \
    = alog::Logger::getLogger(#n "."  #c)

#define AUTIL_LOG_SETUP_TEMPLATE_WITH_TYPENAME2(n,c, typename1, T1, typename2, T2) template <typename1 T1, typename2 T2> \
    alog::Logger *c<T1, T2>::_logger                                    \
    = alog::Logger::getLogger(#n "."  #c)

#define AUTIL_LOG(level, format, args...) ALOG_LOG(_logger, alog::LOG_LEVEL_##level, format, ##args)
#define AUTIL_SLOG(level) ALOG_STREAM(_logger, alog::LOG_LEVEL_##level)
#define AUTIL_SLOG_IF(level, condition) ALOG_STREAM_IF(_logger, alog::LOG_LEVEL_##level, condition)
#define AUTIL_SLOG_ASSERT(condition) ALOG_STREAM_ASSERT(_logger, condition)

#define AUTIL_SLOG_EVERY_N(level, n) ALOG_STREAM_EVERY_N(_logger, alog::LOG_LEVEL_##level, n)
#define AUTIL_SLOG_FIRST_N(level, n) ALOG_STREAM_FIRST_N(_logger, alog::LOG_LEVEL_##level, n)
#define AUTIL_SLOG_EVERY_T(level, seconds) ALOG_STREAM_EVERY_T(_logger, alog::LOG_LEVEL_##level, seconds)
#define AUTIL_SLOG_IF_EVERY_N(level, condition, n) ALOG_STREAM_IF_EVERY_N(_logger, alog::LOG_LEVEL_##level, condition, n)

#define AUTIL_CHECK_EQ(level, val1, val2) ALOG_CHECK_EQ(_logger, alog::LOG_LEVEL_##level, val1, val2)
#define AUTIL_CHECK_NE(level, val1, val2) ALOG_CHECK_NE(_logger, alog::LOG_LEVEL_##level, val1, val2)
#define AUTIL_CHECK_LE(level, val1, val2) ALOG_CHECK_LE(_logger, alog::LOG_LEVEL_##level, val1, val2)
#define AUTIL_CHECK_LT(level, val1, val2) ALOG_CHECK_LT(_logger, alog::LOG_LEVEL_##level, val1, val2)
#define AUTIL_CHECK_GE(level, val1, val2) ALOG_CHECK_GE(_logger, alog::LOG_LEVEL_##level, val1, val2)
#define AUTIL_CHECK_GT(level, val1, val2) ALOG_CHECK_GT(_logger, alog::LOG_LEVEL_##level, val1, val2)
#define AUTIL_CHECK_NOTNULL(level, val) ALOG_CHECK_NOTNULL(_logger, alog::LOG_LEVEL_##level, val)

#define AUTIL_CHECK_STREQ(level, s1, s2) ALOG_CHECK_STREQ(_logger, alog::LOG_LEVEL_##level, s1, s2)
#define AUTIL_CHECK_STRNE(level, s1, s2) ALOG_CHECK_STRNE(_logger, alog::LOG_LEVEL_##level, s1, s2)
#define AUTIL_CHECK_STRCASEEQ(level, s1, s2) ALOG_CHECK_STRCASEEQ(_logger, alog::LOG_LEVEL_##level, s1, s2)
#define AUTIL_CHECK_STRCASENE(level, s1, s2) ALOG_CHECK_STRCASENE(_logger, alog::LOG_LEVEL_##level, s1, s2)

#define AUTIL_CHECK_INDEX(level, I, A) ALOG_CHECK_INDEX(_logger, alog::LOG_LEVEL_##level, I, A)
#define AUTIL_CHECK_BOUND(level, B, A) ALOG_CHECK_BOUND(_logger, alog::LOG_LEVEL_##level, B, A)
#define AUTIL_CHECK_DOUBLE_EQ(level, val1, val2) ALOG_CHECK_DOUBLE_EQ(_logger, alog::LOG_LEVEL_##level, val1, val2)
#define AUTIL_CHECK_NEAR(level, val1, val2, margin) ALOG_CHECK_NEAR(_logger, alog::LOG_LEVEL_##level, val1, val2, margin)

#define AUTIL_LOG_BINARY(level, msg) {                                  \
        if(__builtin_expect(_logger->isLevelEnabled(alog::LOG_LEVEL_##level), 0)) \
            _logger->logBinaryMessage(alog::LOG_LEVEL_##level, msg);    \
    }

#define AUTIL_DECLARE_AND_SETUP_LOGGER(n, c) static alog::Logger *_logger \
    = alog::Logger::getLogger(#n "." #c)
#define AUTIL_LOG_SHUTDOWN() alog::Logger::shutdown()
#define AUTIL_LOG_FLUSH() alog::Logger::flushAll()

#define AUTIL_CONDITION_LOG(condition, true_level, false_level, format, args...) \
    do {                                                                         \
        if (condition) {                                                         \
            ALOG_LOG(_logger, alog::LOG_LEVEL_##true_level, format, ##args)      \
        } else {                                                                 \
            ALOG_LOG(_logger, alog::LOG_LEVEL_##false_level, format, ##args)     \
        }                                                                        \
    } while (0);

#define AUTIL_INTERVAL_LOG(logInterval, level, format, args...)         \
    do {                                                                \
        static int logCounter;                                          \
        if (0 == logCounter) {                                          \
            AUTIL_LOG(level, format, ##args);                           \
            logCounter = logInterval;                                   \
        }                                                               \
        logCounter--;                                                   \
    } while (0)

#define AUTIL_INTERVAL_LOG2(logInterval, level, format, args...)        \
    do {                                                                \
        static int64_t logTimestamp;                                    \
        int64_t now = autil::TimeUtility::currentTimeInSeconds();       \
        if (now - logTimestamp > logInterval) {                         \
            AUTIL_LOG(level, format, ##args);                           \
            logTimestamp = now;                                         \
        }                                                               \
    } while (0)

#define AUTIL_MASSIVE_LOG(level, format, args...)                       \
    do {                                                                \
        static int64_t logCounter = 0;                                  \
        if ((logCounter >> 20) == 0 || (logCounter & 127) == 0) {            \
            AUTIL_LOG(level, format, ##args);                           \
        }                                                               \
        logCounter++;                                                   \
    } while (0)
