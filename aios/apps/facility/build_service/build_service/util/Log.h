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

#include "alog/Configurator.h"
#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "build_service/util/ErrorLogCollector.h"

#define BS_ALOG_CONF_FILE "bs_alog.conf"

#define BS_LOG_SETUP(n, c) alog::Logger* c::_logger = alog::Logger::getLogger("BS." #n "." #c)

#define BS_LOG_SETUP_TEMPLATE(n, c, T)                                                                                 \
    template <typename T>                                                                                              \
    alog::Logger* c<T>::_logger = alog::Logger::getLogger("BS." #n "." #c)

#define BS_LOG_SETUP_TEMPLATE_2(n, c, T1, T2)                                                                          \
    template <typename T1, typename T2>                                                                                \
    alog::Logger* c<T1, T2>::_logger = alog::Logger::getLogger("BS." #n "." #c)

#define BS_LOG_SETUP_TEMPLATE_3(n, c, T1, T2, T3)                                                                      \
    template <typename T1, typename T2, typename T3>                                                                   \
    alog::Logger* c<T1, T2, T3>::_logger = alog::Logger::getLogger("BS." #n "." #c)

#define BS_ROOT_LOG_CONFIG() alog::Configurator::configureRootLogger()
#define BS_LOG_CONFIG(filename)                                                                                        \
    do {                                                                                                               \
        try {                                                                                                          \
            alog::Configurator::configureLogger(filename);                                                             \
        } catch (std::exception & e) {                                                                                 \
            std::cerr << "WARN! Failed to configure logger!" << e.what() << ",use default log conf." << std::endl;     \
            BS_ROOT_LOG_CONFIG();                                                                                      \
        }                                                                                                              \
    } while (0)

#define BS_LOG_CONFIG_FROM_STRING(content)                                                                             \
    do {                                                                                                               \
        try {                                                                                                          \
            alog::Configurator::configureLoggerFromString(content);                                                    \
        } catch (std::exception & e) {                                                                                 \
            std::cerr << "configure logger use [" << content << "] failed" << std::endl;                               \
            BS_ROOT_LOG_CONFIG();                                                                                      \
        }                                                                                                              \
    } while (0)

#define BS_ROOT_LOG_SETLEVEL(level) alog::Logger::getRootLogger()->setLevel(alog::LOG_LEVEL_##level)

#define BS_LOG_DECLARE() static alog::Logger* _logger

#define BS_ENSURE_STRING_LITERAL(x)                                                                                    \
    do {                                                                                                               \
        (void)x "";                                                                                                    \
    } while (0)

#define ADAPTIVE_COLLECT_ERROR_LOG(level, format, args...)                                                             \
    if (alog::LOG_LEVEL_##level <= alog::LOG_LEVEL_WARN) {                                                             \
        ERROR_COLLECTOR_LOG(level, format, ##args);                                                                    \
    }

#ifndef NDEBUG
#define BS_LOG(level, format, args...)                                                                                 \
    do {                                                                                                               \
        BS_ENSURE_STRING_LITERAL(format);                                                                              \
        ALOG_LOG(_logger, alog::LOG_LEVEL_##level, format, ##args);                                                    \
        ADAPTIVE_COLLECT_ERROR_LOG(level, format, ##args)                                                              \
    } while (0)
#else
#define BS_LOG(level, format, args...)                                                                                 \
    ALOG_LOG(_logger, alog::LOG_LEVEL_##level, format, ##args);                                                        \
    ADAPTIVE_COLLECT_ERROR_LOG(level, format, ##args)
#endif

#define BS_PREFIX_LOG(level, format, args...) BS_LOG(level, "[%s] " format, LOG_PREFIX, ##args)

#define BS_DECLARE_AND_SETUP_LOGGER(c) static alog::Logger* _logger = alog::Logger::getLogger("BS." #c)

#define BS_LOG_SHUTDOWN() alog::Logger::shutdown()

#define BS_LOG_FLUSH() alog::Logger::flushAll()

#define BS_INTERVAL_LOG(logInterval, level, format, args...)                                                           \
    do {                                                                                                               \
        static int logCounter;                                                                                         \
        if (0 == logCounter) {                                                                                         \
            BS_LOG(level, format, ##args);                                                                             \
            logCounter = logInterval;                                                                                  \
        }                                                                                                              \
        logCounter--;                                                                                                  \
    } while (0)

#define BS_INTERVAL_LOG2(logInterval, level, format, args...)                                                          \
    do {                                                                                                               \
        static int64_t logTimestamp;                                                                                   \
        int64_t now = autil::TimeUtility::currentTimeInSeconds();                                                      \
        if (now - logTimestamp > logInterval) {                                                                        \
            BS_LOG(level, format, ##args);                                                                             \
            logTimestamp = now;                                                                                        \
        }                                                                                                              \
    } while (0)
