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
#ifndef CARBON_LOG_H_
#define CARBON_LOG_H_

#include "alog/Logger.h"
#include "alog/Configurator.h"
#include <iostream>

#define CARBON_DEFAULT_LOG_CONF "carbon_alog.conf"

#define CARBON_LOG_SETUP(n, c)  alog::Logger *c::_logger \
    = alog::Logger::getLogger("carbon."#n "." #c)

#define CARBON_ROOT_LOG_CONFIG() alog::Configurator::configureRootLogger()
#define CARBON_LOG_CONFIG(filename) do {                           \
        try {                                                   \
            alog::Configurator::configureLogger(filename);      \
        } catch(std::exception &e) {                            \
            std::cerr << "WARN! Failed to configure logger!"    \
                      << e.what() << ",use default log conf."   \
                      << std::endl;                             \
            CARBON_ROOT_LOG_CONFIG();                              \
        }                                                       \
    }while(0)

#define CARBON_LOG_CONFIG_FROM_STRING(content) do {                        \
        try {                                                           \
            alog::Configurator::configureLoggerFromString(content);     \
        } catch(std::exception &e) {                                    \
            std::cerr << "WARN! Failed to configure logger!"            \
                      << e.what() << ",use default log conf."           \
                      << std::endl;                                     \
            CARBON_ROOT_LOG_CONFIG();                                      \
        }                                                               \
    }while(0)

#define CARBON_ROOT_LOG_SETLEVEL(level)                                  \
    alog::Logger::getRootLogger()->setLevel(alog::LOG_LEVEL_##level)

#define CARBON_LOG_DECLARE() static alog::Logger *_logger

#define CARBON_ENSURE_STRING_LITERAL(x)  do { (void) x ""; } while(0)

#ifndef NDEBUG
#define CARBON_LOG(level, format, args...)                                 \
    do {                                                                \
        CARBON_ENSURE_STRING_LITERAL(format);                                  \
        ALOG_LOG(_logger, alog::LOG_LEVEL_##level, format, ##args);     \
    } while (0)
#else
#define CARBON_LOG(level, format, args...) ALOG_LOG(_logger, alog::LOG_LEVEL_##level, format, ##args)
#endif

#define CARBON_PREFIX_LOG(level, format, args...) \
    CARBON_LOG(level, "[%s] " format, LOG_PREFIX, ##args)

#define CARBON_DECLARE_AND_SETUP_LOGGER(c) static alog::Logger *_logger    \
    = alog::Logger::getLogger("carbon." #c)

#define CARBON_LOG_SHUTDOWN() alog::Logger::shutdown()

#define CARBON_LOG_FLUSH() alog::Logger::flushAll()

#endif
