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
#ifndef HIPPO_LOG_H_
#define HIPPO_LOG_H_

#include "alog/Logger.h"
#include "alog/Configurator.h"
#include <iostream>


#define ALOG_CONF_FILE "hippo_alog.conf"

#define HIPPO_LOG_SETUP(n, c)  alog::Logger *c::_logger \
    = alog::Logger::getLogger("hippo."#n "." #c)

#define HIPPO_LOG_SETUP_TEMPLATE(n, c, T) template <typename T>   \
    alog::Logger *c<T>::_logger                                 \
    = alog::Logger::getLogger("hippo."#n "." #c)

#define HIPPO_LOG_SETUP_TEMPLATE_2(n,c, T1, T2) template <typename T1, typename T2> \
    alog::Logger *c<T1, T2>::_logger                           \
    = alog::Logger::getLogger("hippo." #n "."  #c)


#define TIMELINE_LOG_SETUP(n, c) alog::Logger *c::_timelineAlogLogger \
    = alog::Logger::getLogger("timeline."#n "." #c)

#define TIMELINE_LOG_DECLARE() static alog::Logger *_timelineAlogLogger

#define HIPPO_ROOT_LOG_CONFIG() alog::Configurator::configureRootLogger()
#define HIPPO_LOG_CONFIG(filename) do {                           \
        try {                                                   \
            alog::Configurator::configureLogger(filename);      \
        } catch(std::exception &e) {                            \
            std::cerr << "WARN! Failed to configure logger!"    \
                      << e.what() << ",use default log conf."   \
                      << std::endl;                             \
            HIPPO_ROOT_LOG_CONFIG();                              \
        }                                                       \
    }while(0)

#define HIPPO_LOG_CONFIG_FROM_STRING(content) do {                        \
        try {                                                           \
            alog::Configurator::configureLoggerFromString(content);     \
        } catch(std::exception &e) {                                    \
            std::cerr << "WARN! Failed to configure logger!"            \
                      << e.what() << ",use default log conf."           \
                      << std::endl;                                     \
            HIPPO_ROOT_LOG_CONFIG();                                      \
        }                                                               \
    }while(0)

#define HIPPO_ROOT_LOG_SETLEVEL(level)                                  \
    alog::Logger::getRootLogger()->setLevel(alog::LOG_LEVEL_##level)

#define HIPPO_LOG_DECLARE() static alog::Logger *_logger

#define HIPPO_ENSURE_STRING_LITERAL(x)  do { (void) x ""; } while(0)

#ifndef NDEBUG
#define HIPPO_LOG(level, format, args...)                                 \
    do {                                                                \
        HIPPO_ENSURE_STRING_LITERAL(format);                                  \
        ALOG_LOG(_logger, alog::LOG_LEVEL_##level, format, ##args);     \
    } while (0)
#else
#define HIPPO_LOG(level, format, args...) ALOG_LOG(_logger, alog::LOG_LEVEL_##level, format, ##args)
#endif

#define HIPPO_DECLARE_AND_SETUP_LOGGER(c) static alog::Logger *_logger    \
    = alog::Logger::getLogger("hippo." #c)

#define HIPPO_LOG_SHUTDOWN() alog::Logger::shutdown()

#define HIPPO_LOG_FLUSH() alog::Logger::flushAll()

#define HIPPO_LOG_LEVEL_ENABLE(level) _logger->isLevelEnabled(alog::LOG_LEVEL_##level)

#endif
