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
#ifndef FUTURE_LITE_LOG_H
#define FUTURE_LITE_LOG_H

#include "alog/Logger.h"
#include "alog/Configurator.h"
#include <iostream>

#define FL_ROOT_LOG_CONFIG() alog::Configurator::configureRootLogger()
#define FL_LOG_CONFIG(filename) do {                         \
        try {                                                   \
            alog::Configurator::configureLogger(filename);      \
        } catch(std::exception &e) {                            \
            std::cerr << "WARN! Failed to configure logger!"    \
                      << e.what() << ",use default log conf."   \
                      << std::endl;                             \
            FL_ROOT_LOG_CONFIG();                            \
        }                                                       \
    }while(0)

#define FL_LOG_CONFIG_FROM_STRING(content) do {                  \
        try {                                                       \
            alog::Configurator::configureLoggerFromString(content); \
        } catch(std::exception &e) {                                \
            std::cerr << "configure logger use ["                   \
                      << content<< "] failed"                       \
                      << std::endl;                                 \
            FL_ROOT_LOG_CONFIG();                                \
        }                                                           \
    }while(0)


#define FL_ROOT_LOG_SETLEVEL(level)                                  \
    alog::Logger::getRootLogger()->setLevel(alog::LOG_LEVEL_##level)

#define FL_LOG_DECLARE() static alog::Logger *_logger

#define FL_LOG_SETUP(n, c) alog::Logger *c::_logger  \
    = alog::Logger::getLogger(#n "." #c)

#define FL_LOG_SETUP_TEMPLATE(n, c, T) template <typename T> \
    alog::Logger *c<T>::_logger                                 \
    = alog::Logger::getLogger(#n "." #c)

#define FL_LOG_SETUP_TEMPLATE_2(n, c, T1, T2) template <typename T1, typename T2> \
    alog::Logger *c<T1,T2>::_logger                                     \
    = alog::Logger::getLogger(#n "." #c)

#define FL_LOG(level, format, args...) ALOG_LOG(_logger, alog::LOG_LEVEL_##level, format, ##args)

#define FL_LOG_BINARY(level, msg) {                                  \
        if(__builtin_expect(_logger->isLevelEnabled(alog::LOG_LEVEL_##level), 0)) \
            _logger->logBinaryMessage(alog::LOG_LEVEL_##level, msg);    \
    }

#define FL_DECLARE_AND_SETUP_LOGGER(n, c) static alog::Logger *_logger \
    = alog::Logger::getLogger(#n "." #c)
#define FL_LOG_SHUTDOWN() alog::Logger::shutdown()
#define FL_LOG_FLUSH() alog::Logger::flushAll()

#endif //FL_LOG_H_
