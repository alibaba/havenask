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
/**
*@file AlogDefine.h
*@brief the file to define some micros.
*
*@version 1.0.7
*@date 2010.01.17
*@author jinhui.li
*/
#ifndef _ALOG_ALOG_DEFINE_H_
#define _ALOG_ALOG_DEFINE_H_

#include <iostream>

#include "Logger.h"
#include "Configurator.h"

#define ALOG_CONFIG_ROOT_LOGGER() alog::Configurator::configureRootLogger()
#define ALOG_CONFIG_LOGGER(filename) do {                       \
        try {                                                   \
            alog::Configurator::configureLogger(filename);      \
        } catch(std::exception &e) {                            \
            std::cerr << "Error!!! Failed to configure logger!" \
                      << e.what() << std::endl;                 \
            exit(-1);                                           \
        }                                                       \
    }while(0)

#define ALOG_SET_ROOT_LOG_LEVEL(level)                                  \
    alog::Logger::getRootLogger()->setLevel(alog::LOG_LEVEL_##level)

#define ALOG_SETUP(n, c) alog::Logger *c::_logger  \
    = alog::Logger::getLogger(#n "." #c)
#define ALOG_SETUP_TEMPLATE(n, c, T) template <typename T> \
    alog::Logger *c<T>::_logger                            \
    = alog::Logger::getLogger(#n "." #c)

#define ALOG_DECLARE() static alog::Logger *_logger
#define ALOG_DECLARE_AND_SETUP_LOGGER(n, c) static alog::Logger *_logger \
    = alog::Logger::getLogger(#n "." #c)
#define ALOG_LOG_SHUTDOWN() alog::Logger::shutdown()
#define ALOG_LOG_FLUSH() alog::Logger::flushAll()


#endif

