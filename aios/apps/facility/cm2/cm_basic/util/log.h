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

/*********************************************************************
 * $Author: liang.xial $
 *
 * $LastChangedBy: liang.xial $
 *
 * $Revision: 2577 $
 *
 * $LastChangedDate: 2011-03-09 09:50:55 +0800 (Wed, 09 Mar 2011) $
 *
 * $Id: Log.h 2577 2011-03-09 01:50:55Z liang.xial $
 *
 * $Brief: ini file parser $
 ********************************************************************/

#ifndef __CM_BASIC_UTIL_LOG_H_
#define __CM_BASIC_UTIL_LOG_H_

#include <cstring>

#include "alog/Configurator.h"
#include "alog/Logger.h"
#ifndef SAP_LOG_SETUP
#define SAP_LOG_SETUP(conf_path)                                                                                       \
    do {                                                                                                               \
        if (strlen(conf_path) > 0) {                                                                                   \
            try {                                                                                                      \
                alog::Configurator::configureLogger(conf_path);                                                        \
            } catch (std::exception & e) {                                                                             \
                fprintf(stderr, "config from '%s' failed, using default root.\n", conf_path);                          \
                alog::Configurator::configureRootLogger();                                                             \
            }                                                                                                          \
        } else {                                                                                                       \
            alog::Configurator::configureRootLogger();                                                                 \
        }                                                                                                              \
    } while (0)
#endif

namespace cm_basic {
extern const char* CM_BASIC_ACCESS_LOG;
extern const char* CM_BASIC_LOG;
extern const char* CM_BASIC_SUBSCRIBER_LOG;
extern const char* CM_BASIC_NODE_STATUS_CHANGE_LOG;
extern const char* CM_BASIC_SUBSET_CHANGE_LOG;
} // namespace cm_basic

#ifndef TLOG
#define TLOG(fmt, args...)                                                                                             \
    alog::Logger::getLogger(cm_basic::CM_BASIC_LOG)                                                                    \
        ->log(alog::LOG_LEVEL_INFO, __FILE__, __LINE__, __func__, fmt, ##args);                                        \
    alog::Logger::getLogger(cm_basic::CM_BASIC_LOG)->flush()
#endif

#ifndef TNOTE
#define TNOTE(fmt, args...)                                                                                            \
    alog::Logger::getLogger(cm_basic::CM_BASIC_LOG)                                                                    \
        ->log(alog::LOG_LEVEL_INFO, __FILE__, __LINE__, __func__, fmt, ##args);                                        \
    alog::Logger::getLogger(cm_basic::CM_BASIC_LOG)->flush()
#endif

#ifndef TERR
#define TERR(fmt, args...)                                                                                             \
    alog::Logger::getLogger(cm_basic::CM_BASIC_LOG)                                                                    \
        ->log(alog::LOG_LEVEL_ERROR, __FILE__, __LINE__, __func__, fmt, ##args)
#endif

#ifndef TWARN
#define TWARN(fmt, args...)                                                                                            \
    alog::Logger::getLogger(cm_basic::CM_BASIC_LOG)                                                                    \
        ->log(alog::LOG_LEVEL_WARN, __FILE__, __LINE__, __func__, fmt, ##args)
#endif

#ifndef TACCESS
#define TACCESS(fmt, args...)                                                                                          \
    alog::Logger::getLogger(cm_basic::CM_BASIC_ACCESS_LOG)->log(alog::LOG_LEVEL_INFO, fmt, ##args)
#endif

#ifndef TACCESS_NOISY
#define TACCESS_NOISY(fmt, args...)                                                                                    \
    alog::Logger::getLogger(cm_basic::CM_BASIC_ACCESS_LOG)->log(alog::LOG_LEVEL_TRACE1, fmt, ##args)
#endif

#ifndef TDEBUG
#define TDEBUG(fmt, args...)                                                                                           \
    alog::Logger::getLogger(cm_basic::CM_BASIC_LOG)                                                                    \
        ->log(alog::LOG_LEVEL_DEBUG, __FILE__, __LINE__, __func__, fmt, ##args)
#endif

#endif //_UTIL_LOG_H_
