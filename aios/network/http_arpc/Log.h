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
#ifndef HTTP_ARPC_LOG_H_
#define HTTP_ARPC_LOG_H_

#include "alog/Logger.h"
#include "alog/Configurator.h"
#include "aios/network/anet/alogadapter.h"

#if !defined ALOG_VERSION || ALOG_VERSION < 010101
#error "From this version HTTP_ARPC requires ALOG >= 1.1.1. Please check your ALOG." 
#endif

namespace http_arpc {

extern ILogger * httparpclogger;
inline ILogger * GetHttpArpcLogger() { return httparpclogger; }
inline void SetHttpArpcLogger(ILogger *l) { httparpclogger = l; }

#define HTTP_ARPC_LOG_CONFIG(config) (http_arpc::httparpclogger->logSetup(config))
#define HTTP_ARPC_LOG_SHUTDOWN() (http_arpc::httparpclogger->logTearDown()) 

/* ALOG_LOG will expand to logger->log(...), which complies with our ILogger
 * interface, so we just reuse ALOG_LOG for simplification. */
#define HTTP_ARPC_LOG(level, format, args...) ALOG_LOG(http_arpc::httparpclogger, alog::LOG_LEVEL_##level, format, ##args)

#define HTTP_ARPC_DECLARE_AND_SETUP_LOGGER(c) \
    static alog::Logger *_logger              \
    = alog::Logger::getLogger("HTTPARPC." #c)

}

#endif
