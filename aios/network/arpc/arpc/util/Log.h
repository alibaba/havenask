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
#ifndef ARPC_LOG_H_
#define ARPC_LOG_H_

#include <iostream>

#include "aios/network/anet/alogadapter.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "alog/Configurator.h"
#include "alog/Logger.h"
#include "alog/Version.h"

#if !defined ALOG_VERSION || ALOG_VERSION < 010101
#error "From this version ARPC requires ALOG >= 1.1.1. Please check your ALOG."
#endif

ARPC_BEGIN_NAMESPACE(arpc);

extern ILogger *arpclogger;
inline ILogger *GetArpcLogger() { return arpclogger; }
inline void SetArpcLogger(ILogger *l) { arpclogger = l; }

#define ARPC_LOG_CONFIG(config) (arpc::arpclogger->logSetup(config))
#define ARPC_LOG_SHUTDOWN() (arpc::arpclogger->logTearDown())

/* ALOG_LOG will expand to logger->log(...), which complies with our ILogger
 * interface, so we just reuse ALOG_LOG for simplification. */
#define ARPC_LOG(level, format, args...) ALOG_LOG(arpc::arpclogger, alog::LOG_LEVEL_##level, format, ##args)
#define ARPC_DECLARE_AND_SETUP_LOGGER(c)

ARPC_END_NAMESPACE(arpc);

#endif
