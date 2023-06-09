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
#include <stdarg.h>
#include <strings.h>
#include <stddef.h>
#include <stdint.h>
#include <iostream>
#include <string>

#include "alog/Configurator.h"
#include "aios/network/anet/alogadapter.h"
#include "alog/Logger.h"

static const char *g_errstr[] = {"NO_LEVEL", "NO_LEVEL", "ERROR","WARN","INFO","DEBUG","SPAM"};

AlogAdapter::AlogAdapter(const std::string & loggerName) {
    logger = alog::Logger::getLogger(loggerName.c_str());
}
AlogAdapter::AlogAdapter(const char *loggerName) {
    logger = alog::Logger::getLogger(loggerName);
}

AlogAdapter::AlogAdapter() {
    logger = alog::Logger::getRootLogger();
}

AlogAdapter::~AlogAdapter() {}

void AlogAdapter::logSetupStatic() {
    alog::Configurator::configureRootLogger();
}

void AlogAdapter::logSetupStatic(const std::string &configFile) {
    try {
        alog::Configurator::configureLogger(configFile.c_str());
    } catch(std::exception &e) {
        std::cerr << "WARN! Failed to configure logger!"
                  << e.what() << ",use default log conf."
                  << std::endl;
        alog::Configurator::configureRootLogger();
    }
}

void AlogAdapter::logTearDownStatic() {
    alog::Logger::shutdown();
}

int AlogAdapter::getLogLevel(void) {
    int rc = logger->getLevel();
    if (rc < 0) rc = 0;
    return  rc;
}

void AlogAdapter::setLogLevel(const char *level) {
    if (level == NULL) return;
    int l = sizeof(g_errstr)/sizeof(char*);
    for (int i=0; i<l; i++) {
        if (strcasecmp(level, g_errstr[i]) == 0) {
            uint32_t tmp = i;
            if (tmp >= alog::LOG_LEVEL_COUNT) {
                tmp = alog::LOG_LEVEL_DEBUG;
            }
            logger->setLevel(tmp);
            break;
        }
    }
}

void AlogAdapter::setLogLevel(const int level) {
    logger->setLevel(level);
}

void AlogAdapter::logPureMessage(int level, const char *file, int line, const char *function, const char *buffer)
{
    if (__builtin_expect((!logger->isLevelEnabled(level)),1))
        return;

    logger->log(level, file, line, function, "%s", buffer);
}

void AlogAdapter::log(int level, const char *file, int line, const char *function, const char *fmt, ...) {
    if (__builtin_expect((!logger->isLevelEnabled(level)),1))
        return;

    va_list va;
    va_start(va, fmt);
    logger->logVaList(level, file, line, function, fmt, va);
    va_end(va);
}
