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
#ifndef ALOG_ADAPTER_H_
#define ALOG_ADAPTER_H_
#include <string>

#include "aios/network/anet/ilogger.h"
#include "alog/Logger.h"

class AlogAdapter : public ILogger {
public:
    AlogAdapter();
    AlogAdapter(const std::string &loggerName);
    AlogAdapter(const char *loggerName);
    ~AlogAdapter();
    void log(int level, const char *file, int line, const char *function, const char *fmt, ...)
        __attribute__((format(printf, 6, 7)));
    void logPureMessage(int level, const char *file, int line, const char *function, const char *);
    void setLogLevel(const char *level);
    void setLogLevel(const int level);
    int getLogLevel();
    bool isLevelEnabled(const int level) { return logger->isLevelEnabled(level); }

    void logSetup() { AlogAdapter::logSetupStatic(); }
    void logSetup(const std::string &configFile) { AlogAdapter::logSetupStatic(configFile); }
    void logTearDown() { AlogAdapter::logTearDownStatic(); }
    static void logSetupStatic();
    static void logSetupStatic(const std::string &configFile);
    static void logTearDownStatic();

    alog::Logger *getAlogLogger(void) { return logger; }

private:
    alog::Logger *logger;
};

#endif
