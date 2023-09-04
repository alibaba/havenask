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
#ifndef ILOGGER_H_
#define ILOGGER_H_

#include <string>

/**
 * Interface to define how log system is used in ANET/ARPC.
 */
class ILogger {
public:
    virtual ~ILogger() {}

    virtual void setLogLevel(const int level) = 0;
    virtual void setLogLevel(const char *level) = 0;
    virtual void logPureMessage(int level, const char *file, int line, const char *function, const char *buf) = 0;
    virtual void log(int level, const char *file, int line, const char *function, const char *fmt, ...)
        __attribute__((format(printf, 6, 7))) = 0;
    virtual bool isLevelEnabled(const int level) = 0;

    /* The interfaces below are for the purpose of search app compliance. */
    virtual void logSetup() = 0;
    virtual void logSetup(const std::string &configFile) = 0;
    virtual void logTearDown() = 0;
};

#endif
