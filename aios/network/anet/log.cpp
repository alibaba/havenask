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
#include "aios/network/anet/log.h"

#include <string>

class ILogger;
#ifdef HAVE_ALOG
#include "aios/network/anet/alogadapter.h"
#endif

namespace anet {

#ifdef HAVE_ALOG
AlogAdapter alogAdapter(std::string("anet"));
ILogger *logger = &alogAdapter;

#else
static int g_level = 1;

Logger::Logger() {}

Logger::~Logger() {}

void Logger::logSetup() {}

void Logger::logSetup(const std::string &configFile) {}

void Logger::logTearDown() {}

void Logger::setLogLevel(const char *level) {
    if (level == NULL)
        return;
    int l = sizeof(g_errstr) / sizeof(char *);
    for (int i = 0; i < l; i++) {
        if (strcasecmp(level, g_errstr[i]) == 0) {
            g_level = i;
            break;
        }
    }
}

void Logger::setLogLevel(const int level) {
    if (level < 0) {
        g_level = 0;
    } else {
        g_level = level;
    }
}

void Logger::logMessage(int level, const char *file, int line, const char *function, const char *fmt, ...) {
    if (level > g_level)
        return;

    char buffer[1024];
    time_t t;
    time(&t);
    struct tm tm;
    ::localtime_r((const time_t *)&t, &tm);

    int size = snprintf(buffer,
                        1024,
                        "[%04d-%02d-%02d %02d:%02d:%02d] %-5s %s (%s:%d) %s\n",
                        tm.tm_year + 1900,
                        tm.tm_mon + 1,
                        tm.tm_mday,
                        tm.tm_hour,
                        tm.tm_min,
                        tm.tm_sec,
                        g_errstr[level],
                        function,
                        file,
                        line,
                        fmt);
    // 去掉过多的换行
    while (buffer[size - 2] == '\n')
        size--;
    buffer[size] = '\0';

    va_list args;
    va_start(args, fmt);
    vfprintf(stderr, buffer, args);
    fflush(stderr);
    va_end(args);
}

void Logger::rotateLog(const char *filename) {
    if (access(filename, R_OK) == 0) {
        char oldLogFile[256];
        time_t t;
        time(&t);
        struct tm tm;
        ::localtime_r((const time_t *)&t, &tm);
        sprintf(oldLogFile,
                "%s.%04d%02d%02d%02d%02d%02d",
                filename,
                tm.tm_year + 1900,
                tm.tm_mon + 1,
                tm.tm_mday,
                tm.tm_hour,
                tm.tm_min,
                tm.tm_sec);
        rename(filename, oldLogFile);
    }
    int fd = open(filename, O_RDWR | O_CREAT | O_APPEND, 0640);
    dup2(fd, 1);
    dup2(fd, 2);
    close(fd);
}
#endif
} // namespace anet

/////////////
