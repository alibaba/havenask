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
*@file LoggingEvent.h
*@brief the file to declare LoggingEvent struct.
*
*@version 1.0.4
*@date 2009.03.05
*@author bingbing.yang
*/
#ifndef _ALOG_LOGGINGEVENT_H_
#define _ALOG_LOGGINGEVENT_H_

#include <unistd.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <stdlib.h>
#include <syscall.h>
#include <string>
#include <iosfwd>

namespace alog
{
static const char* s_gLevelName[] = {"DISABLE", "FATAL", "ERROR", "WARN",  "INFO", "DEBUG", "TRACE1", "TRACE2", "TRACE3", "NOTSET"};
static constexpr std::size_t s_gFmtLength = 27;
static constexpr std::size_t s_gFmtZoneLength = 33;

extern int32_t sysPid;
extern thread_local int64_t sysTid;

struct LoggingEvent
{
public:
    /**
    *@brief to get formatted current time stored in param cur as a return.
    *
    *@param cur formatted time as a return value.
    *@param length the cur buffer's length
    */
    void getCurTime(char cur[], int length, char zoneTime[], int zoneTimeLength)
    {
        struct timeval t;
        gettimeofday(&t, NULL);
        struct tm tim;
        ::localtime_r(&t.tv_sec, &tim);
        snprintf(cur, length, "%04d-%02d-%02d %02d:%02d:%02d.%06ld",
                 tim.tm_year + 1900, tim.tm_mon + 1, tim.tm_mday,
                 tim.tm_hour, tim.tm_min, tim.tm_sec, t.tv_usec);
        strcpy(zoneTime, cur);
        char *p = zoneTime + 26;
        strftime(p, zoneTimeLength - 26, ":%z", &tim);
    }

    /**
    *@brief constructor.
    *
    *@param name logger name.
    *@param msg user's logger message.
    *@param level logging level.
    */
    LoggingEvent(const std::string& name, const std::string& msg, const uint32_t level, const std::string& file = "", const int& line = 0, const std::string& func = "") :
        loggerName(name), message(msg), level(level),levelStr(s_gLevelName[level]),
        file(file), line(line), function(func), pid(sysPid), tid(sysTid)
    {
        getCurTime(loggingTime, s_gFmtLength, loggingZoneTime, s_gFmtZoneLength);
    }
    /** The logger name. */
    std::string loggerName;
    /** The application supplied message of logging event. */
    const std::string message;
    /** Level of logging event. */
    const uint32_t level;
    /** Level string of logging event. */
    const std::string levelStr;
    /** File name. */
    const std::string file;
    /** Line count. */
    const int line;
    /** Function name. */
    const std::string function;
    /** Process id */
    const int pid;
    /** Thread id */
    const long tid;
    /** Time yyyy-mm-dd hh:mm:ss:ms*/
    char loggingTime[s_gFmtLength];
    /** Time yyyy-mm-dd hh:mm:ss:ms*/
    char loggingZoneTime[s_gFmtZoneLength];
};
}
#endif
