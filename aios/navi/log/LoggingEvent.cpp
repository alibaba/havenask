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
#include "navi/log/LoggingEvent.h"
#include "navi/proto/GraphVis.pb.h"

namespace navi {

static const char *LEVEL_NAME_ARRAY[] = { "DISABLE",   "FATAL",    "ERROR",
                                          "WARN",      "INFO",     "DEBUG",
                                          "TRACE1",    "TRACE2",   "TRACE3",
                                          "SCHEDULE1", "SCHEDULE2", "SCHEDULE3",
                                          "NOTSET" };

static int32_t sysPid = getpid();
static thread_local int64_t sysTid = (long)syscall(SYS_gettid);

LoggingEvent::LoggingEvent(const std::string &name_,
                           void *object_,
                           const std::string &prefix_,
                           const char *msg_,
                           const LogLevel level_,
                           const char *file_,
                           int line_,
                           const char *func_)
    : name(name_)
    , object(object_)
    , prefix(prefix_)
    , message(msg_)
    , level(level_)
    , file(file_)
    , line(line_)
    , func(func_)
    , pid(sysPid)
    , tid(sysTid)
{
    gettimeofday(&time, NULL);
}

LoggingEvent::LoggingEvent()
    : object(nullptr)
    , level(LOG_LEVEL_DISABLE)
    , line(0)
    , pid(0)
    , tid(0)
{
    time.tv_sec = 0;
    time.tv_usec = 0;
}

void LoggingEvent::formatTime(char buffer[], int length) const {
    struct tm tim;
    ::localtime_r(&time.tv_sec, &tim);
    snprintf(buffer, length, "%04d-%02d-%02d %02d:%02d:%02d.%06ld",
             tim.tm_year + 1900, tim.tm_mon + 1, tim.tm_mday, tim.tm_hour,
             tim.tm_min, tim.tm_sec, time.tv_usec);
}

const char *LoggingEvent::levelStr() const {
    return LEVEL_NAME_ARRAY[level];
}

const char *LoggingEvent::levelStrByLevel(LogLevel level_) {
    return LEVEL_NAME_ARRAY[level_];
}

void LoggingEvent::fillProto(LoggingEventDef *eventDef) const {
    eventDef->set_name(name);
    eventDef->set_object((uint64_t)object);
    eventDef->set_prefix(prefix);
    eventDef->set_message(message);
    eventDef->set_bt(bt);
    eventDef->set_level(level);
    eventDef->set_file(file);
    eventDef->set_line(line);
    eventDef->set_func(func);
    eventDef->set_pid(pid);
    eventDef->set_tid(tid);
    eventDef->set_time_sec(time.tv_sec);
    eventDef->set_time_usec(time.tv_usec);
}

void LoggingEvent::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(name);
    dataBuffer.write((uint64_t)object);
    dataBuffer.write(prefix);
    dataBuffer.write(message);
    dataBuffer.write(bt);
    dataBuffer.write((int32_t)level);
    dataBuffer.write(file);
    dataBuffer.write(line);
    dataBuffer.write(func);
    dataBuffer.write(pid);
    dataBuffer.write(tid);
    dataBuffer.write((int64_t)time.tv_sec);
    dataBuffer.write((int64_t)time.tv_usec);
}

void LoggingEvent::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(name);
    uint64_t objectInt = 0;
    dataBuffer.read(objectInt);
    object = (void *)objectInt;
    dataBuffer.read(prefix);
    dataBuffer.read(message);
    dataBuffer.read(bt);

    int32_t levelInt = 0;
    dataBuffer.read(levelInt);
    level = (LogLevel)levelInt;

    dataBuffer.read(file);
    dataBuffer.read(line);
    dataBuffer.read(func);
    dataBuffer.read(pid);
    dataBuffer.read(tid);

    int64_t sec = 0;
    int64_t usec = 0;
    dataBuffer.read(sec);
    dataBuffer.read(usec);
    time.tv_sec = sec;
    time.tv_usec = usec;
}

}
