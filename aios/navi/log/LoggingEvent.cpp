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
#include "navi/engine/StringHashTable.h"
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

void LoggingEvent::toProto(LoggingEventDef *eventDef,
                           StringHashTable *stringHashTable) const
{
    if (stringHashTable) {
        auto hashValue = eventDef->mutable_hash();
        hashValue->set_prefix(stringHashTable->getHash(prefix));
        hashValue->set_file(stringHashTable->getHash(file));
        hashValue->set_func(stringHashTable->getHash(func));
        hashValue->set_message(stringHashTable->getHash(message));
        hashValue->set_bt(stringHashTable->getHash(bt));
    } else {
        auto strValue = eventDef->mutable_str();
        strValue->set_name(name);
        strValue->set_prefix(prefix);
        strValue->set_file(file);
        strValue->set_func(func);
        strValue->set_message(message);
        strValue->set_bt(bt);
    }
    eventDef->set_object((uint64_t)object);
    eventDef->set_level(level);
    eventDef->set_line(line);
    eventDef->set_pid(pid);
    eventDef->set_tid(tid);
    eventDef->set_time_sec(time.tv_sec);
    eventDef->set_time_usec(time.tv_usec);
}

std::string LoggingEvent::getStrFromHash(
    ::google::protobuf::uint64 hash,
    const ::google::protobuf::Map<::google::protobuf::uint64, std::string>
        &strTableMap)
{
    auto it = strTableMap.find(hash);
    if (strTableMap.end() == it) {
        return std::string();
    } else {
        return it->second;
    }
}

void LoggingEvent::fromProto(const LoggingEventDef &eventDef,
                             const SymbolTableDef *symbolTableDef)
{
    if (eventDef.has_str()) {
        const auto &strValue = eventDef.str();
        name = strValue.name();
        prefix = strValue.prefix();
        file = strValue.file();
        func = strValue.func();
        message = strValue.message();
        bt = strValue.bt();
    } else if (eventDef.has_hash() && symbolTableDef) {
        const auto &strTableMap = symbolTableDef->table();
        const auto &hashValue = eventDef.hash();
        prefix = getStrFromHash(hashValue.prefix(), strTableMap);
        file = getStrFromHash(hashValue.file(), strTableMap);
        func = getStrFromHash(hashValue.func(), strTableMap);
        message = getStrFromHash(hashValue.message(), strTableMap);
        bt = getStrFromHash(hashValue.bt(), strTableMap);
    }
    object = (void *)eventDef.object();
    level = (LogLevel)eventDef.level();
    line = eventDef.line();
    pid = eventDef.pid();
    tid = eventDef.tid();
    time.tv_sec = eventDef.time_sec();
    time.tv_usec = eventDef.time_usec();
}
}
