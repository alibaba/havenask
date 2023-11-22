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
#pragma once

#include "navi/common.h"
#include "autil/DataBuffer.h"
#include "autil/TimeUtility.h"
#include <unistd.h>
#include <stdint.h>
#include <stdio.h>
#include <string>
#include <sys/syscall.h>
#include <sys/time.h>
#include <google/protobuf/map.h>

namespace navi {

class LoggingEventDef;
class StringHashTable;
class SymbolTableDef;

class LoggingEvent
{
public:
    LoggingEvent(const std::string &name_, void *object_,
                 const std::string &prefix_, const char *msg_,
                 const LogLevel level_, const char *file_, int line_,
                 const char *func_);
    LoggingEvent();
public:
    void formatTime(char buffer[], int length) const;
    const char *levelStr() const;
public:
    static const char *levelStrByLevel(LogLevel level_);
public:
    void toProto(LoggingEventDef *eventDef,
                 StringHashTable *stringHashTable) const;
    void fromProto(const LoggingEventDef &eventDef,
                   const SymbolTableDef *symbolTableDef = nullptr);
private:
    static std::string getStrFromHash(
        ::google::protobuf::uint64 hash,
        const ::google::protobuf::Map<::google::protobuf::uint64, std::string>
            &strTableMap);
public:
    std::string name;
    void *object;
    std::string prefix;
    std::string message;
    std::string bt;
    LogLevel level;
    std::string file;
    int line;
    std::string func;
    int32_t pid;
    int64_t tid;
    struct timeval time;
};

NAVI_TYPEDEF_PTR(LoggingEvent);

}
