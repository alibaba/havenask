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
#include "navi/log/LoggingEvent.h"
#include "autil/DataBuffer.h"
#include <list>

namespace navi {

class GraphTraceDef;
class StringHashTable;

class TraceCollector
{
public:
    TraceCollector();
    ~TraceCollector();
private:
    TraceCollector(const TraceCollector &);
    TraceCollector &operator=(const TraceCollector &);
public:
    void setFormatPattern(const std::string &pattern);
    void append(LoggingEvent &event);
    void append(LoggingEvent &&event);
    void merge(TraceCollector &other);
    void format(std::vector<std::string> &traceVec) const;
    void toProto(GraphTraceDef *traceDef, StringHashTable *stringHashTable);
    void fromProto(const GraphTraceDef &traceDef,
                   const SymbolTableDef &symbolTableDef);
    bool empty() const;
    size_t eventCount() const;
private:
    std::string _formatPattern;
    std::list<LoggingEvent> _eventList;
};

}
