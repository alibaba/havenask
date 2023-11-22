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
#ifndef NAVI_TRACEAPPENDER_H
#define NAVI_TRACEAPPENDER_H

#include "autil/Lock.h"
#include "navi/log/AlogAppender.h"
#include "navi/log/LogBtFilter.h"
#include "navi/log/TraceCollector.h"

namespace navi {

class NaviLogManager;

class TraceAppender : public Appender
{
public:
    TraceAppender(const std::string &levelStr);
    TraceAppender(const TraceAppender &other);
    ~TraceAppender();
private:
    TraceAppender &operator=(const TraceAppender &);
public:
    void addBtFilter(const LogBtFilterParam &param);
    void setLogManager(NaviLogManager *logManager);
    int append(LoggingEvent &event) override;
    void flush() override {
    }
    void collectTrace(TraceCollector &collector);
private:
    bool logBt(const LoggingEvent &event);
private:
    autil::ThreadMutex _lock;
    TraceCollector _traceCollector;
    LogBtFilter _btFilter;
};

}

#endif //NAVI_TRACEAPPENDER_H
