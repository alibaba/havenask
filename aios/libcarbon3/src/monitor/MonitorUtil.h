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
#ifndef CARBON_MONITORUTIL_H
#define CARBON_MONITORUTIL_H

#include "common/common.h"
#include "monitor/CarbonMonitor.h"
#include "autil/TimeUtility.h"

#define REPORT_USED_TIME(metricName, tags)                          \
    carbon::monitor::ReportUsedTime                                     \
    __reportUsedTime(metricName, tags)

BEGIN_CARBON_NAMESPACE(monitor);

class ReportUsedTime
{
public:
    ReportUsedTime(const std::string& metricName, const TagMap& tags)
        : _tags(tags), _metricName(metricName)
    {
        _beginTime = autil::TimeUtility::currentTime();
        _endTime = -1;
    }
    ~ReportUsedTime() {
        _endTime = autil::TimeUtility::currentTime();
        double usedTimeMS = (double)(_endTime - _beginTime)/1000.0;
        MONITOR_REPORT_TAG(_metricName.c_str(), usedTimeMS, _tags);
    }
private:
    ReportUsedTime(const ReportUsedTime &);
    ReportUsedTime& operator=(const ReportUsedTime &);
public:
    TagMap _tags;
    std::string _metricName;
    int64_t _beginTime;
    int64_t _endTime;
};

END_CARBON_NAMESPACE(monitor);

#endif //CARBON_MONITORUTIL_H
