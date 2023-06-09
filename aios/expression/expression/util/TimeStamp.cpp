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
#include "expression/util/TimeStamp.h"
#include <iostream>
#include <sstream>

using namespace std;

namespace expression {

TimeStamp::TimeStamp() { 
    _time.tv_sec = _time.tv_usec = 0;
}

TimeStamp::TimeStamp(int64_t timeStamp) {
    _time.tv_sec = timeStamp / 1000 / 1000;
    _time.tv_usec = timeStamp - _time.tv_sec * 1000 * 1000;
}

TimeStamp::TimeStamp(int32_t second, int32_t us) {
    _time.tv_sec = second;
    _time.tv_usec = us;
}

TimeStamp::~TimeStamp() { 
}

// return ms
int64_t operator - (const TimeStamp& l,
		    const TimeStamp& r) {
    int64_t usecs = (l._time.tv_usec - r._time.tv_usec);

    return (int64_t)(l._time.tv_sec-r._time.tv_sec)*1000000 + usecs;
}

bool operator == (const TimeStamp &l,
                  const TimeStamp &r)
{
    return l._time.tv_sec == r._time.tv_sec
        && l._time.tv_usec == r._time.tv_usec;
}

bool operator != (const TimeStamp &l,
                  const TimeStamp &r)
{
    return l._time.tv_sec != r._time.tv_sec
        || l._time.tv_usec != r._time.tv_usec;
}

TimeStamp& TimeStamp::operator = (const timeval &atime) {
    _time.tv_sec = atime.tv_sec;
    _time.tv_usec = atime.tv_usec;
    return *this;
}

ostream& operator << (ostream& out, const TimeStamp& timeStamp) {
    out<< timeStamp._time.tv_sec << ":" << timeStamp._time.tv_usec;
    return out;
}

void TimeStamp::setCurrentTimeStamp() {
    gettimeofday(&_time,NULL);
}

string TimeStamp::getFormatTime() {
    time_t t = _time.tv_sec;
    tm tim;
    localtime_r(&t, &tim);
    char buf[128];
    snprintf(buf, 128,
             "%04d-%02d-%02d %02d:%02d:%02d.%03d",
             tim.tm_year + 1900,
             tim.tm_mon + 1,
             tim.tm_mday,
             tim.tm_hour,
             tim.tm_min,
             tim.tm_sec,
             (int)_time.tv_usec / 1000);
    return string(buf);
}

std::string TimeStamp::getFormatTime(int64_t timeStamp) {
    TimeStamp ts(timeStamp);
    return ts.getFormatTime();
}

}

