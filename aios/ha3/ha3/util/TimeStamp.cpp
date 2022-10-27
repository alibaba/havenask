#include <ha3/util/TimeStamp.h>
#include <iostream>
#include <sstream>
#include <stdio.h>

using namespace std;
BEGIN_HA3_NAMESPACE(util);
 HA3_LOG_SETUP(util, TimeStamp);

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

END_HA3_NAMESPACE(util);

