#ifndef ISEARCH_TIMESTAMP_H
#define ISEARCH_TIMESTAMP_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <sys/time.h>

#ifndef CLOCKS_PER_SECOND
#define CLOCKS_PER_SECOND 1000000
#endif

BEGIN_HA3_NAMESPACE(util);

class TimeStamp
{
public:
    TimeStamp();
    TimeStamp(int64_t timeStamp);
    TimeStamp(int32_t second, int32_t us);
    ~TimeStamp();

public:  
    friend int64_t operator - (const TimeStamp &l,
                               const TimeStamp &r);
    friend bool operator == (const TimeStamp &l,
                             const TimeStamp &r);
    friend bool operator != (const TimeStamp &l,
                             const TimeStamp &r);
    friend std::ostream& operator << (std::ostream &out, 
            const TimeStamp &timeStamp);

    TimeStamp& operator = (const timeval &atime);
  
    void setCurrentTimeStamp();
    timeval getTime() {return _time;}
    std::string getFormatTime();

    static std::string getFormatTime(int64_t timeStamp);
private:
    timeval _time;

private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(util);

#endif //ISEARCH_TIMESTAMP_H
