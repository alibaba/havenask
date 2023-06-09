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
#ifndef ISEARCH_EXPRESSION_TIMESTAMP_H
#define ISEARCH_EXPRESSION_TIMESTAMP_H

#include <sys/time.h>
#include <stdint.h>
#include <iostream>

#ifndef CLOCKS_PER_SECOND
#define CLOCKS_PER_SECOND 1000000
#endif

namespace expression {

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
};

}

#endif //ISEARCH_EXPRESSION_TIMESTAMP_H
