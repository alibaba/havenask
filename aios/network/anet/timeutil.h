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
#ifndef ANET_TIMEUTIL_H_
#define ANET_TIMEUTIL_H_
#include <stdint.h>
namespace anet {

class TimeUtil {
public:
    /*
     * 得到当前时间
     */
    static int64_t getTime();

    /*
     * 设置当前时间到now中
     */
    static void setNow();

    /*
     * convert a time usec value to a date str.
     * buf should have at least 26 slot in size.
     */
    static void getTimeStr(int64_t tval, char *buf);

public:
    static int64_t _now;
    static const int64_t MIN;
    static const int64_t MAX;
    static const int64_t PRE_MAX;
};

} // namespace anet

#endif
