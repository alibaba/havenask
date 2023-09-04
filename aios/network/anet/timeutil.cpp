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
#include "aios/network/anet/timeutil.h"

#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
namespace anet {

int64_t TimeUtil::_now = 0;
const int64_t TimeUtil::MIN = (1ll << 63);
const int64_t TimeUtil::MAX = ~(1ll << 63);
const int64_t TimeUtil::PRE_MAX = ~(1ll << 63) - 1;

/*
 * 得到当前时间
 */
int64_t TimeUtil::getTime() {
    struct timeval t;
    gettimeofday(&t, NULL);
    return (static_cast<int64_t>(t.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(t.tv_usec));
}

/*
 * 设置当前时间
 */
void TimeUtil::setNow() { _now = getTime(); }

/*
 * buf should have at least 26 char in length.
 */
void TimeUtil::getTimeStr(int64_t tval, char *buf) {
    time_t t = tval / 1000000;
    ctime_r(&t, buf);
}

} // namespace anet
