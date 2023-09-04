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
#ifndef ANET_UTIL_H
#define ANET_UTIL_H

#include <stdint.h>
#include <stdlib.h>

#include "aios/network/anet/timeutil.h"

namespace anet {

inline int getRandInt(int max) {
    int seed = (int)TimeUtil::getTime();
    srand(seed);
    int rc = rand() % max;
    return rc;
}

inline uint32_t revertBit(uint32_t num, int pos) {
    uint32_t o = ~num;
    pos = pos % 32;
    int n = 1 << pos;

    return ((num & ~n) | (o & n));
}

} // namespace anet

#endif
