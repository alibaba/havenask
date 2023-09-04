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
#include "aios/network/gig/multi_call/util/RandomGenerator.h"

#include "autil/TimeUtility.h"
#if !__aarch64__
#include <cpuid.h>
#endif

namespace multi_call {

AUTIL_LOG_SETUP(multi_call, RandomGenerator);
bool RandomGenerator::_supportHWRandom = false;
bool RandomGenerator::_useHWRandom = true;
const int32_t RandomGenerator::RETRY_COUNT = 20;

bool RandomGenerator::supportRdRand() {
#if !__aarch64__
    unsigned int eax = 0;
    unsigned int ebx = 0;
    unsigned int ecx = 0;
    unsigned int edx = 0;
    if (__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
        return ((ecx & bit_RDRND) == bit_RDRND);
    }
#endif
    return false;
}

uint64_t RandomGenerator::get() {
    auto tmp = _randomSeed;
#if !__aarch64__
    if (likely(useHDRandom())) {
        unsigned long long randomNum;
        for (int32_t loop = 0; loop < RETRY_COUNT; loop++) {
            if (_rdrand64_step(&randomNum)) {
                _randomSeed = randomNum;
                return tmp;
            }
        }
        AUTIL_LOG(WARN, "hardware rand failed");
    }
#endif
    // use soft random
    _randomSeed = _randomSeed * 1103515245 + 12345;
    return tmp;
}

} // namespace multi_call
