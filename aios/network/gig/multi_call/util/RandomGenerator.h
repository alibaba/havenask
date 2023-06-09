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
#ifndef ISEARCH_MULTI_CALL_RANDOMGENERATOR_H
#define ISEARCH_MULTI_CALL_RANDOMGENERATOR_H

#include "aios/network/gig/multi_call/common/common.h"
#if !__aarch64__
#include <immintrin.h>
#endif

namespace multi_call {

class RandomGenerator {
public:
    RandomGenerator(uint64_t randomSeed) : _randomSeed(randomSeed) {
        _supportHWRandom = supportRdRand();
    }
    ~RandomGenerator() {}

private:
    RandomGenerator(const RandomGenerator &);
    RandomGenerator &operator=(const RandomGenerator &);

public:
    void init(uint64_t randomSeed) { _randomSeed = randomSeed; }
    uint64_t get();

public:
    static void setUseHDRandom(bool value) { _useHWRandom = value; }
    static bool useHDRandom() { return _supportHWRandom && _useHWRandom; }

private:
    static bool supportRdRand();

private:
    volatile uint64_t _randomSeed;

private:
    static bool _supportHWRandom;
    static bool _useHWRandom;

private:
    static const int32_t RETRY_COUNT;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(RandomGenerator);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_RANDOMGENERATOR_H
