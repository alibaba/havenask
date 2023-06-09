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
#include "aios/network/gig/multi_call/util/RandomHash.h"
#include <algorithm>
#include <assert.h>

using namespace std;

namespace multi_call {

int32_t RandomHash::get(uint64_t key, const vector<RandomHashNode> &weights) {
    assert(!weights.empty());
    auto back = weights.back();
    assert(back.weightSum > 0);
    uint32_t r = key % back.weightSum;
    auto iter = std::upper_bound(weights.begin(), weights.end(), r);
    return iter->index;
}

} // namespace multi_call
