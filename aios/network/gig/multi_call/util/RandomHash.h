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
#ifndef ISEARCH_MULTI_CALL_RANDOMHASH_H
#define ISEARCH_MULTI_CALL_RANDOMHASH_H

#include "aios/network/gig/multi_call/common/common.h"

namespace multi_call {

struct RandomHashNode {
public:
    RandomHashNode(uint32_t weightSum_, uint32_t index_) : weightSum(weightSum_), index(index_) {
    }
    ~RandomHashNode() {
    }
    uint32_t weightSum;
    uint32_t index;
};

inline bool operator<(uint32_t key, const RandomHashNode &rhs) {
    return key < rhs.weightSum;
}

class RandomHash
{
public:
    RandomHash() {
    }
    ~RandomHash() {
    }

private:
    RandomHash(const RandomHash &);
    RandomHash &operator=(const RandomHash &);

public:
    static int32_t get(uint64_t key, const std::vector<RandomHashNode> &weights);
};

} // namespace multi_call

#endif
