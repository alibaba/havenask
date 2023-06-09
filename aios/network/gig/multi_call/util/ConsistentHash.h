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
#ifndef ISEARCH_MULTI_CALL_CONSISTENTHASH_H
#define ISEARCH_MULTI_CALL_CONSISTENTHASH_H

#include "aios/network/gig/multi_call/common/common.h"

namespace multi_call {
struct HashNode {
    HashNode() : key(0), offset(0), label(0) {}
    uint64_t key;
    uint16_t offset;
    uint8_t label;
    bool operator<(const HashNode &rha) const;
} __attribute__((packed));

class ConsistentHash {
public:
    typedef HashNode *Iterator;

public:
    ConsistentHash();
    ~ConsistentHash();

private:
    ConsistentHash(const ConsistentHash &);
    ConsistentHash &operator=(const ConsistentHash &);

public:
    void init(size_t rowCount, size_t virtualNodeCount);
    void add(size_t offset, uint64_t key);
    // you should call construct before use!
    void construct();

    Iterator get(uint64_t key) const;
    Iterator begin() const;
    Iterator end() const;

private:
    size_t _virtualNodeCount;
    size_t _hashCircleSize;
    HashNode *_hashCircle;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_CONSISTENTHASH_H
