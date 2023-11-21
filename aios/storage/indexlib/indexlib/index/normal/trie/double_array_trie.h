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
#pragma once

#include <memory>

#include "autil/ConstString.h"
#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class DoubleArrayTrie
{
public:
    typedef std::pair<autil::StringView, int32_t> KVPair;
    typedef std::vector<KVPair, autil::mem_pool::pool_allocator<KVPair>> KVPairVector;

private:
    struct Node {
        Node() : left(0), right(0), code(0), depth(0) {}
        size_t left;
        size_t right;
        uint32_t code;
        uint32_t depth;
    };
    typedef std::vector<Node> NodeVector;

    struct State {
        State() : base(0), check(0) {}
        int32_t base;
        uint32_t check; // ATTENTION: 4G limit
    };
    typedef std::vector<State, autil::mem_pool::pool_allocator<State>> StateArray;
    static const uint32_t INIT_SIZE = 8192;

public:
    DoubleArrayTrie(autil::mem_pool::PoolBase* pool);
    ~DoubleArrayTrie();

public:
    // for build
    bool Build(const KVPairVector& sortedKVPairVector);
    autil::StringView Data() const;
    float CompressionRatio() const;
    static size_t EstimateMemoryUse(size_t keyCount);
    static size_t EstimateMemoryUseFactor();

    // for search
    static int32_t Match(const void* data, const autil::StringView& key);
    static size_t PrefixSearch(const void* data, const autil::StringView& key, KVPairVector& results,
                               int32_t maxMatches = -1);
    static size_t GetFirstReserveSize() { return INIT_SIZE * sizeof(State); }

private:
    void FindChildNodes(const KVPairVector& sortedKVPairVector, const Node& parentNode, NodeVector& childNodes);
    size_t FindBegin(const KVPairVector& sortedKVPairVector, const NodeVector& childNodes);
    bool CheckOtherChildNodes(const NodeVector& childNodes, size_t begin);
    void Resize(size_t newSize);
    size_t Insert(const KVPairVector& sortedKVPair, const NodeVector& childNodes);

private:
    StateArray mStateArray;
    std::vector<bool> mUsed;
    size_t mNextCheckPos;
    size_t mProgress;
    size_t mSize;
    bool mError;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DoubleArrayTrie);
/////////////////////////////////////////////////////
inline int32_t DoubleArrayTrie::Match(const void* data, const autil::StringView& key)
{
    assert(data);
    const State* stateArray = (const State*)data;
    int32_t base = stateArray[0].base;
    uint32_t pos;
    size_t keyLength = key.size();
    const uint8_t* keyData = (uint8_t*)key.data();
    for (size_t i = 0; i < keyLength; ++i) {
        pos = base + (uint32_t)keyData[i] + 1;
        if ((uint32_t)base == stateArray[pos].check) {
            base = stateArray[pos].base;
        } else {
            return INVALID_DOCID;
        }
    }
    pos = base;
    int32_t value = stateArray[pos].base;
    if ((uint32_t)base == stateArray[pos].check && value < 0) {
        return -(value + 1);
    }
    return INVALID_DOCID;
}

inline size_t DoubleArrayTrie::PrefixSearch(const void* data, const autil::StringView& key, KVPairVector& results,
                                            int32_t maxMatches)
{
    assert(data);
    const State* stateArray = (const State*)data;
    int32_t base = stateArray[0].base;
    uint32_t pos;
    int32_t value;
    size_t keyLength = key.size();
    const uint8_t* keyData = (uint8_t*)key.data();
    int32_t matchCount = 0; // results may not empty
    for (size_t i = 0; i < keyLength; ++i) {
        if (unlikely(matchCount >= maxMatches)) {
            return matchCount;
        }
        pos = base; // +0;
        value = stateArray[pos].base;
        if ((uint32_t)base == stateArray[pos].check && value < 0) {
            autil::StringView retKey((const char*)keyData, i);
            int32_t retValue = -(value + 1);
            results.push_back(std::make_pair(retKey, retValue));
            ++matchCount;
        }
        pos = base + (uint32_t)(keyData[i]) + 1; // +C
        if ((uint32_t)base == stateArray[pos].check) {
            base = stateArray[pos].base;
        } else {
            return matchCount;
        }
    }
    if (unlikely(matchCount >= maxMatches)) {
        return matchCount;
    }
    pos = base; // +0
    value = stateArray[pos].base;
    if ((uint32_t)base == stateArray[pos].check && value < 0) {
        autil::StringView retKey((const char*)keyData, keyLength);
        int32_t retValue = -(value + 1);
        results.push_back(std::make_pair(retKey, retValue));
        ++matchCount;
    }

    return matchCount;
}
}} // namespace indexlib::index
