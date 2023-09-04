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
#ifndef __CM_SUB_MAGLEV_CONHASH_H
#define __CM_SUB_MAGLEV_CONHASH_H

#include "autil/Log.h"

namespace cm_sub {

struct MaglevConhashNode {
public:
    MaglevConhashNode(const std::string& id, uint32_t weight, uint32_t virtualNodeRatio, uint64_t tableSize);

public:
    std::string id;
    uint32_t virtualNodeRatio;
    double virtualWeight;
    uint64_t offset;
    uint64_t skip;
};

/**
 * This is an implementation of Maglev consistent hashing as described in:
 * https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/44824.pdf
 * section 3.4. Specifically, the algorithm shown in pseudocode listing 1 is implemented with a
 * fixed table size of 65537. This is the recommended table size in section 5.3.
 * 对于Maglev算法来讲, 则并没有做到最小的重新映射, 也即节点变化时, 可能被变化的节点超过实际最小需要变化的节点
 * tableSize增大可以优化这个问题, 但是tableSize增大会导致build的时间更多. 需要使用者权衡.
 * 65537 vs 655373: 修改时的miss rate 1.6 vs 3.2; build的时间比例: 1 vs 15-30(节点数100->1000)
 */
class MaglevConhash
{
public:
    MaglevConhash(uint64_t tableSize = 65537);

    static const uint32_t kVirtualNodeRatio = 20;

public:
    void addNode(const std::string& id, uint32_t weight = 1, uint32_t virtualNodeRatio = kVirtualNodeRatio);
    void removeNode(const std::string& id, uint32_t weight = 1, uint32_t virtualNodeRatio = kVirtualNodeRatio);
    void buildTable();

    int getWeight(const std::string& id);
    std::string lookup(uint64_t key) const;

    MaglevConhash& clone(const MaglevConhash& from);
    void reset();

    // for test verify
    std::map<std::shared_ptr<MaglevConhashNode>, uint64_t> getStats();
    double getTotalWeight();

private:
    uint64_t _tableSize;
    std::vector<std::shared_ptr<MaglevConhashNode>> _table;

    // make sure nodes seq consist
    std::vector<std::shared_ptr<MaglevConhashNode>> _nodes;
    std::map<std::string, std::shared_ptr<MaglevConhashNode>> _idNodeMap;

    AUTIL_LOG_DECLARE();
};
} // namespace cm_sub

#endif // __CM_SUB_MAGLEV_CONHASH_H