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
#ifndef __WEIGHT_RR_H_
#define __WEIGHT_RR_H_

#include <vector>

#include "autil/Log.h"

namespace cm_sub {

/**
 * Weighted Round-Robin Scheduling
 * not thread-safe for concurrent
 * 算法思路:
 *      每轮循环会在大于等于当前权重的节点中进行一轮rr
 *      下一轮循环当前权重会减去GCDWeight
 * 算法举例:
 *      nodes: node1:4 node2:6 node3:8
 *      round1: cw = 8, pick node3
 *      round2: cw = 6, pick node2
 *      round3: cw = 6, pick node3
 *      round4: cw = 4, pick node1
 *      round5: cw = 4, pick node2
 *      round6: cw = 4, pick node3
 */
class WeightedRoundRobin
{
public:
    WeightedRoundRobin() = default;
    WeightedRoundRobin(const WeightedRoundRobin&) = default;
    ~WeightedRoundRobin() = default;

public:
    void addNode(int weight);

    void initSchedData();
    // schedule one node id, return -1 when error
    int schedule();
    // schedule one node id, on which match func return true
    // return -1 when error
    int schedule(std::function<bool(int)> matchFunc);
    void reset();
    void setCurrentId(int id) { _schedData.id = id; }

private:
    // max weight
    int getMaxWeight() const;
    // 辗转相除求最大公约Weight
    int getGCDWeight() const;

private:
    /* current destination pointer for weighted round-robin scheduling */
    struct WrrMark {
        int id {0}; /* point to current node */
        int cw {0}; /* current weight */
        int mw {0}; /* maximum weight */
        int di {0}; /* decreasing interval */
    };

    struct WrrNode {
        int weight {0}; /* node weight */
    };

private:
    std::vector<WrrNode> _nodes;
    WrrMark _schedData;

private:
    AUTIL_LOG_DECLARE();

    friend class WRRTest;
    friend class WRRTest_verify_Test;
    friend class WRRTest_copy_Test;
};

} // namespace cm_sub

#endif
