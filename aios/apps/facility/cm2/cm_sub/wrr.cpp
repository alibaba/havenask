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
#include "aios/apps/facility/cm2/cm_sub/wrr.h"

#include <stdio.h>
#include <string.h>

namespace cm_sub {

/* get the gcd of server weights */
static int gcd(int a, int b)
{
    int c;
    while ((c = a % b)) {
        a = b;
        b = c;
    }
    return b;
}

AUTIL_LOG_SETUP(cm_sub, WeightedRoundRobin);

void WeightedRoundRobin::reset()
{
    _nodes.clear();
    _schedData.id = 0;
    _schedData.cw = 0;
    _schedData.mw = 0;
    _schedData.di = 0;
}

void WeightedRoundRobin::addNode(int weight) { _nodes.push_back({weight}); }

void WeightedRoundRobin::initSchedData()
{
    _schedData.id = 0;
    _schedData.mw = getMaxWeight();
    _schedData.cw = _schedData.mw;
    _schedData.di = getGCDWeight();
}

int WeightedRoundRobin::getMaxWeight() const
{
    int weight = 0;
    for (const auto& node : _nodes) {
        if (node.weight > weight) {
            weight = node.weight;
        }
    }
    return weight;
}

int WeightedRoundRobin::getGCDWeight() const
{
    int g = 0;
    for (const auto& node : _nodes) {
        if (node.weight > 0) {
            if (g > 0) {
                g = gcd(node.weight, g);
            } else {
                g = node.weight;
            }
        }
    }
    return g ? g : 1;
}

int WeightedRoundRobin::schedule()
{
    auto checkFunc = [](int /*unused*/) -> bool { return true; };
    return schedule(checkFunc);
}

int WeightedRoundRobin::schedule(std::function<bool(int)> matchFunc)
{
    if (_nodes.empty()) {
        /* no dest entry */
        AUTIL_LOG(WARN, "no destination available: no destinations present");
        return -1;
    }

    if (_schedData.mw == 0) {
        /* still zero, which means no available servers */
        AUTIL_LOG(WARN, "no destination available\n")
        return -1;
    }

    /*
     * This loop will always terminate, because _schedData.cw in (0, max_weight]
     * and at least one server has its weight equal to max_weight.
     */
    int startId = _schedData.id;
    while (1) {
        /* point to next element */
        if (_schedData.id == _nodes.size()) {
            /* loop start, it is at the end+1 of array */
            _schedData.id = 0;
            _schedData.cw -= _schedData.di;
            if (_schedData.cw <= 0) {
                _schedData.cw = _schedData.mw;
            }
        } else {
            ++_schedData.id;
        }

        if (_schedData.id != _nodes.size()) {
            /* looping, not at the start of the array */
            auto& node = _nodes[_schedData.id];
            if (node.weight >= _schedData.cw && matchFunc(_schedData.id)) {
                /* got it */
                return _schedData.id;
            }
        }

        if (_schedData.id == startId && _schedData.cw == _schedData.di) {
            /* back to the start, and no dest is found. It is only possible when all nodes are overload */
            AUTIL_LOG(WARN, "no destination available: all nodes are overloaded");
            return -1;
        }
    }
    return -1;
}

} // namespace cm_sub
