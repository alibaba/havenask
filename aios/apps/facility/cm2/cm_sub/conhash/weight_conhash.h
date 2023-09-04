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
// weight_conhash.h
// weighted conhash based on Jump Consistent Hash
#ifndef __WEIGHT_CONHASH_H
#define __WEIGHT_CONHASH_H

#include <map>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"

namespace cm_sub {
static const uint32_t kVirtualNodeRatio = 20;

class JumpWeightConHash
{
    typedef std::map<uint64_t, std::string> HashRing;
    typedef std::map<std::string, int> WeightMap;

public:
    void addNode(const std::string& id, int weight = 1, uint32_t virtualNodeRatio = kVirtualNodeRatio);
    void removeNode(const std::string& id, int weight = 1, uint32_t virtualNodeRatio = kVirtualNodeRatio);
    int updateNode(const std::string& id, int weight = 1, uint32_t virtualNodeRatio = kVirtualNodeRatio);
    int getWeight(const std::string& id);

    std::string lookup(uint64_t key);

    JumpWeightConHash& clone(const std::shared_ptr<JumpWeightConHash>& from);
    void reset();

public:
    // for test
    size_t getRingSize();

private:
    autil::ReadWriteLock _lock;
    HashRing _ring;
    WeightMap _weightMap;
    uint32_t _virtualNodeRatio;

private:
    AUTIL_LOG_DECLARE();
};
} // namespace cm_sub

#endif
