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

#include "aios/apps/facility/cm2/cm_sub/conhash/weight_conhash.h"

#include <algorithm>
#include <map>
#include <string>

#include "autil/HashAlgorithm.h"

using namespace std;
using namespace autil;

namespace cm_sub {
AUTIL_LOG_SETUP(cm_sub, JumpWeightConHash);

// different CMCluster may have same spec nodes but inserted into same TopoCluster,
// which means this maybe called with same `id' more than once
void JumpWeightConHash::addNode(const string& id, int weight, uint32_t virtualNodeRatio)
{
    if (weight <= 0) {
        return;
    }

    autil::ScopedWriteLock lock(_lock);
    // 所有节点相同的virtual_node_ratio
    virtualNodeRatio = (virtualNodeRatio == 0 ? kVirtualNodeRatio : virtualNodeRatio);
    _virtualNodeRatio = virtualNodeRatio;
    int count = weight * virtualNodeRatio;
    int currentSize = 0;
    auto ite = _weightMap.find(id);
    if (_weightMap.end() != ite) {
        currentSize = ite->second;
        ite->second += count;
    } else {
        _weightMap[id] = count;
    }
    for (int i = currentSize; i < currentSize + count; ++i) {
        char key[512];
        snprintf(key, sizeof(key), "%s#%d", id.c_str(), i);
        uint64_t hashKey = autil::HashAlgorithm::hashString64(key);
        if (_ring.find(hashKey) != _ring.end()) {
            AUTIL_LOG(ERROR, "ring has hash key conflict old_id[%s], new_id[%s]", _ring[hashKey].c_str(), id.c_str());
        }
        _ring[hashKey] = id;
    }
}

void JumpWeightConHash::removeNode(const string& id, int weight, uint32_t virtualNodeRatio)
{
    if (weight <= 0) {
        return;
    }
    autil::ScopedWriteLock lock(_lock);
    virtualNodeRatio = (virtualNodeRatio == 0 ? kVirtualNodeRatio : virtualNodeRatio);
    int count = weight * virtualNodeRatio;
    auto ite = _weightMap.find(id);
    int currentSize = 0;
    if (_weightMap.end() != ite) {
        currentSize = ite->second;
        ite->second -= count;
        if (ite->second <= 0) {
            _weightMap.erase(ite);
        }
    } else {
        return;
    }
    for (int i = currentSize - 1; i > currentSize - 1 - count && i >= 0; --i) {
        char key[512];
        snprintf(key, sizeof(key), "%s#%d", id.c_str(), i);
        uint64_t hashKey = autil::HashAlgorithm::hashString64(key);
        _ring.erase(hashKey);
    }
}

int JumpWeightConHash::updateNode(const string& id, int weight, uint32_t virtualNodeRatio)
{
    virtualNodeRatio = (virtualNodeRatio == 0 ? kVirtualNodeRatio : virtualNodeRatio);
    _virtualNodeRatio = virtualNodeRatio;
    int count = weight * virtualNodeRatio;
    if (count < 0) {
        count = 0;
    }

    int currentSize = 0;
    {
        autil::ScopedReadLock lock(_lock);
        auto ite = _weightMap.find(id);
        if (ite != _weightMap.end()) {
            currentSize = ite->second;
            if (currentSize < 0) {
                currentSize = 0;
            }
        }
    }
    // match target weight, no change
    if (count == currentSize) {
        return 0;
    }

    autil::ScopedWriteLock lock(_lock);
    if (count > currentSize) {
        // 增加权重
        for (int i = currentSize; i < count; ++i) {
            char key[512];
            snprintf(key, sizeof(key), "%s#%d", id.c_str(), i);
            uint64_t hashKey = autil::HashAlgorithm::hashString64(key);
            if (_ring.find(hashKey) != _ring.end()) {
                AUTIL_LOG(ERROR, "ring has hash key conflict old_id[%s], new_id[%s] index[%d]", _ring[hashKey].c_str(),
                          id.c_str(), i);
            }
            _ring[hashKey] = id;
        }
        _weightMap[id] = count;
        return count - currentSize;
    }

    // 减少权重
    for (int i = currentSize - 1; i >= count && i >= 0; --i) {
        char key[512];
        snprintf(key, sizeof(key), "%s#%d", id.c_str(), i);
        uint64_t hashKey = autil::HashAlgorithm::hashString64(key);
        _ring.erase(hashKey);
    }
    if (count > 0) {
        _weightMap[id] = count;
    } else {
        _weightMap.erase(id);
    }
    return count - currentSize;
}

int JumpWeightConHash::getWeight(const string& id)
{
    autil::ScopedReadLock lock(_lock);
    auto ite = _weightMap.find(id);
    if (_weightMap.end() != ite) {
        return ite->second / _virtualNodeRatio;
    } else {
        return 0;
    }
}

string JumpWeightConHash::lookup(uint64_t key)
{
    autil::ScopedReadLock lock(_lock);
    if (_ring.size() == 0) {
        return "";
    }
    auto ite = _ring.upper_bound(key);
    if (_ring.end() != ite) {
        return ite->second;
    }
    return _ring.begin()->second;
}

JumpWeightConHash& JumpWeightConHash::clone(const std::shared_ptr<JumpWeightConHash>& from)
{
    autil::ScopedWriteLock lock(_lock);
    autil::ScopedReadLock fromLock(from->_lock);
    _ring = from->_ring;
    _weightMap = from->_weightMap;
    _virtualNodeRatio = from->_virtualNodeRatio;
    return *this;
}

void JumpWeightConHash::reset()
{
    autil::ScopedWriteLock lock(_lock);
    _ring.clear();
    _weightMap.clear();
}

size_t JumpWeightConHash::getRingSize()
{
    autil::ScopedReadLock lock(_lock);
    return _ring.size();
}
} // namespace cm_sub