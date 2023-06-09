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
#include "indexlib/index/inverted_index/builtin_index/dynamic/NodeManager.h"

#include "autil/EnvUtil.h"

namespace indexlib::index {
namespace {
const size_t RECLAIM_THRESHOLD = []() {
    size_t threshold = autil::EnvUtil::getEnv<size_t>("indexlib_dynamic_node_reclaim_threshold", 100);
    return threshold;
}();
} // namespace

NodeManager::NodeManager(autil::mem_pool::PoolBase* pool)
    : util::EpochBasedReclaimManager(RECLAIM_THRESHOLD, -1)
    , _pool(pool)
    , _totalMemory(0)
    , _retiredMemory(0)
    , _totalAllocatedMemory(0)
    , _totalFreedMemory(0)
{
    assert(pool);
}

NodeManager::~NodeManager() { Clear(); }

LeafNode* NodeManager::AllocateLeafNode(size_t maxSlotShift)
{
    auto mem = LeafNode::Memory(maxSlotShift);
    auto addr = _pool->allocate(mem);
    _totalMemory += mem;
    _totalAllocatedMemory += mem;
    assert(addr);
    ::memset(addr, 0, mem);
    return ::new (addr) LeafNode(maxSlotShift);
}

InternalNode* NodeManager::AllocateInternalNode(size_t maxSlotShift)
{
    auto mem = InternalNode::Memory(maxSlotShift);
    auto addr = _pool->allocate(mem);
    _totalMemory += mem;
    _totalAllocatedMemory += mem;
    assert(addr);
    ::memset(addr, 0, mem);
    return ::new (addr) InternalNode(maxSlotShift);
}

void NodeManager::RetireNode(Node* node)
{
    auto mem = NodeMemory(node);
    _retiredMemory += mem;
    util::EpochBasedReclaimManager::Retire(node);
}

void NodeManager::Free(void* addr)
{
    auto node = reinterpret_cast<Node*>(addr);
    auto mem = NodeMemory(node);
    _retiredMemory -= mem;
    _totalMemory -= mem;
    _totalFreedMemory += mem;
    _pool->deallocate(addr, mem);
}

} // namespace indexlib::index
