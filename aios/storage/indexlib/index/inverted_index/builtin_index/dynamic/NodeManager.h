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

#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicSearchTreeNode.h"
#include "indexlib/util/EpochBasedReclaimManager.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib::index {
class NodeManager final : public util::EpochBasedReclaimManager
{
public:
    NodeManager(autil::mem_pool::PoolBase* pool);
    ~NodeManager();

    NodeManager(const NodeManager&) = delete;
    NodeManager(NodeManager&&) = delete;

public:
    int64_t TotalMemory() const { return _totalMemory; }
    int64_t RetiredMemory() const { return _retiredMemory; }
    int64_t TotalAllocatedMemory() const { return _totalAllocatedMemory; }
    int64_t TotalFreedMemory() const { return _totalFreedMemory; }

    LeafNode* AllocateLeafNode(size_t maxSlotShift);
    InternalNode* AllocateInternalNode(size_t maxSlotShift);

    void RetireNode(Node* node);

protected:
    void Free(void* addr) override;

private:
    autil::mem_pool::PoolBase* _pool;
    int64_t _totalMemory;
    int64_t _retiredMemory;
    int64_t _totalAllocatedMemory;
    int64_t _totalFreedMemory;
};

} // namespace indexlib::index