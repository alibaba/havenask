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
#include "aios/apps/facility/cm2/cm_sub/conhash/maglev_conhash.h"

#include "autil/HashAlgorithm.h"
#include "autil/cityhash/city.h"

namespace cm_sub {

AUTIL_LOG_SETUP(cm_sub, MaglevConhash);

MaglevConhashNode::MaglevConhashNode(const std::string& mId, uint32_t mWeight, uint32_t mVirtualNodeRatio,
                                     uint64_t tableSize)
    : id(mId)
    , virtualNodeRatio(mVirtualNodeRatio)
    , virtualWeight(mWeight * mVirtualNodeRatio)
    , offset(0)
    , skip(0)
{
    // 选取两种无关的hash函数分别计算,提高计算结果的随机性
    offset = (autil::CityHash64WithSeed(mId.c_str(), mId.size(), 0)) % (tableSize);
    skip = ((autil::HashAlgorithm::hashString64(mId.c_str(), mId.size())) % (tableSize - 1)) + 1;
}

MaglevConhash::MaglevConhash(uint64_t tableSize) : _tableSize(tableSize) {}

void MaglevConhash::addNode(const std::string& id, uint32_t weight, uint32_t virtualNodeRatio)
{
    if (_tableSize <= 1) {
        AUTIL_LOG(WARN, "maglev conhash table size %ld too small, add node failed", _tableSize);
        return;
    }
    virtualNodeRatio = (virtualNodeRatio == 0 ? kVirtualNodeRatio : virtualNodeRatio);
    auto iter = _idNodeMap.find(id);
    if (iter != _idNodeMap.end()) {
        auto& node = iter->second;
        node->virtualWeight += weight * virtualNodeRatio;
        node->virtualNodeRatio = virtualNodeRatio;
        return;
    }

    auto node = std::make_shared<MaglevConhashNode>(id, weight, virtualNodeRatio, _tableSize);
    _idNodeMap[id] = node;
    _nodes.push_back(node);
}

void MaglevConhash::removeNode(const std::string& id, uint32_t weight, uint32_t virtualNodeRatio)
{
    virtualNodeRatio = (virtualNodeRatio == 0 ? kVirtualNodeRatio : virtualNodeRatio);
    auto iter = _idNodeMap.find(id);
    if (iter == _idNodeMap.end()) {
        return;
    }

    auto& node = iter->second;
    node->virtualWeight -= weight * virtualNodeRatio;
    if (node->virtualWeight > 0) {
        return;
    }

    for (auto iter = _nodes.begin(); iter != _nodes.end(); ++iter) {
        if (*iter == node) {
            _nodes.erase(iter);
            break;
        }
    }
    _idNodeMap.erase(id);
}

void MaglevConhash::buildTable()
{
    _table.clear();
    _table.resize(_tableSize);

    double maxWeight = 0;
    // node records for build table <targetWeight, next>
    std::vector<std::tuple<double, uint64_t>> nodeRecords;
    nodeRecords.reserve(_nodes.size());
    for (int i = 0; i < _nodes.size(); i++) {
        nodeRecords.push_back(std::tuple<double, uint64_t> {0, 0});
        auto& node = _nodes.at(i);
        if (maxWeight < node->virtualWeight) {
            maxWeight = node->virtualWeight;
        }
    }

    // Iterate through the table build entries as many times as it takes to fill up the table.
    // 这里有两个约束条件:
    // 1. iteration < _tableSize.
    //    假设每次iteration都能至少有一个bucket被填充, 那么至多tableSize次iteration后, 应该所有的bucket已经被填满
    // 2. node.next < _tableSize
    //    算法的思路是按权重填槽, 因此超出tableSize的槽位不会有对应, 认为没有意义
    uint64_t tableIndex = 0;
    for (uint32_t iteration = 1; tableIndex < _tableSize && iteration < _tableSize; ++iteration) {
        for (uint64_t i = 0; i < _nodes.size(); i++) {
            // To understand how target_weight_ and weight_ are used below, consider a host with weight
            // equal to max_weight. This would be picked on every single iteration. If it had
            // weight equal to max_normalized_weight / 3, then it would only be picked every 3 iterations,
            // etc.
            auto& node = _nodes.at(i);
            std::tuple<double, uint64_t>& nodeRecord = nodeRecords.at(i);
            double& targetWeight = std::get<0>(nodeRecord);
            if (iteration * node->virtualWeight < targetWeight) {
                continue;
            }

            // node can pick bucket this round
            uint64_t& next = std::get<1>(nodeRecord);
            for (; next < _tableSize; next++) {
                // permutation
                uint64_t index = (node->offset + (node->skip * next)) % _tableSize;
                if (_table[index] != nullptr) {
                    continue;
                }
                _table[index] = node;
                break;
            }
            next++;
            targetWeight = targetWeight + maxWeight;
            tableIndex++;
        }
    }

    // 检查是否所有槽都填满了. 如果不是(概率应该比较小), 从index最小的开始填充
    uint64_t left = 0;
    for (uint64_t i = 0; i < _table.size(); i++) {
        if (_table[i] == nullptr) {
            _table[i] = _nodes[left % _nodes.size()];
            left++;
        }
    }
    if (left > 0) {
        AUTIL_LOG(WARN, "maglev conhash fill table left %ld bucket fill on index", left);
    }
}

int MaglevConhash::getWeight(const std::string& id)
{
    auto iter = _idNodeMap.find(id);
    if (iter != _idNodeMap.end()) {
        return (int)(iter->second->virtualWeight / iter->second->virtualNodeRatio);
    }
    return 0;
}

std::string MaglevConhash::lookup(uint64_t key) const
{
    if (_table.empty()) {
        return "";
    }
    uint64_t index = key % _tableSize;
    if (_table[index] == nullptr) {
        AUTIL_LOG(WARN, "lookup failed, found no node in bucket %ld", index);
        return "";
    }
    return _table[index]->id;
}

MaglevConhash& MaglevConhash::clone(const MaglevConhash& from)
{
    reset();
    _tableSize = from._tableSize;
    _nodes.reserve(from._nodes.size());
    for (auto& iter : from._nodes) {
        auto node = std::make_shared<MaglevConhashNode>(*iter);
        _idNodeMap[node->id] = node;
        _nodes.push_back(node);
    }
    if (!from._table.empty()) {
        buildTable();
    }
    return *this;
}

void MaglevConhash::reset()
{
    _table.clear();
    _nodes.clear();
    _idNodeMap.clear();
}

std::map<std::shared_ptr<MaglevConhashNode>, uint64_t> MaglevConhash::getStats()
{
    std::map<std::shared_ptr<MaglevConhashNode>, uint64_t> idCount;
    for (int i = 0; i < _tableSize; i++) {
        auto& v = idCount[_table[i]];
        v++;
    }
    return idCount;
}

double MaglevConhash::getTotalWeight()
{
    double weight = 0;
    for (auto& node : _nodes) {
        weight += node->virtualWeight;
    }
    return weight;
}

} // namespace cm_sub