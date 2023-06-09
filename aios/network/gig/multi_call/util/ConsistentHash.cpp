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
#include "aios/network/gig/multi_call/util/ConsistentHash.h"
#include "aios/network/gig/multi_call/common/ControllerParam.h"
#include <algorithm>

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, ConsistentHash);

bool HashNode::operator<(const HashNode &rha) const { return key < rha.key; }

ConsistentHash::ConsistentHash()
    : _virtualNodeCount(0), _hashCircleSize(0), _hashCircle(NULL) {}

ConsistentHash::~ConsistentHash() {
    _hashCircleSize = 0;
    delete[] _hashCircle;
    _hashCircle = NULL;
}

void ConsistentHash::init(size_t rowCount, size_t virtualNodeCount) {
    _virtualNodeCount = virtualNodeCount;
    _hashCircleSize = _virtualNodeCount * rowCount;
    if (_hashCircle) {
        delete[] _hashCircle;
    }
    _hashCircle = new HashNode[_hashCircleSize];
}

void ConsistentHash::add(size_t offset, uint64_t key) {
    uint64_t virtualKey = key;
    size_t index = offset * _virtualNodeCount;
    for (size_t i = 0; i < _virtualNodeCount; ++i, ++index) {
        auto &node = _hashCircle[index];
        node.key = virtualKey;
        node.label = i % MAX_WEIGHT;
        node.offset = offset;
        virtualKey = virtualKey * 1103515245 + 12345;
    }
}

void ConsistentHash::construct() {
    sort(_hashCircle, _hashCircle + _hashCircleSize);
}

ConsistentHash::Iterator ConsistentHash::get(uint64_t key) const {
    HashNode findNode;
    findNode.key = key;
    Iterator it = lower_bound(begin(), end(), findNode);
    if (end() == it) {
        it = begin();
    }
    return it;
}

ConsistentHash::Iterator ConsistentHash::begin() const { return _hashCircle; }
ConsistentHash::Iterator ConsistentHash::end() const {
    return _hashCircle + _hashCircleSize;
}

} // namespace multi_call
