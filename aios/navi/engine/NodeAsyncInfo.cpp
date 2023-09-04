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
#include "navi/engine/NodeAsyncInfo.h"

namespace navi {

NodeAsyncInfo::NodeAsyncInfo(Node *node, const NodeAsyncInfoPtr &old)
    : _node(node)
{
    if (old) {
        _pipes = old->_pipes;
    }
}

NodeAsyncInfo::~NodeAsyncInfo() {
    if (_readyMap) {
        ReadyBitMap::freeReadyBitMap(_readyMap);
    }
    if (_consumedMap) {
        ReadyBitMap::freeReadyBitMap(_consumedMap);
    }
}

AsyncPipePtr NodeAsyncInfo::addOne(ActivateStrategy activateStrategy) {
    auto newPipe = std::make_shared<AsyncPipe>(_node, activateStrategy);
    _pipes.push_back(newPipe);
    consolidate();
    return newPipe;
}

bool NodeAsyncInfo::consolidate() {
    std::vector<AsyncPipePtr> newPipes;
    std::swap(newPipes, _pipes);
    _independentPipes.clear();
    for (const auto &pipe : newPipes) {
        if (pipe->canRecycle()) {
            continue;
        }
        _pipes.push_back(pipe);
        if (AS_INDEPENDENT == pipe->getActivateStrategy()) {
            _independentPipes.push_back(pipe);
        }
    }
    refresh();
    return !_pipes.empty();
}

void NodeAsyncInfo::refresh() {
    auto bitMapSize = _pipes.size();
    _readyMap = ReadyBitMap::createReadyBitMap(bitMapSize);
    _consumedMap = ReadyBitMap::createReadyBitMap(bitMapSize);
    for (size_t index = 0; index < bitMapSize; index++) {
        const auto &pipe = _pipes[index];
        pipe->switchBitMap(_readyMap, _consumedMap, index);
    }
}

void NodeAsyncInfo::terminate() {
    for (const auto &pipe : _pipes) {
        pipe->terminate();
    }
    _pipes.clear();
}

bool NodeAsyncInfo::independentReady() const {
    for (const auto &pipe : _independentPipes) {
        auto index = pipe->getIndex();
        if (_readyMap->isReady(index)) {
            return true;
        }
    }
    return false;
}

bool NodeAsyncInfo::isOk() const {
    return _readyMap->isOk();
}

bool NodeAsyncInfo::hasReady() const {
    return _readyMap->hasReady();
}

bool NodeAsyncInfo::isFinish() const {
    return _readyMap->isFinish();
}

bool NodeAsyncInfo::isFinishConsumed() const {
    return _consumedMap->isFinish();
}

}

