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

#include "navi/util/ReadyBitMap.h"
#include "navi/engine/AsyncPipe.h"

namespace navi {

class Node;

class NodeAsyncInfo
{
public:
    NodeAsyncInfo(Node *node, const std::shared_ptr<NodeAsyncInfo> &old);
    ~NodeAsyncInfo();
    NodeAsyncInfo(const NodeAsyncInfo &) = delete;
    NodeAsyncInfo &operator=(const NodeAsyncInfo &) = delete;
public:
    AsyncPipePtr addOne(ActivateStrategy activateStrategy);
    bool consolidate();
    void terminate();
public:
    bool independentReady() const;
    bool isOk() const;
    bool hasReady() const;
    bool isFinish() const;
    bool isFinishConsumed() const;
private:
    void refresh();
private:
    Node *_node;
    std::vector<AsyncPipePtr> _pipes;
    std::vector<AsyncPipePtr> _independentPipes;
    ReadyBitMap *_readyMap = nullptr;
    ReadyBitMap *_consumedMap = nullptr;
};

NAVI_TYPEDEF_PTR(NodeAsyncInfo);

}

