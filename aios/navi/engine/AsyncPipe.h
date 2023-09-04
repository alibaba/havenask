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

#include <list>

#include "navi/common.h"
#include "navi/engine/Data.h"

namespace navi {

class Node;
class ReadyBitMap;

class AsyncPipe {
public:
    AsyncPipe(Node *node, ActivateStrategy activateStrategy);
    AsyncPipe(const AsyncPipe &) = delete;
    AsyncPipe &operator=(const AsyncPipe &) = delete;
    virtual ~AsyncPipe(){};

public:
    virtual ErrorCode setData(const DataPtr &data); // virtual for test
    virtual ErrorCode setEof();                     // virtual for test
    virtual bool getData(DataPtr &data, bool &eof); // virtual for test
    virtual void terminate();                       // virtual for test

private:
    void switchBitMap(ReadyBitMap *newReadyMap, ReadyBitMap *newConsumedMap, size_t newPipeIndex);
    ActivateStrategy getActivateStrategy() const { return _activateStrategy; }
    bool canRecycle() const;
    size_t getIndex() const { return _pipeIndex; }
    void decItemCount();
    void incItemCount();
    void scheduleNode() const;

private:
    friend class NodeAsyncInfo;

private:
    Node *_node;
    ReadyBitMap *_nodeReadyMap = nullptr;
    ReadyBitMap *_nodeConsumedMap = nullptr;
    size_t _pipeIndex = 0;
    ActivateStrategy _activateStrategy;
    bool _terminated : 1;
    bool _eof        : 1;
    autil::ThreadMutex _mutex;
    std::list<DataPtr> _dataList;
};

NAVI_TYPEDEF_PTR(AsyncPipe);

} // namespace navi
