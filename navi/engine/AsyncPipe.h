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
    AsyncPipe(Node *node, ReadyBitMap *readyMap, ReadyBitMap *consumedMap, size_t pipeIndex);
    AsyncPipe(const AsyncPipe &) = delete;
    AsyncPipe &operator=(const AsyncPipe &) = delete;
    virtual ~AsyncPipe() {};
public:
    ErrorCode setData(const DataPtr &data);
    ErrorCode setEof();
    void getData(DataPtr &data, bool &eof);
    void terminate();

private:
    void decItemCount();
    void incItemCount();
    void scheduleNode() const;

private:
    Node *_node;
    ReadyBitMap *_nodeReadyMap;
    ReadyBitMap *_nodeConsumedMap;
    size_t _pipeIndex;
    bool _terminated;
    autil::ThreadMutex _mutex;
    std::list<DataPtr> _dataList;
};

NAVI_TYPEDEF_PTR(AsyncPipe);

} // namespace navi
