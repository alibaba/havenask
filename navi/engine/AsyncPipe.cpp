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
#include "navi/engine/AsyncPipe.h"

#include "navi/engine/Node.h"

namespace navi {

AsyncPipe::AsyncPipe(Node *node, ReadyBitMap *readyMap, ReadyBitMap *consumedMap, size_t pipeIndex)
    : _node(node)
    , _nodeReadyMap(readyMap)
    , _nodeConsumedMap(consumedMap)
    , _pipeIndex(pipeIndex)
    , _terminated(false) {
    if (!_node) { // only for test
        return;
    }
    incItemCount();
}

void __attribute__((weak)) AsyncPipe::terminate() {
    {
        std::list<DataPtr> dataList;
        {
            autil::ScopedLock lock(_mutex);
            assert(!_terminated);
            _terminated = true;
            std::swap(dataList, _dataList);
        }
        NAVI_KERNEL_LOG(SCHEDULE1, "async pipe terminate, index[%lu], dataList count [%lu]",
                        _pipeIndex, dataList.size());
    }
    decItemCount();
}

ErrorCode __attribute__((weak)) AsyncPipe::setData(const DataPtr &data) {
    NAVI_KERNEL_LOG(SCHEDULE1, "async pipe set data, index[%lu] data[%p].", _pipeIndex, data.get());
    {
        autil::ScopedLock lock(_mutex);
        if (_terminated) {
            NAVI_KERNEL_LOG(DEBUG, "async pipe set data when terminated, index[%lu].", _pipeIndex);
            return EC_ASYNC_TERMINATED;
        }

        if (_nodeReadyMap->isFinish(_pipeIndex)) {
            NAVI_KERNEL_LOG(ERROR,
                            "async pipe index[%lu] setData error, nodeReadyMap is finish.",
                            _pipeIndex);
            return EC_DATA_AFTER_EOF;
        }

        _dataList.emplace_back(data);
        _nodeReadyMap->setReady(_pipeIndex, true);
        _node->incNodeSnapshot();
        incItemCount();
    }
    scheduleNode();
    decItemCount();
    return EC_NONE;
}

void __attribute__((weak)) AsyncPipe::getData(DataPtr &data, bool &eof) {
    // can only be called by kernel compute
    {
        autil::ScopedLock lock(_mutex);
        auto finish = _nodeReadyMap->isFinish(_pipeIndex);
        auto empty = _dataList.empty();
        data = nullptr;
        if (!empty) {
            data = _dataList.front();
            _dataList.pop_front();
            empty = _dataList.empty();
        }
        eof = finish && empty;
        _nodeReadyMap->setReady(_pipeIndex, !empty);
        _nodeConsumedMap->setFinish(_pipeIndex, eof);
        _node->incNodeSnapshot();
        NAVI_KERNEL_LOG(SCHEDULE1, "async pipe index[%lu] get data, finish[%d] eof[%d] data[%p].",
                        _pipeIndex, finish, eof, data.get());
    }
    scheduleNode();
}

ErrorCode __attribute__((weak)) AsyncPipe::setEof() {
    NAVI_KERNEL_LOG(SCHEDULE1, "async pipe set eof, index[%lu].", _pipeIndex);
    {
        autil::ScopedLock lock(_mutex);
        if (_terminated) {
            NAVI_KERNEL_LOG(DEBUG, "async pipe set eof when terminated, index[%lu].", _pipeIndex);
            return EC_ASYNC_TERMINATED;
        }
        _nodeReadyMap->setFinish(_pipeIndex, true);
        _node->incNodeSnapshot();

        incItemCount();
    }
    scheduleNode();
    decItemCount();
    return EC_NONE;
}

void AsyncPipe::incItemCount() {
    auto *work = _node->getGraph()->getParam()->worker;
    work->incItemCount();
}

void AsyncPipe::decItemCount() {
    auto *work = _node->getGraph()->getParam()->worker;
    work->decItemCount();
}

void AsyncPipe::scheduleNode() const {
    _node->schedule(_node);
}

} // namespace navi
