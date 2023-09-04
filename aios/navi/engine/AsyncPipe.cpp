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

AsyncPipe::AsyncPipe(Node *node, ActivateStrategy activateStrategy)
    : _node(node)
    , _activateStrategy(activateStrategy)
    , _terminated(false)
    , _eof(false)
{
    if (!_node) { // only for test
        return;
    }
    incItemCount();
}

void AsyncPipe::terminate() {
    {
        std::list<DataPtr> dataList;
        {
            autil::ScopedLock lock(_mutex);
            std::swap(dataList, _dataList);
            if (_terminated) {
                return;
            }
            if (_nodeReadyMap) {
                _nodeReadyMap->setFinish(_pipeIndex, true);
                _nodeReadyMap->setReady(_pipeIndex, true);
            }
            if (_nodeConsumedMap) {
                _nodeConsumedMap->setFinish(_pipeIndex, true);
            }
            _terminated = true;
        }
        NAVI_KERNEL_LOG(SCHEDULE1, "async pipe terminate, index[%lu], dataList count [%lu]",
                        _pipeIndex, dataList.size());
    }
    if (_node) {
        _node->notifyPipeTerminate();
    }
    decItemCount();
}

ErrorCode AsyncPipe::setData(const DataPtr &data) {
    NAVI_KERNEL_LOG(SCHEDULE1, "async pipe set data, index[%lu] data[%p].", _pipeIndex, data.get());
    {
        autil::ScopedLock lock(_mutex);
        if (_terminated) {
            NAVI_KERNEL_LOG(DEBUG, "async pipe set data when terminated, index[%lu].", _pipeIndex);
            return EC_ASYNC_TERMINATED;
        }
        if (_eof) {
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

bool AsyncPipe::getData(DataPtr &data, bool &eof) {
    bool ret = false;
    // can only be called by kernel compute
    {
        autil::ScopedLock lock(_mutex);
        if (_terminated) {
            data.reset();
            eof = _eof;
            return false;
        }
        auto empty = _dataList.empty();
        data = nullptr;
        if (!empty) {
            data = _dataList.front();
            _dataList.pop_front();
            empty = _dataList.empty();
            ret = true;
        }
        eof = _eof && empty;
        _nodeReadyMap->setReady(_pipeIndex, !empty);
        _nodeConsumedMap->setFinish(_pipeIndex, eof);
        _node->incNodeSnapshot();
        incItemCount();
        NAVI_KERNEL_LOG(SCHEDULE1, "async pipe index[%lu] get data, finish[%d] eof[%d] data[%p] ret[%d]",
                        _pipeIndex, _eof, eof, data.get(), ret);
    }
    scheduleNode();
    decItemCount();
    if (eof) {
        terminate();
    }
    return ret;
}

ErrorCode AsyncPipe::setEof() {
    NAVI_KERNEL_LOG(SCHEDULE1, "async pipe set eof, index[%lu].", _pipeIndex);
    {
        autil::ScopedLock lock(_mutex);
        if (_terminated) {
            NAVI_KERNEL_LOG(DEBUG, "async pipe set eof when terminated, index[%lu].", _pipeIndex);
            return EC_ASYNC_TERMINATED;
        }
        if (_dataList.empty()) {
            _dataList.push_back(nullptr);
        }
        _eof = true;
        _nodeReadyMap->setFinish(_pipeIndex, true);
        _node->incNodeSnapshot();
        incItemCount();
    }
    scheduleNode();
    decItemCount();
    return EC_NONE;
}

void AsyncPipe::switchBitMap(ReadyBitMap *newReadyMap, ReadyBitMap *newConsumedMap, size_t newPipeIndex) {
    autil::ScopedLock lock(_mutex);
    auto oldIndex = _pipeIndex;
    bool optional = _activateStrategy != AS_REQUIRED;
    {
        auto oldReadyMap = _nodeReadyMap;
        auto oldReadySize = oldReadyMap ? oldReadyMap->size() : 0;
        bool ready = false;
        if (oldIndex < oldReadySize) {
            ready = oldReadyMap->isReady(oldIndex);
        }
        newReadyMap->setFinish(newPipeIndex, _eof);
        newReadyMap->setOptional(newPipeIndex, optional);
        newReadyMap->setReady(newPipeIndex, ready);
    }
    {
        auto oldConsumedMap = _nodeConsumedMap;
        auto oldConsumedSize = oldConsumedMap ? oldConsumedMap->size() : 0;
        bool consumedFinish = false;
        if (oldIndex < oldConsumedSize) {
            consumedFinish = oldConsumedMap->isFinish(oldIndex);
        }
        newConsumedMap->setFinish(newPipeIndex, consumedFinish);
        newConsumedMap->setOptional(newPipeIndex, optional);
    }
    _nodeReadyMap = newReadyMap;
    _nodeConsumedMap = newConsumedMap;
    _pipeIndex = newPipeIndex;
}

bool AsyncPipe::canRecycle() const {
    return _terminated || (_nodeConsumedMap && _nodeConsumedMap->isFinish(_pipeIndex));
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
