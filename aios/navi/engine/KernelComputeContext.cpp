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
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/Graph.h"
#include "navi/engine/LocalSubGraph.h"
#include "navi/engine/Node.h"
#include "navi/log/NaviLogger.h"

namespace navi {

GroupDatas::GroupDatas()
    : _begin(nullptr)
    , _end(nullptr)
{
}

GroupDatas::GroupDatas(InputSnapshot *begin, InputSnapshot *end)
    : _begin(begin)
    , _end(end)
{
}

GroupDatas::Iterator::Iterator()
    : _ptr(nullptr)
{
}

GroupDatas::Iterator::Iterator(InputSnapshot *ptr)
    : _ptr(ptr)
{
}

GroupDatas::Iterator GroupDatas::Iterator::operator+(size_t offset) const {
    auto it = *this;
    it._ptr += offset;
    return it;
}

GroupDatas::Iterator GroupDatas::Iterator::operator++() {
    ++_ptr;
    return *this;
}

bool GroupDatas::Iterator::operator!=(const Iterator &rhs) const {
    return _ptr != rhs._ptr;
}

const StreamData &GroupDatas::Iterator::operator*() {
    return _ptr->data;
}

GroupDatas::Iterator GroupDatas::begin() const {
    return Iterator(_begin);
}

GroupDatas::Iterator GroupDatas::end() const {
    return Iterator(_end);
}

size_t GroupDatas::size() const {
    return _end - _begin;
}

const StreamData &GroupDatas::operator[](size_t index) {
    return _begin[index].data;
}

bool GroupDatas::eof() const {
    for (const auto &streamData : *this) {
        if (!streamData.eof) {
            return false;
        }
    }
    return true;
}

KernelComputeContext::KernelComputeContext(Node *node)
    : KernelInitContext(node)
    , _deadLock(true)
    , _ignoreDeadlock(false)
{
    assert(_node);
    _metric = node->getMetric();
}

bool KernelComputeContext::getInput(PortIndex index, DataPtr &data, bool &eof) {
    StreamData streamData;
    auto ret = getInput(index, streamData);
    data = std::move(streamData.data);
    eof = streamData.eof;
    return ret;
}

bool KernelComputeContext::getInput(PortIndex index, StreamData &streamData) {
    auto ret = _node->getInput(index, streamData);
    if (ret) {
        _deadLock = false;
    }
    return ret;
}

bool KernelComputeContext::getGroupInput(IndexType group, GroupDatas &datas) {
    auto groupCount = _node->getInputGroupCount();
    if (group >= groupCount) {
        NAVI_KERNEL_LOG(ERROR,
                        "group index [%u] overflow, node [%s] kernel [%s] "
                        "input group count [%lu]",
                        group, _node->getName().c_str(),
                        _node->getKernelName().c_str(), groupCount);
        return false;
    }
    return doGetGroupInput(group, datas);
}

bool KernelComputeContext::doGetGroupInput(IndexType group, GroupDatas &datas) {
    InputSnapshot *begin = nullptr;
    InputSnapshot *end = nullptr;
    if (!_node->snapshotGroupInput(group, begin, end)) {
        return false;
    }
    datas = GroupDatas(begin, end);
    _deadLock = false;
    return true;
}

// index can be group index
bool KernelComputeContext::setOutput(PortIndex index,
                                     const DataPtr &data, bool eof)
{
    auto ret = _node->setOutput(index, data, eof);
    if (ret) {
        _deadLock = false;
    }
    return ret;
}

bool KernelComputeContext::deadLock() const {
    return (!_ignoreDeadlock) && _deadLock;
}

void KernelComputeContext::setIgnoreDeadlock() {
    _node->incNodeSnapshot();
    _ignoreDeadlock = true;
}

void KernelComputeContext::stopSchedule() {
    _node->stopSchedule();
}

ErrorCode KernelComputeContext::fork(GraphDef *graphDef, const ForkGraphParam &param) {
    auto ret = _node->fork(graphDef, param);
    if (EC_NONE == ret) {
        _deadLock = false;
    }
    return ret;
}

const KernelMetric &KernelComputeContext::getMetric() const {
    return *_metric;
}

void KernelComputeContext::appendResult(NaviResult &result) {
    _node->appendResult(result);
}

void KernelComputeContext::updateTraceLevel(const std::string &levelStr) const {
    _node->updateTraceLevel(levelStr);
}

bool KernelComputeContext::updateTimeoutMs(int64_t timeoutMs) const {
    return _node->updateTimeoutMs(timeoutMs);
}

void KernelComputeContext::fillTrace(std::vector<std::string> &traceVec) const {
    _node->fillTrace(traceVec);
}

LoggingEvent KernelComputeContext::firstErrorEvent() const {
    return _node->firstErrorEvent();
}

ErrorCode KernelComputeContext::getScopeErrorCode() const {
    return _node->getScopeErrorCode();
}

int64_t KernelComputeContext::getRemainTimeMs() const {
    return _node->getRemainTimeMs();
}

}
