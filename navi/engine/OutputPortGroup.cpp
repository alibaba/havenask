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
#include "navi/engine/OutputPortGroup.h"
#include "navi/engine/Edge.h"
#include "navi/engine/PartInfo.h"
#include "navi/log/NaviLogger.h"

namespace navi {

OutputPortGroup::OutputPortGroup()
    : _readyMap(nullptr)
    , _outputIndexStart(INVALID_INDEX)
{
}

OutputPortGroup::~OutputPortGroup() {
    ReadyBitMap::freeReadyBitMap(_readyMap);
}

void OutputPortGroup::addOutput(IndexType index, Edge *edge) {
    assert(edge != nullptr);
    _outputs.emplace_back(index, edge);
}

bool OutputPortGroup::postInit(const PartInfo &partInfo, autil::mem_pool::Pool *pool) {
    if (_readyMap) {
        return true;
    }
    assert(0 == _outputs.size());
    _readyMap = partInfo.createReadyBitMap(pool);
    _readyMap->setReady(true);
    _readyMap->setOptional(false);
    _readyMap->setFinish(false);

    NAVI_KERNEL_LOG(SCHEDULE2, "end post init outputs with part info, group [%p] readyMap [%s]",
                    this, autil::StringUtil::toString(*_readyMap).c_str());

    return true;
}

bool OutputPortGroup::postInit(autil::mem_pool::Pool *pool) {
    if (_readyMap) {
        return true;
    }

    std::sort(_outputs.begin(), _outputs.end(), OutputPortGroup::compare);
    IndexType arraySize = _outputs.size();
    for (IndexType index = 0; index < arraySize; index++) {
        auto actual = _outputs[index].first;
        assert(actual >= index);
        if (actual > index) {
            auto inputDef = _outputs[index].second->getInputDef();
            NAVI_KERNEL_LOG(ERROR, "group index skip, lack index [%d], "
                                   "skip to [%d], node [%s], port [%s]",
                            index, actual, inputDef->node_name().c_str(),
                            inputDef->port_name().c_str());
            return false;
        }
    }

    _readyMap = ReadyBitMap::createReadyBitMap(pool, _outputs.size());
    _readyMap->setReady(true);
    _readyMap->setOptional(false);
    _readyMap->setFinish(false);

    NAVI_KERNEL_LOG(SCHEDULE2, "end post init outputs, group [%p] readyMap [%s]",
                    this, autil::StringUtil::toString(*_readyMap).c_str());
    return true;
}

void OutputPortGroup::setOutputIndexStart(IndexType start) {
    _outputIndexStart = start;
}

IndexType OutputPortGroup::getOutputIndexStart() const {
    return _outputIndexStart;
}

ReadyBitMap *OutputPortGroup::getReadyMap() const {
    return _readyMap;
}

size_t OutputPortGroup::getOutputCount() const {
    return _outputs.size();
}

bool OutputPortGroup::isOk() const {
    return !_readyMap || _readyMap->isOk();
}

bool OutputPortGroup::isEof() const {
    return !_readyMap || _readyMap->isFinish();
}

Edge *OutputPortGroup::getEdge(size_t index) const {
    if (index < _outputs.size()) {
        return _outputs[index].second;
    } else {
        return nullptr;
    }
}

size_t OutputPortGroup::getOutputDegree() const {
    size_t count = 0;
    for (const auto &pair : _outputs) {
        count += pair.second->getOutputDegree();
    }
    return count;
}

bool OutputPortGroup::compare(const std::pair<IndexType, Edge *> &left,
                              const std::pair<IndexType, Edge *> &right)
{
    return left.first < right.first;
}

}
