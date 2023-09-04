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
#include "navi/engine/InputPortGroup.h"
#include "navi/engine/PartInfo.h"

namespace navi {

std::ostream &operator<<(std::ostream &os, const InputPortGroup &inputGroup) {
    os << (void *)&inputGroup << "," << inputGroup.getInputIndexStart() << "," << inputGroup.getInputCount() << " readyMap[";
    auto *readyMap = inputGroup.getReadyMap();
    if (readyMap) {
        autil::StringUtil::toStream(os, *readyMap);
    }
    return os << "]";
}

std::ostream &operator<<(std::ostream &os, const InputPortGroup *inputGroup) {
    if (!inputGroup) {
        os << nullptr;
    } else {
        autil::StringUtil::toStream(os, *inputGroup);
    }
    return os;
}

InputPortGroup::InputPortGroup(InputTypeDef type, InputTypeDef groupType)
    : _type(type)
    , _autoFillIndex(true)
    , _groupType(groupType)
    , _readyMap(nullptr)
    , _inputIndexStart(INVALID_INDEX)
{
}

InputPortGroup::~InputPortGroup() {
    ReadyBitMap::freeReadyBitMap(_readyMap);
}

IndexType InputPortGroup::addInput(IndexType index, const EdgeOutputInfo &info) {
    IndexType size = _inputs.size();
    _inputs.emplace_back(index, info);
    if (_autoFillIndex && 0 == index) {
        return size;
    } else {
        _autoFillIndex = false;
        return index;
    }
}

bool InputPortGroup::compare(const std::pair<IndexType, EdgeOutputInfo> &left,
                             const std::pair<IndexType, EdgeOutputInfo> &right)
{
    return left.first < right.first;
}

bool InputPortGroup::postInit(const PartInfo &partInfo) {
    if (_readyMap) {
        return true;
    }
    assert(0 == _inputs.size());
    _readyMap = partInfo.createReadyBitMap();
    _readyMap->setReady(false);
    _readyMap->setOptional(IT_OPTIONAL == _type);
    _readyMap->setFinish(false);

    NAVI_KERNEL_LOG(SCHEDULE2, "end post init inputs with part info, group [%p] readyMap [%s]",
                    this, autil::StringUtil::toString(*_readyMap).c_str());
    return true;
}

bool InputPortGroup::postInit() {
    if (_readyMap) {
        return true;
    }
    IndexType arraySize = _inputs.size();
    if (!_autoFillIndex) {
        std::sort(_inputs.begin(), _inputs.end(), InputPortGroup::compare);
        for (IndexType index = 0; index < arraySize; index++) {
            auto actual = _inputs[index].first;
            if (actual < index) {
                NAVI_KERNEL_LOG(ERROR,
                        "multiple connection to group index[%d], "
                        "current edge [%s], previous edge [%s]",
                        actual,
                        _inputs[index].second.getDebugName().c_str(),
                        _inputs[actual].second.getDebugName().c_str());
                return false;
            } else if (actual > index) {
                NAVI_KERNEL_LOG(ERROR,
                        "group index skip,  lack index [%d], skip to "
                        "[%d], edge [%s]",
                        index, actual,
                        _inputs[index].second.getDebugName().c_str());
                return false;
            }
        }
    }
    _readyMap = ReadyBitMap::createReadyBitMap(_inputs.size());
    _readyMap->setReady(false);
    _readyMap->setOptional(IT_OPTIONAL == _type);
    _readyMap->setFinish(false);

    NAVI_KERNEL_LOG(SCHEDULE2, "end post init inputs, group [%p] readyMap [%s]",
                    this, autil::StringUtil::toString(*_readyMap).c_str());
    return true;
}

void InputPortGroup::setInputIndexStart(IndexType start) {
    _inputIndexStart = start;
}

IndexType InputPortGroup::getInputIndexStart() const {
    return _inputIndexStart;
}

ReadyBitMap *InputPortGroup::getReadyMap() const {
    return _readyMap;
}

size_t InputPortGroup::getInputCount() const {
    return _inputs.size();
}

bool InputPortGroup::isOk() const {
    return !_readyMap || _readyMap->isOk();
}

bool InputPortGroup::hasReady() const {
    return !_readyMap || (_readyMap->isOk() && _readyMap->hasReady());
}

bool InputPortGroup::isEof() const {
    return !_readyMap || _readyMap->isFinish();
}

void InputPortGroup::setEof(const Node *callNode) const {
    for (const auto &pair : _inputs) {
        pair.second.setEof(callNode);
    }
}

void InputPortGroup::setEof(IndexType index, const Node *callNode) const {
    _inputs[index].second.setEof(callNode);
}

EdgeOutputInfo InputPortGroup::getEdgeOutputInfo(IndexType index) const {
    if (index < _inputs.size()) {
        return _inputs[index].second;
    } else {
        return {};
    }
}

void InputPortGroup::startUpStreamSchedule(const Node *callNode) const {
    for (const auto &pair : _inputs) {
        pair.second.startUpStreamSchedule(callNode);
    }
}

void InputPortGroup::startUpStreamSchedule(IndexType index, const Node *callNode) const {
    _inputs[index].second.startUpStreamSchedule(callNode);
}

}
