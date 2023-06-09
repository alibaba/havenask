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
#include "navi/engine/PartState.h"

#include "navi/util/ReadyBitMap.h"

namespace navi {

MessageCounter::MessageCounter() {
    atomic_set(&_messageCount, 0);
}

void MessageCounter::incMessageCount() {
    atomic_inc(&_messageCount);
}

void MessageCounter::decMessageCount() {
    atomic_dec(&_messageCount);
}

int64_t MessageCounter::messageCount() const {
    return atomic_read(&_messageCount);
}

BorderPartState::BorderPartState(autil::mem_pool::Pool *pool,
                                 size_t edgeCount)
{
    _portEofMap = ReadyBitMap::createReadyBitMap(pool, edgeCount);
    _portEofMap->setFinish(false);
}

BorderPartState::~BorderPartState() {
    ReadyBitMap::freeReadyBitMap(_portEofMap);
}

void BorderPartState::setEof(size_t edgeId) {
    _portEofMap->setFinish(edgeId, true);
}

bool BorderPartState::eof() const {
    return _portEofMap->isFinish();
}

BorderState::BorderState() {}

bool BorderState::init(autil::mem_pool::Pool *pool, const PartInfo &partInfo,
                       size_t edgeCount)
{
    if (_partStates) {
        return false;
    }
    // TODO sub border part states?
    auto partStates =
        (BorderPartState *)malloc(sizeof(BorderPartState) * partInfo.getFullPartCount());
    for (int32_t i = 0; i < partInfo.getFullPartCount(); i++) {
        auto mem = &partStates[i];
        new (mem) BorderPartState(pool, edgeCount);
    }
    _partInfo = partInfo;
    _partStates = partStates;
    return true;
}

BorderState::~BorderState() {
    if (_partStates) {
        for (NaviPartId partId = 0; partId < _partInfo.getFullPartCount(); partId++) {
            POOL_DELETE_CLASS(&_partStates[partId]);
        }
        free(_partStates);
    }
    _partStates = nullptr;
}

void BorderState::setEof(NaviPartId partId, size_t edgeId) {
    assert(_partInfo.isUsed(partId));
    _partStates[partId].setEof(edgeId);
}

bool BorderState::eof(NaviPartId partId) const {
    assert(_partInfo.isUsed(partId));
    return _partStates[partId].eof();
}

bool BorderState::eof() const {
    bool retEof = true;
    for (auto partId : _partInfo) {
        retEof &= eof(partId);
    }
    return retEof;
}

void BorderState::incMessageCount(NaviPartId partId) {
    assert(_partInfo.isUsed(partId));
    _partStates[partId].incMessageCount();
}

void BorderState::decMessageCount(NaviPartId partId) {
    assert(_partInfo.isUsed(partId));
    _partStates[partId].decMessageCount();
}

int64_t BorderState::messageCount(NaviPartId partId) const {
    assert(_partInfo.isUsed(partId));
    return _partStates[partId].messageCount();
}

StreamPartState::StreamPartState()
    : _sending(false)
    , _inited(false)
{
}

StreamPartState::~StreamPartState() {
}

bool StreamPartState::inited() const {
    return _inited;
}

void StreamPartState::setInited() {
    _inited = true;
}

StreamState::StreamState()
    : _partCount(1)
    , _partStates(nullptr)
    , _partSendEofMap(nullptr)
    , _partReceiveEofMap(nullptr)
    , _partCancelMap(nullptr)
{
}

StreamState::~StreamState() {
    if (_partStates) {
        for (NaviPartId partId = 0; partId < _partCount; partId++) {
            POOL_DELETE_CLASS(&_partStates[partId]);
        }
        _partStates = nullptr;
    }
    ReadyBitMap::freeReadyBitMap(_partSendEofMap);
    ReadyBitMap::freeReadyBitMap(_partReceiveEofMap);
    ReadyBitMap::freeReadyBitMap(_partCancelMap);
}

bool StreamState::init(autil::mem_pool::Pool *pool) {
    if (_partStates) {
        return false;
    }
    auto allocSize = sizeof(StreamPartState) * _partCount;
    auto mem = (StreamPartState *)pool->allocate(allocSize);;
    for (NaviPartId partId = 0; partId < _partCount; partId++) {
        new (&mem[partId]) StreamPartState();
    }
    _partStates = mem;

    _partSendEofMap = ReadyBitMap::createReadyBitMap(pool, _partCount);
    _partSendEofMap->setFinish(false);

    _partReceiveEofMap = ReadyBitMap::createReadyBitMap(pool, _partCount);
    _partReceiveEofMap->setFinish(false);

    _partCancelMap = ReadyBitMap::createReadyBitMap(pool, _partCount);
    _partCancelMap->setFinish(false);
    return true;
}

bool StreamState::streamInited() const {
    return nullptr != _partStates;
}

void StreamState::lock(NaviPartId partId) {
    _partStates[partId]._lock.lock();
}

void StreamState::unlock(NaviPartId partId) {
    _partStates[partId]._lock.unlock();
}

StreamPartState &StreamState::getPartState(NaviPartId partId) {
    return _partStates[partId];
}

void StreamState::setPartCount(NaviPartId partCount) {
    _partCount = partCount;
}

NaviPartId StreamState::getPartCount() const {
    return _partCount;
}

bool StreamState::inited(NaviPartId partId) const {
    return _partStates[partId].inited();
}

void StreamState::setInited(NaviPartId partId) {
    _partStates[partId].setInited();
}

void StreamState::incMessageCount(NaviPartId partId) {
    _partStates[partId].incMessageCount();
}

void StreamState::decMessageCount(NaviPartId partId) {
    _partStates[partId].decMessageCount();
}

bool StreamState::noMessage(NaviPartId partId) const {
    auto &partStat = _partStates[partId];
    return partStat.messageCount() <= 0 && partStat.inited();
}

void StreamState::setSendEof(NaviPartId partId) {
    _partSendEofMap->setFinish(partId, true);
}

bool StreamState::sendEof(NaviPartId partId) const {
    return _partSendEofMap->isFinish(partId);
}

bool StreamState::sendEof() const {
    return _partSendEofMap->isFinish();
}

void StreamState::setReceiveEof(NaviPartId partId) {
    _partReceiveEofMap->setFinish(partId, true);
}

bool StreamState::receiveEof(NaviPartId partId) const {
    return _partReceiveEofMap->isFinish(partId);
}

bool StreamState::receiveEof() const {
    return _partReceiveEofMap->isFinish();
}

void StreamState::receiveCancel(NaviPartId partId) {
    _partCancelMap->setFinish(partId, true);
}

bool StreamState::cancelled(NaviPartId partId) const {
    return _partCancelMap->isFinish(partId);
}

}
