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
#include "aios/network/gig/multi_call/stream/GigStreamRpcInfo.h"

#include "autil/DataBuffer.h"
#include "autil/TimeUtility.h"

using namespace std;

namespace multi_call {

const char *convertGigStreamRpcStatus(GigStreamRpcStatus status) {
    switch (status) {
    case GSRS_NONE:
        return "NONE";
    case GSRS_EOF:
        return "EOF";
    case GSRS_CANCELLED:
        return "CANCELLED";
    }
    return "INVALID";
}

GigStreamRpcInfo::GigStreamRpcInfo(int64_t partId_) : partId(partId_) {
}

void GigStreamRpcInfo::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(partId);
    dataBuffer.write(spec);
    dataBuffer.write(sendCount);
    dataBuffer.write(receiveCount);
    dataBuffer.write(sendBeginTs);
    dataBuffer.write(sendEndTs);
    dataBuffer.write((char)sendStatus);
    dataBuffer.write(receiveBeginTs);
    dataBuffer.write(receiveEndTs);
    dataBuffer.write((char)receiveStatus);
    dataBuffer.write(isRetry);
}

void GigStreamRpcInfo::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(partId);
    dataBuffer.read(spec);
    dataBuffer.read(sendCount);
    dataBuffer.read(receiveCount);
    dataBuffer.read(sendBeginTs);
    dataBuffer.read(sendEndTs);
    char status;
    dataBuffer.read(status);
    sendStatus = (GigStreamRpcStatus)status;
    dataBuffer.read(receiveBeginTs);
    dataBuffer.read(receiveEndTs);
    dataBuffer.read(status);
    receiveStatus = (GigStreamRpcStatus)status;
    dataBuffer.read(isRetry);
}

bool GigStreamRpcInfo::isLack() const {
    return receiveStatus != GSRS_EOF;
}

std::ostream &operator<<(std::ostream &os, const GigStreamRpcInfo &rpcInfo) {
    return os << "{"
              << "part:" << rpcInfo.partId
              << " durUs:" << rpcInfo.receiveEndTs - rpcInfo.sendBeginTs
              << " SS:" << convertGigStreamRpcStatus(rpcInfo.sendStatus)
              << " RS:" << convertGigStreamRpcStatus(rpcInfo.receiveStatus)
              << " SPEC:" << rpcInfo.spec << " SC:" << rpcInfo.sendCount
              << " RC:" << rpcInfo.receiveCount << " SBT:" << rpcInfo.sendBeginTs
              << " SET:" << rpcInfo.sendEndTs << " RBT:" << rpcInfo.receiveBeginTs
              << " RET:" << rpcInfo.receiveEndTs << " Retry:" << rpcInfo.isRetry << "}";
}

bool GigStreamRpcInfo::operator==(const GigStreamRpcInfo &other) const {
    return (partId == other.partId && sendCount == other.sendCount &&
            receiveCount == other.receiveCount && sendBeginTs == other.sendBeginTs &&
            sendEndTs == other.sendEndTs && sendStatus == other.sendStatus &&
            receiveBeginTs == other.receiveBeginTs && receiveEndTs == other.receiveEndTs &&
            receiveStatus == other.receiveStatus && spec == other.spec && isRetry == other.isRetry);
}

void GigStreamRpcRecord::begin() {
    {
        autil::ScopedReadLock lock(_lock);
        if (_beginTs != 0) {
            return;
        }
    }

    autil::ScopedWriteLock lock(_lock);
    if (_beginTs != 0) {
        return;
    }
    _beginTs = autil::TimeUtility::currentTime();
}

void GigStreamRpcRecord::eof() {
    autil::ScopedWriteLock lock(_lock);
    if (_status != GSRS_NONE) {
        return;
    }
    if (_beginTs == 0) {
        _beginTs = autil::TimeUtility::currentTime();
    }
    _status = GSRS_EOF;
    _endTs = autil::TimeUtility::currentTime();
}

void GigStreamRpcRecord::cancel() {
    autil::ScopedWriteLock lock(_lock);
    if (_status != GSRS_NONE) {
        return;
    }
    if (_beginTs == 0) {
        _beginTs = autil::TimeUtility::currentTime();
    }
    _status = GSRS_CANCELLED;
    _endTs = autil::TimeUtility::currentTime();
}

void GigStreamRpcRecord::snapshot(GigStreamRpcStatus &status, int64_t &beginTs,
                                  int64_t &endTs) const {
    autil::ScopedReadLock lock(_lock);
    status = _status;
    beginTs = _beginTs;
    endTs = _endTs;
}

GigStreamRpcInfoController::GigStreamRpcInfoController(PartIdTy partId) : _partId(partId) {
    atomic_set(&_closureCount, 0);
    atomic_set(&_totalSendClosureCount, 0);
    atomic_set(&_totalReceiveClosureCount, 0);
    atomic_set(&_totalInitClosureCount, 0);
    atomic_set(&_totalFinishClosureCount, 0);
    atomic_set(&_totalCancelClosureCount, 0);
    atomic_set(&_totalSendCount, 0);
    atomic_set(&_totalReceiveCount, 0);
    atomic_set(&_totalSendSize, 0);
    atomic_set(&_totalReceiveSize, 0);
}

GigStreamRpcInfoController::~GigStreamRpcInfoController() {
}

void GigStreamRpcInfoController::snapshot(GigStreamRpcInfo &rpcInfo) const {
    rpcInfo.partId = _partId;
    rpcInfo.sendCount = atomic_read(&_totalSendCount);
    rpcInfo.receiveCount = atomic_read(&_totalReceiveCount);
    _sendRecord.snapshot(rpcInfo.sendStatus, rpcInfo.sendBeginTs, rpcInfo.sendEndTs);
    _receiveRecord.snapshot(rpcInfo.receiveStatus, rpcInfo.receiveBeginTs, rpcInfo.receiveEndTs);
}

void calcStreamRpcConverage(const GigStreamRpcInfoMap &rpcInfo, size_t &usedRpcCount,
                            size_t &lackRpcCount) {
    usedRpcCount = 0;
    lackRpcCount = 0;
    for (auto &pair : rpcInfo) {
        size_t tmpUsedRpcCount = 0, tmpLackRpcCount = 0;
        calcStreamRpcConverage(pair.second, tmpUsedRpcCount, tmpLackRpcCount);
        usedRpcCount += tmpUsedRpcCount;
        lackRpcCount += tmpLackRpcCount;
    }
}

void calcStreamRpcConverage(const GigStreamRpcInfoVec &rpcInfoVec, size_t &usedRpcCount,
                            size_t &lackRpcCount) {
    usedRpcCount = 0;
    lackRpcCount = 0;
    for (auto &partRpcInfo : rpcInfoVec) {
        ++usedRpcCount;
        if (partRpcInfo.isLack()) {
            ++lackRpcCount;
        }
    }
}

} // namespace multi_call
