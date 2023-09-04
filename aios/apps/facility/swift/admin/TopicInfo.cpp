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
#include "swift/admin/TopicInfo.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "autil/TimeUtility.h"
#include "ext/alloc_traits.h"
#include "swift/common/PathDefine.h"
#include "swift/config/ConfigDefine.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/MessageComparator.h"

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, TopicInfo);
using namespace autil;
using namespace std;
using namespace swift::protocol;
using namespace swift::common;
using namespace swift::config;

static const uint16_t DEFAULT_RANGE_FROM = 0;
static const uint16_t DEFAULT_RANGE_TO = 65535;

TopicInfo::TopicInfo(const protocol::TopicCreationRequest &topicMeta, PartitionStatus status) : _topicMeta(topicMeta) {
    _currentBrokers.resize(topicMeta.partitioncount());
    _targetBrokers.resize(topicMeta.partitioncount());
    _lastBrokers.resize(topicMeta.partitioncount());
    _status.resize(topicMeta.partitioncount(), status);
    _lastStatus.resize(topicMeta.partitioncount(), status);
    _inlineVersions.resize(topicMeta.partitioncount(), protocol::InlineVersion());
    _sessionIds.resize(topicMeta.partitioncount(), -1);
    calRangeInfo();
}

TopicInfo::~TopicInfo() {}

void TopicInfo::calRangeInfo() {
    uint32_t partCount = getPartitionCount();
    if (partCount == 0) {
        return;
    }
    _ranges.resize(partCount);
    uint32_t rangeCount = DEFAULT_RANGE_TO - DEFAULT_RANGE_FROM + 1;
    uint32_t rangeFrom = DEFAULT_RANGE_FROM;
    uint32_t rangeTo = DEFAULT_RANGE_TO;
    uint32_t c = rangeCount / partCount;
    uint32_t m = rangeCount % partCount;
    uint32_t from = rangeFrom;
    for (uint32_t i = 0; i < partCount && from <= rangeTo; ++i) {
        uint32_t to = from + c + (i >= m ? 0 : 1) - 1;
        _ranges[i] = make_pair((uint16_t)from, (uint16_t)to);
        from = to + 1;
    }
}

bool TopicInfo::getRangeInfo(uint32_t index, uint32_t &from, uint32_t &to) {
    if (index >= _ranges.size()) {
        return false;
    }
    from = _ranges[index].first;
    to = _ranges[index].second;
    return true;
}

void TopicInfo::setCurrentRoleAddress(uint32_t index, const string &address) {
    ScopedWriteLock lock(_lock);
    assert(index < _currentBrokers.size());
    _currentBrokers[index] = address;
}

void TopicInfo::setTargetRoleAddress(uint32_t index, const string &address) {
    ScopedWriteLock lock(_lock);
    assert(index < _targetBrokers.size());
    _targetBrokers[index] = address;
    if (!address.empty()) {
        _lastBrokers[index] = address;
    }
}

string TopicInfo::getCurrentRoleAddress(uint32_t index) const {
    ScopedReadLock lock(_lock);
    assert(index < _currentBrokers.size());
    return _currentBrokers[index];
}

string TopicInfo::getTargetRoleAddress(uint32_t index) const {
    ScopedReadLock lock(_lock);
    assert(index < _targetBrokers.size());
    return _targetBrokers[index];
}

string TopicInfo::getLastRoleAddress(uint32_t index) const {
    ScopedReadLock lock(_lock);
    assert(index < _lastBrokers.size());
    return _lastBrokers[index];
}

string TopicInfo::getTargetRole(uint32_t index) const { return PathDefine::parseRole(getTargetRoleAddress(index)); }
string TopicInfo::getCurrentRole(uint32_t index) const { return PathDefine::parseRole(getCurrentRoleAddress(index)); }
string TopicInfo::getLastRole(uint32_t index) const { return PathDefine::parseRole(getLastRoleAddress(index)); }

void TopicInfo::setStatus(uint32_t index, protocol::PartitionStatus status) {
    ScopedWriteLock lock(_lock);
    assert(index < _status.size());
    _lastStatus[index] = _status[index];
    _status[index] = status;
}

protocol::PartitionStatus TopicInfo::getStatus(uint32_t index) const {
    ScopedReadLock lock(_lock);
    assert(index < _status.size());
    return _status[index];
}

void TopicInfo::setSessionId(uint32_t index, int64_t sessionId) {
    ScopedWriteLock lock(_lock);
    assert(index < _sessionIds.size());
    if (-1 != sessionId) {
        _sessionIds[index] = sessionId;
    }
}

int64_t TopicInfo::getSessionId(uint32_t index) const {
    ScopedReadLock lock(_lock);
    assert(index < _sessionIds.size());
    return _sessionIds[index];
}

protocol::TopicStatus TopicInfo::getTopicStatus() const {
    ScopedReadLock lock(_lock);
    protocol::TopicStatus topicStatus = TOPIC_STATUS_WAITING;
    bool hasWaiting = false;
    bool hasStarting = false;
    size_t runningPartitionCount = 0;
    for (size_t i = 0; i < _status.size(); ++i) {
        if (PARTITION_STATUS_WAITING == _status[i]) {
            hasWaiting = true;
        } else if (PARTITION_STATUS_RUNNING == _status[i]) {
            runningPartitionCount++;
        } else if (PARTITION_STATUS_STARTING == _status[i]) {
            hasStarting = true;
        }
    }

    if (runningPartitionCount == _status.size()) {
        topicStatus = TOPIC_STATUS_RUNNING;
    } else if (runningPartitionCount > 0) {
        topicStatus = TOPIC_STATUS_PARTIAL_RUNNING;
    } else if (hasWaiting) {
        topicStatus = TOPIC_STATUS_WAITING;
    } else if (hasStarting) {
        topicStatus = TOPIC_STATUS_PARTIAL_RUNNING;
    } else {
        topicStatus = TOPIC_STATUS_STOPPING;
    }

    return topicStatus;
}

bool TopicInfo::needChange(uint32_t index, bool fastRecover, uint64_t selfVersion) const {
    ScopedReadLock lock(_lock);
    assert(index < _targetBrokers.size());
    assert(index < _currentBrokers.size());
    bool changed = false;
    string chgLog = _topicMeta.topicname() + " " + to_string(index);
    if (_targetBrokers[index] != _currentBrokers[index]) {
        chgLog += " broker:" + _currentBrokers[index] + "->" + _targetBrokers[index];
        changed = true;
    }
    if (_lastStatus[index] != _status[index]) {
        chgLog += " status:" + to_string(_lastStatus[index]) + "->" + to_string(_status[index]);
        changed = true;
    }
    if (fastRecover &&
        (!_inlineVersions[index].partversion() || selfVersion != _inlineVersions[index].masterversion())) {
        assert(index < _inlineVersions.size());
        chgLog += " inline:" + _inlineVersions[index].ShortDebugString();
        changed = true;
    }
    if (changed) {
        AUTIL_LOG(INFO, "%s", chgLog.c_str());
    }
    return changed;
}

void TopicInfo::setTopicMeta(const TopicCreationRequest &topicMeta) {
    ScopedWriteLock lock(_lock);
    _topicMeta = topicMeta;
}

const TopicCreationRequest &TopicInfo::getTopicMeta() const {
    ScopedReadLock lock(_lock);
    return _topicMeta;
}

string TopicInfo::getTopicName() const {
    ScopedReadLock lock(_lock);
    return _topicMeta.topicname();
}

string TopicInfo::getTopicGroup() const {
    ScopedReadLock lock(_lock);
    if (_topicMeta.has_topicgroup()) {
        return _topicMeta.topicgroup();
    } else {
        return DEFAULT_GROUP_NAME;
    }
}

uint32_t TopicInfo::getPartitionCount() const {
    ScopedReadLock lock(_lock);
    return _topicMeta.partitioncount();
}

uint32_t TopicInfo::getRangeCountInPartition() const {
    ScopedReadLock lock(_lock);
    return _topicMeta.rangecountinpartition();
}

void TopicInfo::getPartitionRunInfo(uint32_t &runningPartitionCount,
                                    uint32_t &waitingPartitionCount,
                                    uint32_t &startingPartCount) const {
    ScopedReadLock lock(_lock);
    runningPartitionCount = 0;
    waitingPartitionCount = 0;
    startingPartCount = 0;
    for (size_t i = 0; i < _status.size(); ++i) {
        if (PARTITION_STATUS_WAITING == _status[i]) {
            waitingPartitionCount++;
        } else if (PARTITION_STATUS_RUNNING == _status[i]) {
            runningPartitionCount++;
        } else if (PARTITION_STATUS_STARTING == _status[i]) {
            startingPartCount++;
        }
    }
}

uint32_t TopicInfo::getResource() const {
    ScopedReadLock lock(_lock);
    return _topicMeta.resource();
}

bool TopicInfo::setResource(uint32_t newResource, uint32_t &oldResource) {
    {
        ScopedReadLock lock(_lock);
        oldResource = _topicMeta.resource();
    }
    if (oldResource == newResource) {
        return false;
    }
    ScopedWriteLock lock(_lock);
    _topicMeta.set_resource(newResource);
    return true;
}

uint32_t TopicInfo::getPartitionLimit() const {
    ScopedReadLock lock(_lock);
    return _topicMeta.partitionlimit();
}

bool TopicInfo::setPartitionLimit(uint32_t newLimit, uint32_t &oldLimit) {
    {
        ScopedReadLock lock(_lock);
        oldLimit = _topicMeta.partitionlimit();
    }
    if (oldLimit == newLimit) {
        return false;
    }
    ScopedWriteLock lock(_lock);
    _topicMeta.set_partitionlimit(newLimit);
    return true;
}

int64_t TopicInfo::getModifyTime() const {
    ScopedReadLock lock(_lock);
    return _topicMeta.modifytime();
}

string TopicInfo::getDfsRoot() const {
    ScopedReadLock lock(_lock);
    return _topicMeta.dfsroot();
}

bool TopicInfo::needSchedule() const {
    ScopedReadLock lock(_lock);
    return _topicMeta.topictype() != TOPIC_TYPE_LOGIC;
}

bool operator<(const TopicInfoPtr &lhs, const TopicInfoPtr &rhs) {
    assert(lhs && rhs);
    if (lhs->getResource() == rhs->getResource()) {
        return lhs->getTopicName() < rhs->getTopicName();
    } else {
        return lhs->getResource() < rhs->getResource();
    }
}

TopicType TopicInfo::getTopicType() const {
    ScopedReadLock lock(_lock);
    return _topicMeta.topictype();
}

bool TopicInfo::hasSubPhysicTopics() const {
    ScopedReadLock lock(_lock);
    return (TOPIC_TYPE_LOGIC == _topicMeta.topictype() || TOPIC_TYPE_LOGIC_PHYSIC == _topicMeta.topictype()) &&
           _topicMeta.physictopiclst_size() > 0;
}

vector<string> TopicInfo::physicTopicLst() const {
    vector<string> phyLst;
    ScopedReadLock lock(_lock);
    for (int phyCnt = 0; phyCnt < _topicMeta.physictopiclst_size(); ++phyCnt) {
        phyLst.emplace_back(_topicMeta.physictopiclst(phyCnt));
    }
    return phyLst;
}

uint32_t TopicInfo::physicTopicLstSize() const {
    ScopedReadLock lock(_lock);
    return _topicMeta.physictopiclst_size();
}

const std::string TopicInfo::getLastPhysicTopicName() const {
    ScopedReadLock lock(_lock);
    int phySize = _topicMeta.physictopiclst_size();
    if (phySize > 0) {
        return _topicMeta.physictopiclst(phySize - 1);
    } else if (0 == phySize && TOPIC_TYPE_LOGIC_PHYSIC == _topicMeta.topictype()) {
        return _topicMeta.topicname();
    } else {
        return string();
    }
}

void TopicInfo::setInlineVersion(uint32_t index, protocol::InlineVersion version) {
    ScopedWriteLock lock(_lock);
    assert(index < _inlineVersions.size());
    if (_inlineVersions[index] < version) {
        _inlineVersions[index] = std::move(version);
    }
}

protocol::InlineVersion TopicInfo::getInlineVersion(uint32_t index) const {
    ScopedReadLock lock(_lock);
    assert(index < _inlineVersions.size());
    return _inlineVersions[index];
}

bool TopicInfo::brokerChanged(uint32_t index) const {
    ScopedReadLock lock(_lock);
    assert(index < _targetBrokers.size());
    assert(index < _currentBrokers.size());
    return _targetBrokers[index] != _currentBrokers[index];
}

bool TopicInfo::enableMergeData() const {
    ScopedReadLock lock(_lock);
    return _topicMeta.enablemergedata();
}

bool TopicInfo::enableWriterVersionController() const {
    ScopedReadLock lock(_lock);
    return _topicMeta.versioncontrol();
}

bool TopicInfo::readNotCommittedMsg() const {
    ScopedReadLock lock(_lock);
    return _topicMeta.readnotcommittedmsg();
}
} // namespace admin
} // namespace swift
