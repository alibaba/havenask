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
#include "swift/client/SwiftWriterAdapter.h"

#include <cstddef>
#include <cstdint>
#include <unistd.h>

#include "autil/ThreadPool.h"
#include "autil/TimeUtility.h"
#include "swift/client/SwiftSchemaWriterImpl.h"
#include "swift/client/SwiftWriterImpl.h"
#include "swift/config/ClientAuthorizerInfo.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/BlockPool.h"

using namespace std;
using namespace autil;
using namespace swift::protocol;
using namespace swift::network;
using namespace swift::util;

namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, SwiftWriterAdapter);

SwiftWriterAdapter::SwiftWriterAdapter(SwiftAdminAdapterPtr adminAdapter,
                                       SwiftRpcChannelManagerPtr channelManager,
                                       autil::ThreadPoolPtr mergeThreadPool,
                                       BufferSizeLimiterPtr limiter,
                                       util::BlockPoolPtr blockPool,
                                       monitor::ClientMetricsReporter *reporter)
    : _adminAdapter(adminAdapter)
    , _channelManager(channelManager)
    , _mergeThreadPool(mergeThreadPool)
    , _limiter(limiter)
    , _blockPool(blockPool)
    , _isSwitching(false)
    , _committedFunc(NULL)
    , _errorFunc(NULL)
    , _reporter(reporter)
    , _writeTime(-1)
    , _finishedTime(0) {}

SwiftWriterAdapter::~SwiftWriterAdapter() {
    SwiftWriterImplPtr writer;
    {
        ScopedReadLock lock(_switchLock);
        writer = _writerImpl;
    }
    if (writer) {
        waitFinished(_config.waitFinishedWriterTime);
    }
    _sendRequestThreadPtr.reset();
    _writerImpl.reset();
}

ErrorCode SwiftWriterAdapter::init(const SwiftWriterConfig &config) {
    TopicInfoResponse response;
    protocol::AuthenticationInfo authInfo;
    *(authInfo.mutable_accessauthinfo()) = config.getAccessAuthInfo();
    ErrorCode ec = _adminAdapter->getTopicInfo(config.topicName, response, 0, authInfo);
    if (ERROR_NONE != ec) {
        AUTIL_LOG(ERROR, "get topic info[%s] failed", config.topicName.c_str());
        return ec;
    }
    if (!checkAuthority(response.topicinfo())) {
        AUTIL_LOG(ERROR,
                  "authorization failed, not permitted user[%s], topic[%s]",
                  _adminAdapter->getAuthenticationConf().username.c_str(),
                  response.topicinfo().ShortDebugString().c_str());
        return ERROR_CLIENT_INIT_FAILED;
    }
    //   - N: connect
    //   - LP: find last physic, connect
    //   - L: find last physic, connect
    _topicInfo = response.topicinfo();
    _config = config;
    int physicSize = _topicInfo.physictopiclst_size();
    string physicName;
    if (TOPIC_TYPE_LOGIC_PHYSIC == _topicInfo.topictype() && physicSize != 0) {
        physicName = _topicInfo.physictopiclst(physicSize - 1);
        ec = _adminAdapter->getTopicInfo(physicName, response, 0, authInfo);
        if (ERROR_NONE != ec) {
            AUTIL_LOG(ERROR, "get topic info[%s] failed", physicName.c_str());
            return ec;
        }
    } else if (TOPIC_TYPE_LOGIC == _topicInfo.topictype()) {
        if (physicSize == 0) {
            AUTIL_LOG(ERROR, "logic topic[%s] has not physic topic", _topicInfo.name().c_str());
            return ERROR_CLIENT_INIT_INVALID_PARAMETERS;
        }
        physicName = _topicInfo.physictopiclst(physicSize - 1);
        ec = _adminAdapter->getTopicInfo(physicName, response, 0, authInfo);
        if (ERROR_NONE != ec) {
            AUTIL_LOG(ERROR, "get topic info[%s] failed", physicName.c_str());
            return ec;
        }
    } else if (TOPIC_TYPE_PHYSIC == _topicInfo.topictype()) {
        AUTIL_LOG(ERROR, "cannot write physic topic[%s]", _topicInfo.name().c_str());
        return ERROR_CLIENT_INIT_INVALID_PARAMETERS;
    }

    if (response.topicinfo().sealed()) {
        AUTIL_LOG(ERROR, "cannot write sealed topic[%s]", response.topicinfo().name().c_str());
        return ERROR_TOPIC_SEALED;
    }
    ec = createAndInitWriterImpl(response.topicinfo(), _writerImpl);
    if (ERROR_NONE != ec) {
        return ec;
    }
    if (!_config.isSynchronous && !startSendRequestThread()) {
        return ERROR_CLIENT_INIT_FAILED;
    }
    return ERROR_NONE;
}

ErrorCode SwiftWriterAdapter::createNextPhysicWriterImpl(SwiftWriterImplPtr &retWriterImpl) {
    TopicInfoResponse response;
    protocol::AuthenticationInfo authInfo;
    *(authInfo.mutable_accessauthinfo()) = _config.getAccessAuthInfo();
    ErrorCode ec = _adminAdapter->getTopicInfo(_topicInfo.name(), response, 0, authInfo);
    if (ERROR_NONE != ec) {
        AUTIL_LOG(ERROR, "get topic info[%s] failed", _config.topicName.c_str());
        return ec;
    }

    _topicInfo = response.topicinfo();
    int physicSize = _topicInfo.physictopiclst_size();
    if (0 == physicSize) {
        AUTIL_LOG(ERROR, "logic topic[%s] has no physic topic", _config.topicName.c_str());
        return ERROR_CLIENT_LOGIC_TOPIC_EMPTY_PHYSIC;
    }
    if (TOPIC_TYPE_LOGIC_PHYSIC == _topicInfo.topictype() || TOPIC_TYPE_LOGIC == _topicInfo.topictype()) {
        const string &latestPhysicName = _topicInfo.physictopiclst(physicSize - 1);
        if (latestPhysicName == _writerImpl->getTopicName()) {
            AUTIL_LOG(ERROR, "switch topic for [%s] not ready", latestPhysicName.c_str());
            return ERROR_CLIENT_PHYSIC_TOPIC_SWITCH_NOT_READY;
        }
        ec = _adminAdapter->getTopicInfo(latestPhysicName, response, 0, authInfo);
        if (ERROR_NONE != ec) {
            AUTIL_LOG(ERROR, "get topic info[%s] failed", latestPhysicName.c_str());
            return ec;
        }
    } else {
        AUTIL_LOG(ERROR, "only logic topic support physic topic");
        return ERROR_CLIENT_INIT_INVALID_PARAMETERS;
    }
    return createAndInitWriterImpl(response.topicinfo(), retWriterImpl);
}

ErrorCode SwiftWriterAdapter::createAndInitWriterImpl(const TopicInfo &tinfo, SwiftWriterImplPtr &retWriterImpl) {
    if (tinfo.needschema()) {
        retWriterImpl.reset(new SwiftSchemaWriterImpl(
            _adminAdapter, _channelManager, _mergeThreadPool, tinfo, _limiter, _topicInfo.name(), _reporter));
    } else {
        if (SwiftWriterConfig::DEFAULT_SCHEMA_VERSION != _config.schemaVersion) {
            AUTIL_LOG(ERROR,
                      "topic[%s] not support schema, cannnot set schema[%s]",
                      _config.topicName.c_str(),
                      WRITER_CONFIG_SCHEMA_VERSION);
            return ERROR_CLIENT_INVALID_PARAMETERS;
        }
        retWriterImpl.reset(new SwiftWriterImpl(
            _adminAdapter, _channelManager, _mergeThreadPool, tinfo, _limiter, _topicInfo.name(), _reporter));
    }
    retWriterImpl->setCommittedCallBack(_committedFunc);
    retWriterImpl->setErrorCallBack(_errorFunc);
    ErrorCode ec = ERROR_NONE;
    SwiftWriterConfig config = _config;
    config.topicName = tinfo.name();
    for (uint32_t retry = 0; retry != config.retryTimes; ++retry) {
        ec = retWriterImpl->init(config, _blockPool);
        if (ERROR_NONE == ec) {
            return ec;
        }
        usleep(config.retryTimeInterval);
    }
    return ec;
}

bool SwiftWriterAdapter::startSendRequestThread() {
    _sendRequestThreadPtr.reset();
    _sendRequestThreadPtr = autil::LoopThread::createLoopThread(
        std::bind(&SwiftWriterAdapter::sendRequestLoop, this), _config.sendRequestLoopInterval, "send_loop");
    if (!_sendRequestThreadPtr) {
        AUTIL_LOG(ERROR,
                  "failed to start background send request thread, "
                  "topic name [%s].",
                  _config.topicName.c_str());
        return false;
    }
    AUTIL_LOG(INFO, "start background send thread success, send interval[%ld]", _config.sendRequestLoopInterval);
    return true;
}

// 写消息非线程安全
ErrorCode SwiftWriterAdapter::write(MessageInfo &msgInfo) {
    _writeTime.store(TimeUtility::monotonicTimeUs());
    ErrorCode ec = ERROR_NONE;
    {
        ScopedReadLock lock(_switchLock);
        if (_writerImpl->topicChanged()) {
            doTopicChanged();
        }
        ec = _writerImpl->write(msgInfo);
    }
    if (ERROR_TOPIC_SEALED == ec && TOPIC_TYPE_NORMAL != _topicInfo.topictype()) {
        _isSwitching = true;
        while (_isSwitching) {
            AUTIL_LOG(INFO, "wait logic topic[%s]'s physic switch", _topicInfo.name().c_str());
            _switchNotifier.setNeedNotify(true);
            _switchNotifier.wait(1000 * 1000);
        }
        {
            ScopedReadLock lock(_switchLock);
            return _writerImpl->write(msgInfo);
        }
    } else {
        return ec;
    }
}

int64_t SwiftWriterAdapter::getLastRefreshTime(uint32_t pid) const {
    ScopedReadLock lock(_switchLock);
    return _writerImpl->getLastRefreshTime(pid);
}

int64_t SwiftWriterAdapter::getCommittedCheckpointId() const {
    ScopedReadLock lock(_switchLock);
    return _writerImpl->getCommittedCheckpointId();
}

pair<int32_t, uint16_t>
SwiftWriterAdapter::getPartitionIdByHashStr(const string &zkPath, const string &topicName, const string &hashStr) {
    ScopedReadLock lock(_switchLock);
    return _writerImpl->getPartitionIdByHashStr(zkPath, topicName, hashStr);
}

pair<int32_t, uint16_t> SwiftWriterAdapter::getPartitionIdByHashStr(const string &hashStr) {
    ScopedReadLock lock(_switchLock);
    return _writerImpl->getPartitionIdByHashStr(hashStr);
}

void SwiftWriterAdapter::getWriterMetrics(const string &zkPath, const string &topicName, WriterMetrics &writerMetric) {
    ScopedReadLock lock(_switchLock);
    return _writerImpl->getWriterMetrics(zkPath, topicName, writerMetric);
}

bool SwiftWriterAdapter::isSyncWriter() const {
    ScopedReadLock lock(_switchLock);
    return _writerImpl->isSyncWriter();
}

bool SwiftWriterAdapter::clearBuffer(int64_t &cpBeg, int64_t &cpEnd) {
    ScopedReadLock lock(_switchLock);
    return _writerImpl->clearBuffer(cpBeg, cpEnd);
}

void SwiftWriterAdapter::setForceSend(bool forceSend) {
    ScopedReadLock lock(_switchLock);
    return _writerImpl->setForceSend(forceSend);
}

bool SwiftWriterAdapter::isFinished() {
    if (_isSwitching) {
        return false;
    }
    ScopedReadLock lock(_switchLock);
    return _writerImpl->isFinished();
}

bool SwiftWriterAdapter::isAllSent() {
    if (_isSwitching) {
        return false;
    }
    ScopedReadLock lock(_switchLock);
    return _writerImpl->isAllSent();
}

std::string SwiftWriterAdapter::getTopicName() {
    ScopedReadLock lock(_switchLock);
    return _writerImpl->getTopicName();
}

common::SchemaWriter *SwiftWriterAdapter::getSchemaWriter() {
    ScopedReadLock lock(_switchLock);
    return _writerImpl->getSchemaWriter();
}

bool SwiftWriterAdapter::doSwitch() {
    // 1. create new writerImpl
    SwiftWriterImplPtr nextWriterImpl;
    ErrorCode ec = createNextPhysicWriterImpl(nextWriterImpl);
    if (ERROR_NONE != ec) {
        AUTIL_LOG(ERROR, "create next physic topic writerimpl fail");
        return false;
    }
    AUTIL_LOG(INFO,
              "switch %s[%s -> %s]",
              _topicInfo.name().c_str(),
              _writerImpl->getTopicName().c_str(),
              nextWriterImpl->getTopicName().c_str());
    {
        ScopedWriteLock lock(_switchLock);
        // 2. copy writerBuffer data to new writerImpl
        vector<MessageInfo> msgInfoVec;
        if (!_writerImpl->stealUnsendMessage(msgInfoVec)) {
            AUTIL_LOG(ERROR, "steal current writer unsend message failed");
            return false;
        }
        AUTIL_LOG(INFO, "stealed message size[%ld]", msgInfoVec.size());
        for (int i = 0; i < msgInfoVec.size(); ++i) {
            MessageInfo msgInfo = msgInfoVec[i];
            auto retry = _config.retryTimes;
            while (ERROR_NONE != nextWriterImpl->importMsg(msgInfo) && retry > 0) {
                AUTIL_LOG(ERROR, "import msg[%s] fail", msgInfo.data.c_str());
                usleep(_config.retryTimeInterval);
                --retry;
            }
            if (0 == retry) {
                return false;
            }
        }
        // 3. delete and reset writerImpl
        _writerImpl = nextWriterImpl;
        // 4. finish switch
        _isSwitching = false;
    }
    _switchNotifier.notify();
    AUTIL_LOG(INFO, "switch logic[%s] to[%s] success", _topicInfo.name().c_str(), _writerImpl->getTopicName().c_str());
    return true;
}

void SwiftWriterAdapter::sendRequestLoop() {
    static int64_t loopCount = 0;
    loopCount++;
    {
        ScopedReadLock lock(_switchLock);
        if (!_writerImpl) {
            return;
        }
        if (_writerImpl->topicChanged()) {
            doTopicChanged();
        }
    }
    if (!_isSwitching && _finishedTime > _writeTime.load() &&
        loopCount % 20 == 0) { // loop count is used for alway trigger send loop
        return;
    }
    if (_isSwitching) {
        AUTIL_LOG(INFO, "need switch topic %s", _topicInfo.name().c_str());
        if (doSwitch()) {
            AUTIL_LOG(INFO, "switch complete");
        } else {
            AUTIL_LOG(ERROR, "switch fail");
            usleep(1000 * 1000);
        }
    }
    bool hasSealError = false;
    _writerImpl->sendRequest(hasSealError);
    if (hasSealError && TOPIC_TYPE_NORMAL != _topicInfo.topictype()) {
        _isSwitching = true;
    }

    if (loopCount % 10 == 0 && _writerImpl->isFinished()) { // to reduce sys cost when lots of message need send
        _finishedTime = TimeUtility::monotonicTimeUs();
    }
}

protocol::ErrorCode SwiftWriterAdapter::doTopicChanged() {
    ScopedLock lock(_doTopicChangeLock);
    if (!_writerImpl->topicChanged()) {
        return ERROR_NONE;
    }
    AUTIL_LOG(INFO, "topic[%s] do topic change", _topicInfo.name().c_str());
    TopicInfoResponse response;
    const string &topicName = _writerImpl->getTopicName();
    protocol::AuthenticationInfo authInfo;
    *(authInfo.mutable_accessauthinfo()) = _config.getAccessAuthInfo();
    ErrorCode ec = _adminAdapter->getTopicInfo(topicName, response, _writerImpl->getTopicVersion(), authInfo);
    if (ERROR_NONE != ec) {
        AUTIL_LOG(ERROR, "get topic info[%s] failed", topicName.c_str());
        return ec;
    }
    if (topicName == _topicInfo.name()) {
        _topicInfo = response.topicinfo();
    }
    const TopicInfo &topicInfo = response.topicinfo();
    AUTIL_LOG(INFO, "topic[%s] info change to[%s]", topicName.c_str(), topicInfo.ShortDebugString().c_str());
    _writerImpl->doTopicChanged(topicInfo.partitioncount(), topicInfo.rangecountinpartition());
    return ERROR_NONE;
}

void SwiftWriterAdapter::setCommittedCallBack(const std::function<void(const vector<pair<int64_t, int64_t>> &)> &func) {
    _committedFunc = func;
    ScopedReadLock lock(_switchLock);
    _writerImpl->setCommittedCallBack(_committedFunc);
}

void SwiftWriterAdapter::setErrorCallBack(const std::function<void(const ErrorCode &)> &func) {
    _errorFunc = func;
    ScopedReadLock lock(_switchLock);
    _writerImpl->setErrorCallBack(func);
}

bool SwiftWriterAdapter::checkAuthority(const protocol::TopicInfo &ti) {
    if (!ti.has_opcontrol()) {
        return true;
    }
    return ti.opcontrol().canwrite();
}

} // namespace client
} // namespace swift
