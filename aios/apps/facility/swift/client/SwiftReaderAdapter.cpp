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
#include "swift/client/SwiftReaderAdapter.h"

#include <cstddef>
#include <unistd.h>

#include "autil/TimeUtility.h"
#include "swift/client/SwiftSchemaReaderImpl.h"
#include "swift/client/trace/ReadMessageTracer.h"
#include "swift/config/ClientAuthorizerInfo.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/LogicTopicHelper.h"
#include "swift/util/ProtoUtil.h"

using namespace swift::protocol;
using namespace swift::util;
using namespace autil;
using namespace std;

namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, SwiftReaderAdapter);

SwiftReaderAdapter::SwiftReaderAdapter(network::SwiftAdminAdapterPtr adminAdapter,
                                       network::SwiftRpcChannelManagerPtr channelManager,
                                       monitor::ClientMetricsReporter *reporter)
    : _adminAdapter(adminAdapter)
    , _channelManager(channelManager)
    , _acceptTimestamp(-1)
    , _notifier(NULL)
    , _reporter(reporter) {}

SwiftReaderAdapter::~SwiftReaderAdapter() {
    if (_msgTracer) {
        _msgTracer->flush();
    }
}

ErrorCode SwiftReaderAdapter::init(const SwiftReaderConfig &config) {
    protocol::TopicInfoResponse response;
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
    //   - N、LP: connect
    //   - L: find first physic, connect
    _topicInfo = response.topicinfo();
    _readerConfig = config;
    if (TOPIC_TYPE_LOGIC == _topicInfo.topictype()) {
        if (0 == _topicInfo.physictopiclst_size()) {
            AUTIL_LOG(ERROR, "logic topic[%s] has not physic topic", _topicInfo.name().c_str());
            return ERROR_CLIENT_INIT_INVALID_PARAMETERS;
        }
        const string &physicName = _topicInfo.physictopiclst(0);
        ec = _adminAdapter->getTopicInfo(physicName, response, 0, authInfo);
        if (ERROR_NONE != ec) {
            AUTIL_LOG(ERROR, "get topic info[%s] failed", physicName.c_str());
            return ec;
        }
    } else if (TOPIC_TYPE_PHYSIC == _topicInfo.topictype()) {
        AUTIL_LOG(ERROR, "cannot read physic topic[%s]", _topicInfo.name().c_str());
        return ERROR_CLIENT_INIT_INVALID_PARAMETERS;
    }
    ec = createAndInitReaderImpl(response.topicinfo(), _swiftReaderImpl);
    if (ERROR_NONE != ec) {
        return ec;
    }
    return ERROR_NONE;
}

ErrorCode SwiftReaderAdapter::createNextPhysicReaderImpl(SwiftReaderImplPtr &retReaderImpl) {
    string nextTopic;
    ErrorCode ec = getNextPhysicTopicName(nextTopic);
    if (ERROR_NONE != ec) {
        AUTIL_LOG(ERROR,
                  "get[%s]'s next topic failed, error[%s]",
                  _swiftReaderImpl->getTopicName().c_str(),
                  ErrorCode_Name(ec).c_str());
        return ec;
    }
    TopicInfoResponse response;
    protocol::AuthenticationInfo authInfo;
    *(authInfo.mutable_accessauthinfo()) = _readerConfig.getAccessAuthInfo();
    ec = _adminAdapter->getTopicInfo(nextTopic, response, 0, authInfo);
    if (ERROR_NONE != ec) {
        AUTIL_LOG(ERROR, "get topic info[%s] failed, error[%s]", nextTopic.c_str(), ErrorCode_Name(ec).c_str());
        if (ERROR_ADMIN_TOPIC_NOT_EXISTED == ec) { // local topic info may not update
            ErrorCode updateEc = updateLogicTopicInfo();
            AUTIL_LOG(INFO,
                      "update logic topic[%s] info, error[%s]",
                      _topicInfo.name().c_str(),
                      ErrorCode_Name(updateEc).c_str());
        }
        return ec;
    }
    AUTIL_LOG(INFO, "get next physic topic[%s] response[%s]", nextTopic.c_str(), response.ShortDebugString().c_str());
    return createAndInitReaderImpl(response.topicinfo(), retReaderImpl);
}

ErrorCode SwiftReaderAdapter::createAndInitReaderImpl(const TopicInfo &tinfo, SwiftReaderImplPtr &retReaderImpl) {
    if (tinfo.needschema()) {
        retReaderImpl.reset(
            new SwiftSchemaReaderImpl(_adminAdapter, _channelManager, tinfo, _notifier, _topicInfo.name(), _reporter));
    } else {
        retReaderImpl.reset(
            new SwiftReaderImpl(_adminAdapter, _channelManager, tinfo, _notifier, _topicInfo.name(), _reporter));
    }
    retReaderImpl->setDecompressThreadPool(_decompressThreadPool);
    SwiftReaderConfig config = _readerConfig;
    config.topicName = tinfo.name();
    ErrorCode ec = ERROR_NONE;
    for (uint32_t retry = 0; retry != config.retryTimes; ++retry) {
        ec = retReaderImpl->init(config);
        if (ERROR_NONE == ec) {
            return ec;
        }
        usleep(500 * 1000);
    }
    return ec;
}

ErrorCode SwiftReaderAdapter::read(int64_t &timeStamp, protocol::Messages &msgs, int64_t timeout) {
    ErrorCode ec = readMsg(timeStamp, msgs, timeout);
    if (ec == ERROR_BROKER_SESSION_CHANGED) {
        ec = readMsg(timeStamp, msgs, timeout);
        if (ec == ERROR_BROKER_SESSION_CHANGED) {
            ec = ERROR_CLIENT_NO_MORE_MESSAGE;
        }
    }
    return ec;
}

ErrorCode SwiftReaderAdapter::read(int64_t &timeStamp, protocol::Message &msg, int64_t timeout) {
    ErrorCode ec = readMsg(timeStamp, msg, timeout);
    if (ec == ERROR_BROKER_SESSION_CHANGED) {
        ec = readMsg(timeStamp, msg, timeout);
        if (ec == ERROR_BROKER_SESSION_CHANGED) {
            ec = ERROR_CLIENT_NO_MORE_MESSAGE;
        }
    }
    return ec;
}

ErrorCode SwiftReaderAdapter::seekByTimestamp(int64_t timeStamp, bool force) {
    if (TOPIC_TYPE_LOGIC_PHYSIC == _topicInfo.topictype() || TOPIC_TYPE_LOGIC == _topicInfo.topictype()) {
        ErrorCode ec = seekPhysicTopic(timeStamp);
        AUTIL_LOG(INFO,
                  "%s seekPhysicTopic[%ld], error[%s]",
                  TopicType_Name(_topicInfo.topictype()).c_str(),
                  timeStamp,
                  ErrorCode_Name(ec).c_str());
        if (ERROR_NONE != ec) {
            AUTIL_LOG(ERROR, "seek physic topic fail[%s]", ErrorCode_Name(ec).c_str());
            return ec;
        }
    }
    SwiftReaderImplPtr reader = getSwiftReader();
    ErrorCode ec = reader->seekByTimestamp(timeStamp, force);
    AUTIL_LOG(INFO,
              "%s seek to[%ld], force[%d], error[%s]",
              reader->getTopicName().c_str(),
              timeStamp,
              force,
              ErrorCode_Name(ec).c_str());
    if (ERROR_ADMIN_TOPIC_NOT_EXISTED == ec || ERROR_CLIENT_LOGIC_TOPIC_NOT_IN_BROKER == ec) {
        ErrorCode updateEc = updateLogicTopicInfo();
        AUTIL_LOG(INFO,
                  "update logic topic[%s] info, error[%s]",
                  _topicInfo.name().c_str(),
                  ErrorCode_Name(updateEc).c_str());
    }
    return ec;
}

ErrorCode SwiftReaderAdapter::seekByProgress(const ReaderProgress &progress, bool force) {
    if (!validateReaderProgress(progress)) {
        AUTIL_LOG(ERROR, "validata reader progress failed, [%s]", progress.ShortDebugString().c_str());
        return ERROR_CLIENT_INVALID_PARAMETERS;
    }

    if (TOPIC_TYPE_LOGIC_PHYSIC == _topicInfo.topictype() || TOPIC_TYPE_LOGIC == _topicInfo.topictype()) {
        ErrorCode ec = seekPhysicTopic(progress);
        AUTIL_LOG(INFO,
                  "%s seekPhysicTopic, error[%s]",
                  TopicType_Name(_topicInfo.topictype()).c_str(),
                  ErrorCode_Name(ec).c_str());
        if (ERROR_NONE != ec) {
            AUTIL_LOG(ERROR, "seek physic topic fail[%s]", ErrorCode_Name(ec).c_str());
            return ec;
        }
    }
    SwiftReaderImplPtr reader = getSwiftReader();
    ErrorCode ec = reader->seekByProgress(progress, force);
    AUTIL_LOG(INFO,
              "%s seek[%s], force[%d], error[%s]",
              reader->getTopicName().c_str(),
              progress.ShortDebugString().c_str(),
              force,
              ErrorCode_Name(ec).c_str());
    if (ERROR_ADMIN_TOPIC_NOT_EXISTED == ec || ERROR_CLIENT_LOGIC_TOPIC_NOT_IN_BROKER == ec) {
        ErrorCode updateEc = updateLogicTopicInfo();
        AUTIL_LOG(INFO,
                  "update logic topic[%s] info, error[%s]",
                  _topicInfo.name().c_str(),
                  ErrorCode_Name(updateEc).c_str());
    }
    return ec;
}

void SwiftReaderAdapter::checkCurrentError(protocol::ErrorCode &errorCode, string &errorMsg) const {
    SwiftReaderImplPtr reader = getSwiftReader();
    reader->checkCurrentError(errorCode, errorMsg);
}

SwiftPartitionStatus SwiftReaderAdapter::getSwiftPartitionStatus() {
    SwiftReaderImplPtr reader = getSwiftReader();
    return reader->getSwiftPartitionStatus();
}

void SwiftReaderAdapter::setRequiredFieldNames(const vector<string> &fieldNames) {
    _readerConfig.requiredFields = fieldNames;
    SwiftReaderImplPtr reader = getSwiftReader();
    reader->setRequiredFieldNames(fieldNames);
}

void SwiftReaderAdapter::setFieldFilterDesc(const string &fieldFilterDesc) {
    _readerConfig.fieldFilterDesc = fieldFilterDesc;
    SwiftReaderImplPtr reader = getSwiftReader();
    reader->setFieldFilterDesc(fieldFilterDesc);
}

void SwiftReaderAdapter::setTimestampLimit(int64_t timestampLimit, int64_t &acceptTimestamp) {
    SwiftReaderImplPtr reader = getSwiftReader();
    reader->setTimestampLimit(timestampLimit, acceptTimestamp);
    _acceptTimestamp = acceptTimestamp;
}

vector<string> SwiftReaderAdapter::getRequiredFieldNames() {
    SwiftReaderImplPtr reader = getSwiftReader();
    return reader->getRequiredFieldNames();
}

bool SwiftReaderAdapter::updateCommittedCheckpoint(int64_t checkpoint) {
    SwiftReaderImplPtr reader = getSwiftReader();
    return reader->updateCommittedCheckpoint(checkpoint);
}

ErrorCode SwiftReaderAdapter::seekByMessageId(int64_t msgId) {
    SwiftReaderImplPtr reader = getSwiftReader();
    return reader->seekByMessageId(msgId);
}

int64_t SwiftReaderAdapter::getNextMsgTimestamp() const {
    SwiftReaderImplPtr reader = getSwiftReader();
    return reader->getNextMsgTimestamp();
}

int64_t SwiftReaderAdapter::getCheckpointTimestamp() const {
    SwiftReaderImplPtr reader = getSwiftReader();
    return reader->getCheckpointTimestamp();
}

int64_t SwiftReaderAdapter::getMaxLastMsgTimestamp() const {
    SwiftReaderImplPtr reader = getSwiftReader();
    return reader->getMaxLastMsgTimestamp();
}
void SwiftReaderAdapter::setDecompressThreadPool(const autil::ThreadPoolPtr &decompressThreadPool) {
    _decompressThreadPool = decompressThreadPool;
    if (_swiftReaderImpl) {
        _swiftReaderImpl->setDecompressThreadPool(_decompressThreadPool);
    }
}

void SwiftReaderAdapter::setSwiftReader(SwiftReaderImplPtr swiftReader) {
    ScopedWriteLock lock(_readerLock);
    _swiftReaderImpl = swiftReader;
}

void SwiftReaderAdapter::setMessageTracer(ReadMessageTracerPtr msgTracer) { _msgTracer = msgTracer; }

SwiftReaderImplPtr SwiftReaderAdapter::getSwiftReader() const {
    ScopedReadLock lock(_readerLock);
    return _swiftReaderImpl;
}

common::SchemaReader *SwiftReaderAdapter::getSchemaReader(const char *data, protocol::ErrorCode &ec) {
    SwiftReaderImplPtr reader = getSwiftReader();
    return reader->getSchemaReader(data, ec);
}

ErrorCode SwiftReaderAdapter::getReaderProgress(ReaderProgress &progress) const {
    SwiftReaderImplPtr reader = getSwiftReader();
    return reader->getReaderProgress(progress);
}

ErrorCode SwiftReaderAdapter::doTopicChanged() {
    ScopedLock lock(_mutex);
    SwiftReaderImplPtr reader = getSwiftReader();
    if (!reader->isTopicChanged()) {
        return ERROR_NONE;
    }
    const string &topicName = reader->getTopicName();
    AUTIL_LOG(INFO, "topic[%s] do topic change", topicName.c_str());
    protocol::TopicInfoResponse response;
    protocol::AuthenticationInfo authInfo;
    *(authInfo.mutable_accessauthinfo()) = _readerConfig.getAccessAuthInfo();
    ErrorCode ec = _adminAdapter->getTopicInfo(topicName, response, reader->getTopicVersion(), authInfo);
    if (ERROR_NONE != ec) {
        AUTIL_LOG(ERROR, "get topic info[%s] failed", topicName.c_str());
        if (ERROR_ADMIN_TOPIC_NOT_EXISTED == ec // P -> P
            && TOPIC_TYPE_LOGIC == _topicInfo.topictype() && topicName != _topicInfo.name()) {
            AUTIL_LOG(INFO, "%s not exist, need switch to next", topicName.c_str());
            if (!doSwitch()) {
                AUTIL_LOG(ERROR, "%s switch to next fail", topicName.c_str());
                return ERROR_CLIENT_PHYSIC_TOPIC_SWITCH_NOT_READY;
            }
            ec = ERROR_NONE;
        }
        return ec;
    }
    if (topicName == _topicInfo.name()) {
        _topicInfo = response.topicinfo();
        if (TOPIC_TYPE_LOGIC == _topicInfo.topictype()) { // LP -> P
            AUTIL_LOG(INFO, "%s is logic topic, need switch to physic", _topicInfo.name().c_str());
            if (!doSwitch()) {
                AUTIL_LOG(ERROR, "%s switch to physic fail", _topicInfo.name().c_str());
                return ERROR_CLIENT_PHYSIC_TOPIC_SWITCH_NOT_READY;
            }
            return ERROR_NONE;
        }
    }
    const TopicInfo &topicInfo = response.topicinfo();
    AUTIL_LOG(INFO, "topic[%s] change[%s]", topicInfo.name().c_str(), topicInfo.ShortDebugString().c_str());
    if (needResetReader(topicInfo, reader->getPartitionCount())) {
        SwiftReaderImplPtr newReaderImpl;
        ErrorCode ec = createAndInitReaderImpl(topicInfo, newReaderImpl);
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create readerImpl failed [%s]", ErrorCode_Name(ec).c_str());
            return ec;
        }
        if (_acceptTimestamp != -1) {
            int64_t accectTimestamp;
            newReaderImpl->setTimestampLimit(_acceptTimestamp, accectTimestamp);
        }
        ec = reseekNewReader(reader, newReaderImpl);
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "reseek new reader failed, error [%s], sleep 1s, retrying", ErrorCode_Name(ec).c_str());
            usleep(1000 * 1000);
            return ec;
        }
        setSwiftReader(newReaderImpl);
    } else {
        AUTIL_LOG(INFO, "reset %s topic change", reader->getTopicName().c_str());
        reader->setTopicLongPollingEnabled(topicInfo.enablelongpolling());
        reader->resetTopicChanged();
    }

    return ERROR_NONE;
}

bool SwiftReaderAdapter::needResetReader(const TopicInfo &topicInfo, uint32_t partCnt) {
    if (TOPIC_TYPE_LOGIC == topicInfo.topictype()) {
        return false;
    }
    if (topicInfo.partitioncount() != partCnt) {
        return true;
    }
    return false;
}

ErrorCode SwiftReaderAdapter::reseekNewReader(SwiftReaderImplPtr rawReader, SwiftReaderImplPtr newReader) {
    ErrorCode ec = ERROR_UNKNOWN;
    if (newReader->getPartitionCount() != rawReader->getPartitionCount()) {
        int64_t timestamp = rawReader->getCheckpointTimestamp();
        AUTIL_LOG(INFO, "seek %s to %ld", _readerConfig.topicName.c_str(), timestamp);
        ec = newReader->seekByTimestamp(timestamp);
    } else {
        ReaderProgress progress;
        ec = rawReader->getReaderProgress(progress);
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "get reader progress failed.");
            return ec;
        }
        AUTIL_LOG(INFO, "seek %s to %s", _readerConfig.topicName.c_str(), progress.ShortDebugString().c_str());
        ec = newReader->seekByProgress(progress, true);
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "newReader seekByProgress [%s] failed.", progress.ShortDebugString().c_str());
            return ec;
        }
    }
    return ec;
}

ErrorCode SwiftReaderAdapter::getSchema(int32_t version, int32_t &retVersion, string &schema) {
    SwiftReaderImplPtr reader = getSwiftReader();
    SwiftSchemaReaderImpl *scmReader = dynamic_cast<SwiftSchemaReaderImpl *>(reader.get());
    if (NULL == scmReader) {
        AUTIL_LOG(ERROR, "topic not support schema");
        return ERROR_UNKNOWN;
    }

    return scmReader->getSchema(version, retVersion, schema);
}

const SwiftReaderConfig &SwiftReaderAdapter::getReaderConfig() const { return _readerConfig; }

void SwiftReaderAdapter::setNotifier(Notifier *notifier) { _notifier = notifier; }

bool SwiftReaderAdapter::doSwitch() {
    ScopedWriteLock lock(_readerLock);
    // 1. create new readerImpl
    SwiftReaderImplPtr nextReaderImpl;
    ErrorCode ec = createNextPhysicReaderImpl(nextReaderImpl);
    if (ERROR_NONE != ec) {
        AUTIL_LOG(ERROR,
                  "create[%s]'s next physic topic readerImpl fail, error[%s]",
                  _swiftReaderImpl->getTopicName().c_str(),
                  ErrorCode_Name(ec).c_str());
        return false;
    }
    AUTIL_LOG(INFO,
              "switch %s[%s -> %s]",
              _topicInfo.name().c_str(),
              _swiftReaderImpl->getTopicName().c_str(),
              nextReaderImpl->getTopicName().c_str());
    // 2. delete and reset readerImpl
    _swiftReaderImpl = nextReaderImpl;
    // finish switch
    AUTIL_LOG(INFO, "switch %s to[%s] success", _topicInfo.name().c_str(), _swiftReaderImpl->getTopicName().c_str());
    return true;
}

ErrorCode SwiftReaderAdapter::updateLogicTopicInfo() {
    TopicInfoResponse response;
    protocol::AuthenticationInfo authInfo;
    *(authInfo.mutable_accessauthinfo()) = _readerConfig.getAccessAuthInfo();
    ErrorCode ec = _adminAdapter->getTopicInfo(_topicInfo.name(), response, 0, authInfo);
    if (ERROR_NONE != ec) {
        AUTIL_LOG(ERROR, "get topic info[%s] failed", _topicInfo.name().c_str());
        return ec;
    }
    AUTIL_LOG(INFO,
              "update logic topic[%s] info[%s]",
              _topicInfo.name().c_str(),
              ProtoUtil::plainDiffStr(&_topicInfo, &response.topicinfo()).c_str());
    _topicInfo = response.topicinfo();
    return ec;
}

ErrorCode SwiftReaderAdapter::getNextPhysicTopicName(string &topicName) {
    if (doGetNextPhysicTopicName(topicName)) {
        return ERROR_NONE;
    }
    ErrorCode ec = updateLogicTopicInfo();
    if (ERROR_NONE != ec) {
        return ec;
    }
    if (doGetNextPhysicTopicName(topicName)) {
        return ERROR_NONE;
    } else {
        return ERROR_CLIENT_PHYSIC_TOPIC_SWITCH_NOT_READY;
    }
}

bool SwiftReaderAdapter::doGetNextPhysicTopicName(string &topicName) {
    const string &curTopicName = _swiftReaderImpl->getTopicName();
    int physicTopicCnt = _topicInfo.physictopiclst_size();
    if (_topicInfo.topictype() == TOPIC_TYPE_LOGIC_PHYSIC && _topicInfo.name() == curTopicName && physicTopicCnt > 0) {
        topicName = _topicInfo.physictopiclst(0);
        return true;
    }
    for (int i = 0; i < physicTopicCnt; ++i) {
        if (_topicInfo.physictopiclst(i) == curTopicName) {
            if (i != physicTopicCnt - 1) {
                topicName = _topicInfo.physictopiclst(i + 1);
                return true;
            } else {
                return false;
            }
        }
    }
    if (0 == _topicInfo.physictopiclst_size()) {
        AUTIL_LOG(ERROR, "logic[%s] has no physic topic", _topicInfo.ShortDebugString().c_str());
        return false;
    }
    string physicLstStr;
    for (int i = 0; i < _topicInfo.physictopiclst_size(); i++) {
        physicLstStr += _topicInfo.physictopiclst(i) + ",";
    }
    AUTIL_LOG(INFO,
              "%s not exist in %s's physicLst[%s], set next physic[%s]",
              curTopicName.c_str(),
              _topicInfo.name().c_str(),
              physicLstStr.c_str(),
              _topicInfo.physictopiclst(0).c_str());
    topicName = _topicInfo.physictopiclst(0);
    return true;
}

ErrorCode SwiftReaderAdapter::seekPhysicTopic(int64_t timestamp) {
    string physicTopic;
    ErrorCode ec = findPhysicName(timestamp, physicTopic);
    if (ERROR_NONE != ec) {
        AUTIL_LOG(ERROR, "seek physic topic fail[%s]", ErrorCode_Name(ec).c_str());
        return ec;
    }
    AUTIL_LOG(INFO, "seek to physic topic[%s] success", physicTopic.c_str());
    SwiftReaderImplPtr reader = getSwiftReader();
    if (physicTopic == reader->getTopicName()) {
        return ERROR_NONE;
    } else {
        return updateReaderImpl(physicTopic);
    }
}

ErrorCode SwiftReaderAdapter::seekPhysicTopic(const ReaderProgress &progress) {
    const TopicReaderProgress &topicProgress = progress.topicprogress(0);
    int64_t oldestTimestamp = 0;
    if (topicProgress.partprogress_size() > 0) {
        oldestTimestamp = topicProgress.partprogress(0).timestamp();
        for (int idx = 1; idx < topicProgress.partprogress_size(); ++idx) {
            if (topicProgress.partprogress(idx).timestamp() < oldestTimestamp) {
                oldestTimestamp = topicProgress.partprogress(idx).timestamp();
            }
        }
    }
    return seekPhysicTopic(oldestTimestamp);
}

ErrorCode SwiftReaderAdapter::findPhysicName(int64_t timestamp, string &physicName) {
    bool isLastPhysic;
    ErrorCode ec = doFindPhysicName(timestamp, physicName, isLastPhysic);
    if (ERROR_NONE == ec && !isLastPhysic) {
        return ERROR_NONE;
    }
    ec = updateLogicTopicInfo();
    if (ERROR_NONE != ec) {
        AUTIL_LOG(ERROR, "update topic[%s] info failed", _topicInfo.name().c_str());
        return ec;
    }
    return doFindPhysicName(timestamp, physicName, isLastPhysic);
}

ErrorCode SwiftReaderAdapter::doFindPhysicName(int64_t timestamp, string &physicName, bool &isLastPhysic) {
    isLastPhysic = false;
    physicName = (TOPIC_TYPE_LOGIC_PHYSIC == _topicInfo.topictype()) ? _topicInfo.name() : _topicInfo.physictopiclst(0);
    for (int idx = 0; idx < _topicInfo.physictopiclst_size(); ++idx) {
        const string &physic = _topicInfo.physictopiclst(idx);
        string logic;
        int64_t ts;
        uint32_t partCnt;
        if (!LogicTopicHelper::parsePhysicTopicName(physic, logic, ts, partCnt)) {
            AUTIL_LOG(ERROR, "physic topic[%s] invalid", physic.c_str());
            return ERROR_CLIENT_INVALID_PARAMETERS;
        }
        if (ts <= timestamp) {
            physicName = physic;
        } else {
            return ERROR_NONE;
        }
    }
    isLastPhysic = true;
    return ERROR_NONE;
}

ErrorCode SwiftReaderAdapter::updateReaderImpl(const string &topic) {
    TopicInfoResponse response;
    protocol::AuthenticationInfo authInfo;
    *(authInfo.mutable_accessauthinfo()) = _readerConfig.getAccessAuthInfo();
    ErrorCode ec = _adminAdapter->getTopicInfo(topic, response, 0, authInfo);
    if (ERROR_NONE != ec) {
        AUTIL_LOG(ERROR, "get topic info[%s] failed, error[%s]", topic.c_str(), ErrorCode_Name(ec).c_str());
        if (ERROR_ADMIN_TOPIC_NOT_EXISTED == ec) {
            ErrorCode updateEc = updateLogicTopicInfo();
            AUTIL_LOG(INFO,
                      "update logic topic[%s] info, error[%s]",
                      _topicInfo.name().c_str(),
                      ErrorCode_Name(updateEc).c_str());
        }
        return ec;
    }
    SwiftReaderImplPtr newReaderImpl;
    ec = createAndInitReaderImpl(response.topicinfo(), newReaderImpl);
    if (ERROR_NONE != ec) {
        return ec;
    }
    setSwiftReader(newReaderImpl);
    return ERROR_NONE;
}

bool SwiftReaderAdapter::checkAuthority(const protocol::TopicInfo &ti) {
    if (!ti.has_opcontrol()) {
        return true;
    }
    return ti.opcontrol().canread();
}

bool SwiftReaderAdapter::validateReaderProgress(const ReaderProgress &readerProgress) {
    if (readerProgress.topicprogress_size() != 1) {
        AUTIL_LOG(WARN, "reader progress size not equal 1");
        return false;
    }
    const auto &topicProgress = readerProgress.topicprogress(0);
    if (topicProgress.topicname() == _readerConfig.topicName &&
        topicProgress.uint8filtermask() == _readerConfig.swiftFilter.uint8filtermask() &&
        topicProgress.uint8maskresult() == _readerConfig.swiftFilter.uint8maskresult()) {
        return true;
    } else {
        AUTIL_LOG(WARN,
                  "reader progress not equal, expect topic name [%s] filter mask [%d %d]",
                  _readerConfig.topicName.c_str(),
                  _readerConfig.swiftFilter.uint8filtermask(),
                  _readerConfig.swiftFilter.uint8maskresult());
        return false;
    }
}

} // namespace client
} // namespace swift
