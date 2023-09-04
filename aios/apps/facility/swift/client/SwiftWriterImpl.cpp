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
#include "swift/client/SwiftWriterImpl.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>

#include "autil/HashFuncFactory.h" // IWYU pragma: keep
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "swift/client/SingleSwiftWriter.h"
#include "swift/client/SwiftClientConfig.h"
#include "swift/common/RangeUtil.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/Block.h"

namespace swift {
namespace common {
class MessageInfo;
} // namespace common
} // namespace swift

using namespace std;
using namespace autil;
using namespace swift::protocol;
using namespace swift::common;
using namespace swift::util;
using namespace swift::network;
namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, SwiftWriterImpl);

const std::string SwiftWriterImpl::functionSeperator = ",";
const std::string SwiftWriterImpl::functionParamSeperator = ":";
const std::string SwiftWriterImpl::funcHashId2Pid = "hashId2partId";
const std::string SwiftWriterImpl::funcHashIdAsPid = "hashIdAspartId";
const std::string SwiftWriterImpl::funcHashStr2HashId_HashFuncPrefix = "hashFunction:";
const std::string SwiftWriterImpl::funcHashStr2HashId_DefaultHash = "HASH";
const std::string SwiftWriterImpl::funcHashStr2HashId_GalaxyHash = "GALAXY_HASH";
const std::string SwiftWriterImpl::funcHashStr2HashId_Hash64 = "HASH64";
SwiftWriterImpl::SwiftWriterImpl(SwiftAdminAdapterPtr adminAdapter,
                                 SwiftRpcChannelManagerPtr channelManager,
                                 autil::ThreadPoolPtr mergeThreadPool,
                                 const TopicInfo &topicInfo,
                                 BufferSizeLimiterPtr limiter,
                                 std::string logicTopicName,
                                 monitor::ClientMetricsReporter *reporter)
    : _adminAdapter(adminAdapter)
    , _channelManager(channelManager)
    , _partitionCount(topicInfo.partitioncount())
    , _rangeCountInPartition(topicInfo.rangecountinpartition())
    , _topicChanged(false)
    , _mergeThreadPool(mergeThreadPool)
    , _limiter(limiter)
    , _logicTopicName(logicTopicName)
    , _topicVersion(topicInfo.has_modifytime() ? topicInfo.modifytime() : -1)
    , _committedFunc(NULL)
    , _errorFunc(NULL)
    , _versionControlPid(-1)
    , _reporter(reporter) {
    _sessionIds.resize(_partitionCount, -1);
    for (int i = 0; i < topicInfo.partitioninfos_size(); ++i) {
        _sessionIds[i] = topicInfo.partitioninfos(i).sessionid();
    }
    clear();
}

SwiftWriterImpl::SwiftWriterImpl(const char *hashFunc, uint32_t topicPartCnt) {
    _config.functionChain = hashFunc;
    _rangeUtil.reset(new RangeUtil(topicPartCnt));
}

SwiftWriterImpl::~SwiftWriterImpl() { clear(); }

ErrorCode SwiftWriterImpl::init(const SwiftWriterConfig &config) { return init(config, BlockPoolPtr()); }

ErrorCode SwiftWriterImpl::init(const SwiftWriterConfig &config, BlockPoolPtr blockPool) {
    setWaitLoopInterval(config.waitLoopInterval);
    _config = config;
    if (_config.mergeMsg) {
        if (0 != (_partitionCount & (_partitionCount - 1)) ||
            0 != (_rangeCountInPartition & (_rangeCountInPartition - 1))) {
            AUTIL_LOG(WARN,
                      "topic [%s], partitionCount[%d] or rangeCountInPartition[%d]"
                      " is not 2^n, close mergeMessage function",
                      _config.topicName.c_str(),
                      _partitionCount,
                      _rangeCountInPartition);
            _config.mergeMsg = false;
        }
    }
    _rangeUtil.reset(new RangeUtil(_partitionCount, _rangeCountInPartition));
    if (!initProcessFuncs()) {
        return ERROR_CLIENT_INIT_FAILED;
    }
    if (blockPool == NULL) {
        _blockPool.reset(new BlockPool(_config.maxBufferSize, SwiftClientConfig::DEFAULT_BUFFER_BLOCK_SIZE_KB * 1024));
    } else {
        int64_t bufferSize = _config.maxBufferSize;
        if (bufferSize > blockPool->getBlockSize() * blockPool->getMaxBlockCount()) {
            bufferSize = blockPool->getBlockSize() * blockPool->getMaxBlockCount();
        }
        _blockPool.reset(new BlockPool(blockPool.get(), bufferSize, 0));
    }
    return ERROR_NONE;
}

void SwiftWriterImpl::getWriterMetrics(const string &zkPath, const string &topicName, WriterMetrics &writerMetric) {
    if (_config.zkPath != zkPath || _config.topicName != topicName) {
        return;
    }
    ScopedReadLock lock(_mapLock);
    map<uint32_t, SingleSwiftWriter *>::iterator iter = _writers.begin();
    for (; iter != _writers.end(); iter++) {
        writerMetric.unsendMsgCount += iter->second->getUnsendMessageCount();
        writerMetric.unsendMsgSize += iter->second->getUnsendMessageSize();
        writerMetric.uncommittedMsgCount += iter->second->getUncommittedMsgCount();
        writerMetric.uncommittedMsgSize += iter->second->getUncommittedMsgSize();
    }
}

bool SwiftWriterImpl::isSyncWriter() const { return _config.isSynchronous; }

bool SwiftWriterImpl::clearBuffer(int64_t &cpBeg, int64_t &cpEnd) {
    int64_t maxNotDoneCheckpoint = -1;
    cpBeg = getCommittedCheckpointId();
    ScopedReadLock lock(_mapLock);
    for (map<uint32_t, SingleSwiftWriter *>::iterator it = _writers.begin(); it != _writers.end(); ++it) {
        SingleSwiftWriter *singleWriter = it->second;
        int64_t partCheckpointId;
        if (!singleWriter->getMaxCheckpointId(partCheckpointId)) {
            continue;
        }
        singleWriter->clearBuffer();
        if (partCheckpointId > maxNotDoneCheckpoint) {
            maxNotDoneCheckpoint = partCheckpointId;
        }
    }
    cpEnd = maxNotDoneCheckpoint;
    return maxNotDoneCheckpoint != -1;
}

bool SwiftWriterImpl::stealUnsendMessage(std::vector<common::MessageInfo> &msgInfoVec) {
    ScopedReadLock lock(_mapLock);
    for (map<uint32_t, SingleSwiftWriter *>::iterator it = _writers.begin(); it != _writers.end(); ++it) {
        SingleSwiftWriter *singleWriter = it->second;
        if (!singleWriter->stealUnsendMessage(msgInfoVec)) {
            AUTIL_LOG(ERROR, "steal single writer pid[%d] unsend message failed", it->first);
            return false;
        }
    }
    return true;
}

// to do: construct processor chain, if needed
bool SwiftWriterImpl::initProcessFuncs() {
    string functionChain = _config.functionChain;
    if (functionChain.empty()) {
        return true;
    }
    vector<string> funcs = autil::StringUtil::split(functionChain, functionSeperator);
    for (size_t i = 0; i < funcs.size(); ++i) {
        if (funcHashId2Pid == funcs[i]) {
            _processFuncs.push_back((ProcessFunc)(&SwiftWriterImpl::processHashId2PartId));
        } else if (funcHashIdAsPid == funcs[i]) {
            _processFuncs.push_back((ProcessFunc)(&SwiftWriterImpl::processHashIdAsPartId));
        } else {
            string hashFuncStr = funcs[i];
            if (_hashFunctionBasePtr == NULL) {
                HashFunctionBasePtr funcPtr = createHashFunc(hashFuncStr);
                if (funcPtr == NULL) {
                    AUTIL_LOG(WARN,
                              "topic [%s] create hash function failed, function name:%s.",
                              _config.topicName.c_str(),
                              hashFuncStr.c_str());
                    return false;
                }
                _hashFunctionBasePtr = funcPtr;
                _hashFuncStr = hashFuncStr;
                _processFuncs.push_back((ProcessFunc)(&SwiftWriterImpl::processHashStr));
            }
        }
    }
    return true;
}

HashFunctionBasePtr SwiftWriterImpl::createHashFunc(const string &hashFuncStr) {
    string funcStr = hashFuncStr;
    if (0 == hashFuncStr.find(funcHashStr2HashId_HashFuncPrefix)) {
        funcStr = hashFuncStr.substr(funcHashStr2HashId_HashFuncPrefix.length());
    }
    string hashFuncName;
    uint32_t partitionCnt;
    if (!parseFunctionParam(funcStr, hashFuncName, partitionCnt)) {
        AUTIL_LOG(WARN, "topic [%s] parse function param error:[%s].", _config.topicName.c_str(), funcStr.c_str());
        return HashFunctionBasePtr();
    }
    if (hashFuncName.empty()) {
        AUTIL_LOG(WARN, "topic [%s] cannot recognize function %s.", _config.topicName.c_str(), funcStr.c_str());
        return HashFunctionBasePtr();
    }
    if (partitionCnt != 0) {
        return HashFuncFactory::createHashFunc(hashFuncName, partitionCnt);
    } else {
        return HashFuncFactory::createHashFunc(hashFuncName);
    }
}

bool SwiftWriterImpl::parseFunctionParam(const string &funStr, string &funName, uint32_t &partCnt) {
    vector<string> params = autil::StringUtil::split(funStr, functionParamSeperator);
    if (params.size() == 0) {
        return false;
    } else if (params.size() == 1) {
        funName = params[0];
        partCnt = 0;
    } else {
        funName = params[0];
        uint32_t partitionCnt;
        if (!autil::StringUtil::fromString<uint32_t>(params[1], partitionCnt)) {
            AUTIL_LOG(WARN, "topic [%s] parse partition count error.", _config.topicName.c_str());
            return false;
        }
        uint32_t partitionCount = getPartitionCount();
        if (partitionCnt <= 0 || partitionCnt > partitionCount) {
            AUTIL_LOG(INFO,
                      "topic [%s] partition cnt should small than real partition cnt: %d,"
                      " now use real paratition.",
                      _config.topicName.c_str(),
                      partitionCount);
            partCnt = partitionCount;
        } else {
            partCnt = partitionCnt;
        }
    }
    return true;
}

ErrorCode SwiftWriterImpl::validateMessageInfo(const MessageInfo &msgInfo) {
    if (msgInfo.compress) {
        AUTIL_LOG(WARN, "topic [%s] msg is compressed.", _config.topicName.c_str());
        return ERROR_CLIENT_INVALID_PARAMETERS;
    }
    if (msgInfo.isMerged) {
        AUTIL_LOG(WARN, "topic [%s] msg is merged.", _config.topicName.c_str());
        return ERROR_CLIENT_INVALID_PARAMETERS;
    }
    if (msgInfo.checkpointId < -1) {
        AUTIL_LOG(WARN,
                  "topic [%s] msg checkpointId[%ld] is less than zero.",
                  _config.topicName.c_str(),
                  msgInfo.checkpointId);
        return ERROR_CLIENT_INVALID_PARAMETERS;
    }
    uint32_t partitionCount = getPartitionCount();
    if (msgInfo.pid >= partitionCount) {
        AUTIL_LOG(WARN,
                  "topic [%s] msg pid[%d] is greater than partitionCount[%d].",
                  _config.topicName.c_str(),
                  msgInfo.pid,
                  partitionCount);
        return ERROR_CLIENT_INVALID_PARTITIONID;
    }
    if (-1 != _versionControlPid && _versionControlPid != msgInfo.pid) {
        AUTIL_LOG(WARN,
                  "topic[%s] msg pid[%d] not equal to version control pid[%ld]",
                  _config.topicName.c_str(),
                  msgInfo.pid,
                  _versionControlPid);
        return ERROR_CLIENT_INVALID_PARTITIONID;
    }
    return ERROR_NONE;
}

pair<int32_t, uint16_t>
SwiftWriterImpl::getPartitionIdByHashStr(const string &zkPath, const string &topicName, const string &hashStr) {
    if (_config.zkPath != zkPath || _config.topicName != topicName) {
        return pair<int32_t, uint16_t>(-1, 0);
    }
    if (_processFuncs.size() == 0) {
        return pair<int32_t, uint16_t>(-1, 0);
    }
    MessageInfo messageInfo;
    messageInfo.hashStr = hashStr;
    ErrorCode ec = processMsgInfo(messageInfo);
    if (ERROR_NONE != ec) {
        AUTIL_LOG(INFO,
                  "topic [%s] get partition id from hash string [%s] failed.",
                  _config.topicName.c_str(),
                  hashStr.c_str());
        return pair<int32_t, uint16_t>(-1, 0);
    } else {
        return pair<int32_t, uint16_t>((int32_t)messageInfo.pid, messageInfo.uint16Payload);
    }
}

pair<int32_t, uint16_t> SwiftWriterImpl::getPartitionIdByHashStr(const string &hashStr) {
    if (_processFuncs.size() == 0) {
        return pair<int32_t, uint16_t>(-1, 0);
    }
    MessageInfo messageInfo;
    messageInfo.hashStr = hashStr;
    ErrorCode ec = processMsgInfo(messageInfo);
    if (ERROR_NONE != ec) {
        AUTIL_LOG(INFO,
                  "topic [%s] get partition id from hash string [%s] failed.",
                  _config.topicName.c_str(),
                  hashStr.c_str());
        return pair<int32_t, uint16_t>(-1, 0);
    } else {
        return pair<int32_t, uint16_t>((int32_t)messageInfo.pid, messageInfo.uint16Payload);
    }
}

ErrorCode SwiftWriterImpl::processMsgInfo(MessageInfo &msgInfo) {
    for (vector<ProcessFunc>::iterator it = _processFuncs.begin(); it != _processFuncs.end(); ++it) {
        bool re = (this->**it)(msgInfo);
        if (!re) {
            return ERROR_CLIENT_INVALID_PARAMETERS;
        }
    }

    return ERROR_NONE;
}

ErrorCode SwiftWriterImpl::write(MessageInfo &msgInfo) {
    ErrorCode ec = writeMsg(msgInfo);
    if (ec == ERROR_BROKER_SESSION_CHANGED) {
        ec = ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND;
    }
    return ec;
}

ErrorCode SwiftWriterImpl::writeMsg(MessageInfo &msgInfo) {
    ErrorCode ec = processMsgInfo(msgInfo);
    if (ERROR_NONE != ec) {
        return ec;
    }
    ec = validateMessageInfo(msgInfo);
    if (ERROR_NONE != ec) {
        return ec;
    }
    SingleSwiftWriter *writer = getSingleWriter(msgInfo.pid);
    if (nullptr == writer) {
        AUTIL_LOG(ERROR, "getSingleWriter fail for pid[%u]", msgInfo.pid);
        return ERROR_CLIENT_INVALID_PARTITIONID;
    }
    ec = writer->write(msgInfo, _config.isSynchronous);
    if (_config.isSynchronous && ERROR_NONE == ec) {
        int64_t checkpoint;
        writer->getCommittedCheckpointId(checkpoint);
        setCheckpointId(checkpoint);
    }
    return ec;
}

// msgInfo has payload but no hashStr
ErrorCode SwiftWriterImpl::importMsg(MessageInfo &msgInfo) {
    if (!processHashId2PartId(msgInfo)) {
        AUTIL_LOG(ERROR, "processHashId2PartId[payload:%d] fail", msgInfo.uint16Payload);
        return ERROR_CLIENT_INVALID_PARAMETERS;
    }
    SingleSwiftWriter *writer = getSingleWriter(msgInfo.pid);
    if (nullptr == writer) {
        AUTIL_LOG(ERROR, "getSingleWriter fail for pid[%u]", msgInfo.pid);
        return ERROR_CLIENT_INVALID_PARTITIONID;
    }
    ErrorCode ec = writer->write(msgInfo, _config.isSynchronous);
    if (_config.isSynchronous && ERROR_NONE == ec) {
        int64_t checkpoint;
        writer->getCommittedCheckpointId(checkpoint);
        setCheckpointId(checkpoint);
    }
    return ec;
}

bool SwiftWriterImpl::processHashId2PartId(MessageInfo &msgInfo) {
    int32_t pid = -1;
    {
        ScopedReadLock lock(_rangeUtilLock);
        pid = _rangeUtil->getPartitionId(msgInfo.uint16Payload);
    }
    if (-1 == pid) {
        AUTIL_LOG(ERROR,
                  "topic[%s] get partition id failed hash id is :%d",
                  _config.topicName.c_str(),
                  msgInfo.uint16Payload);
        return false;
    }
    msgInfo.pid = pid;
    return true;
}

bool SwiftWriterImpl::processHashIdAsPartId(MessageInfo &msgInfo) {
    uint16_t hashId = msgInfo.uint16Payload;
    uint32_t partitionCount = getPartitionCount();
    if (hashId >= partitionCount) {
        AUTIL_LOG(WARN,
                  "topic [%s] set partition id failed, hash id is :%d,"
                  "partition count is:%d",
                  _config.topicName.c_str(),
                  hashId,
                  partitionCount);
        return false;
    }
    msgInfo.pid = hashId;
    return true;
}

bool SwiftWriterImpl::processHashStr(MessageInfo &msgInfo) {
    assert(_hashFunctionBasePtr);
    ScopedReadLock lock(_hashFuncLock);
    msgInfo.uint16Payload = _hashFunctionBasePtr->getHashId(msgInfo.hashStr);
    return true;
}

void SwiftWriterImpl::clear() {
    ScopedWriteLock lock(_mapLock);
    _checkpointId = -1;
    _lastErrorCode = ERROR_NONE;
    for (std::map<uint32_t, SingleSwiftWriter *>::iterator it = _writers.begin(); it != _writers.end(); ++it) {
        delete it->second;
    }
    _writers.clear();
}

bool SwiftWriterImpl::isFinished() {
    ScopedReadLock lock(_mapLock);
    for (map<uint32_t, SingleSwiftWriter *>::iterator it = _writers.begin(); it != _writers.end(); ++it) {
        SingleSwiftWriter *singleWriter = it->second;
        assert(singleWriter);
        if (!singleWriter->isBufferEmpty()) {
            return false;
        }
    }
    return true;
}

bool SwiftWriterImpl::isAllSent() {
    ScopedReadLock lock(_mapLock);
    for (map<uint32_t, SingleSwiftWriter *>::iterator it = _writers.begin(); it != _writers.end(); ++it) {
        SingleSwiftWriter *singleWriter = it->second;
        assert(singleWriter);
        if (!singleWriter->isSent()) {
            return false;
        }
    }
    return true;
}

void SwiftWriterImpl::sendRequest(bool &hasSealError) {
    int64_t now = autil::TimeUtility::currentTime();
    int64_t minNotDoneCheckpoint = numeric_limits<int64_t>::max();
    int64_t maxDoneCheckpoint = -1;
    ScopedReadLock lock(_mapLock);
    bool force = _forceSend;
    hasSealError = false;
    for (map<uint32_t, SingleSwiftWriter *>::iterator it = _writers.begin(); it != _writers.end(); ++it) {
        SingleSwiftWriter *singleWriter = it->second;
        assert(singleWriter);
        ErrorCode ec = singleWriter->sendRequest(now, force);
        if (ERROR_TOPIC_SEALED == ec) {
            hasSealError = true;
        }
        int64_t partCheckpointId;
        bool hasMsg = singleWriter->getCommittedCheckpointId(partCheckpointId);
        if (partCheckpointId == SingleSwiftWriter::INVALID_CHECK_POINT_ID) {
            continue;
        }
        if (hasMsg) {
            minNotDoneCheckpoint = min(minNotDoneCheckpoint, partCheckpointId);
        } else {
            maxDoneCheckpoint = max(maxDoneCheckpoint, partCheckpointId);
        }
    }
    if (minNotDoneCheckpoint != numeric_limits<int64_t>::max()) {
        setCheckpointId(minNotDoneCheckpoint);
    } else {
        setCheckpointId(maxDoneCheckpoint);
    }
}

void SwiftWriterImpl::doTopicChanged(uint32_t partCnt, uint32_t rangeCntInPart) {
    if (!_topicChanged) {
        return;
    }
    bool partCntChange = false;
    if (_partitionCount != partCnt) {
        AUTIL_LOG(INFO,
                  "writer topic[%s] changed, partition count[%d -> %d]",
                  _config.topicName.c_str(),
                  _partitionCount,
                  partCnt);
        partCntChange = true;
        _partitionCount = partCnt;
        _sessionIds.resize(_partitionCount, -1);
    }
    if (_rangeCountInPartition != rangeCntInPart) {
        AUTIL_LOG(INFO,
                  "writer topic[%s] changed, partition range count[%d -> %d]",
                  _config.topicName.c_str(),
                  _rangeCountInPartition,
                  rangeCntInPart);
        partCntChange = true;
        _rangeCountInPartition = rangeCntInPart;
    }
    if (!partCntChange) {
        _topicChanged = false;
        return;
    }
    AUTIL_LOG(INFO, "do topic[%s] change, hash str[%s]", _config.topicName.c_str(), _hashFuncStr.c_str());
    {
        ScopedWriteLock lock(_hashFuncLock);
        HashFunctionBasePtr funcPtr = createHashFunc(_hashFuncStr);
        if (funcPtr) {
            _hashFunctionBasePtr = funcPtr;
        }
    }
    {
        ScopedWriteLock lock(_mapLock);
        {
            ScopedWriteLock lock(_rangeUtilLock);
            _rangeUtil.reset(new RangeUtil(_partitionCount, _rangeCountInPartition));
        }
        map<uint32_t, SingleSwiftWriter *>::iterator it = _writers.begin();
        for (; it != _writers.end();) {
            if (it->first >= _partitionCount) {
                delete it->second;
                _writers.erase(it++);
            } else {
                it->second->updateRangeUtil(_rangeUtil);
                it++;
            }
        }
    }
    _topicChanged = false;
}

int64_t SwiftWriterImpl::getCommittedCheckpointId() const {
    ScopedLock lock(_checkpointMutex);
    return _checkpointId;
}

void SwiftWriterImpl::setCheckpointId(int64_t checkpointId) {
    ScopedLock lock(_checkpointMutex);
    _checkpointId = checkpointId;
}

SingleSwiftWriter *SwiftWriterImpl::insertSingleWriter(uint32_t pid) {
    ScopedWriteLock lock(_mapLock);
    if (pid >= _partitionCount) {
        AUTIL_LOG(ERROR, "pid[%u] invalid, must less than partcount[%u]", pid, _partitionCount);
        return nullptr;
    }
    // double check exist for thread safe.
    std::map<uint32_t, SingleSwiftWriter *>::const_iterator it = _writers.find(pid);
    if (it != _writers.end()) {
        return it->second;
    }
    SingleSwiftWriter *writer = new SingleSwiftWriter(_adminAdapter,
                                                      _channelManager,
                                                      pid,
                                                      _config,
                                                      _blockPool,
                                                      _mergeThreadPool,
                                                      _limiter,
                                                      _reporter,
                                                      _topicVersion,
                                                      _sessionIds[pid]);
    writer->setTopicChangeCallBack(std::bind(&SwiftWriterImpl::setTopicChanged, this, std::placeholders::_1));
    writer->setCommittedCallBack(_committedFunc);
    writer->setErrorCallBack(_errorFunc);
    writer->updateRangeUtil(_rangeUtil);
    _writers[pid] = writer;
    AUTIL_LOG(INFO, "create sinlge swift writer [%s %u]", _config.topicName.c_str(), pid);
    return writer;
}

SingleSwiftWriter *SwiftWriterImpl::getSingleWriter(uint32_t pid) {
    {
        ScopedReadLock lock(_mapLock);
        std::map<uint32_t, SingleSwiftWriter *>::const_iterator it = _writers.find(pid);
        if (it != _writers.end()) {
            return it->second;
        }
    }
    return insertSingleWriter(pid);
}

int64_t SwiftWriterImpl::getLastRefreshTime(uint32_t pid) const {
    ScopedReadLock lock(_mapLock);
    std::map<uint32_t, SingleSwiftWriter *>::const_iterator it = _writers.find(pid);
    if (it != _writers.end()) {
        return it->second->getLastRefreshTime();
    }
    return int64_t(-1);
}

ErrorCode SwiftWriterImpl::setTopicChanged(int64_t topicVersion) {
    AUTIL_LOG(INFO,
              "topic[%s] may changed, version[%ld], current partCnt[%d]",
              _config.topicName.c_str(),
              topicVersion,
              _partitionCount);
    if (_topicChanged) {
        return ERROR_NONE;
    }
    _topicChanged = true;
    _topicVersion = topicVersion;
    return ERROR_NONE;
}

common::SchemaWriter *SwiftWriterImpl::getSchemaWriter() { return NULL; }

void SwiftWriterImpl::setCommittedCallBack(const std::function<void(const vector<pair<int64_t, int64_t>> &)> &func) {
    _committedFunc = func;
}

void SwiftWriterImpl::setErrorCallBack(const std::function<void(const ErrorCode &)> &func) { _errorFunc = func; }

} // namespace client
} // namespace swift
