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
#include "swift/broker/service/BrokerPartition.h"

#include <algorithm>
#include <assert.h>
#include <memory>
#include <sstream>
#include <stdint.h>

#include "autil/CommonMacros.h"
#include "autil/Singleton.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/ZlibCompressor.h"
#include "flatbuffers/flatbuffers.h"
#include "fslib/fs/FileSystem.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "swift/broker/service/MirrorPartition.h"
#include "swift/broker/storage/BrokerResponseError.h"
#include "swift/broker/storage/MessageBrain.h"
#include "swift/common/RangeUtil.h"
#include "swift/common/ThreadBasedObjectPool.h"
#include "swift/config/BrokerConfig.h"
#include "swift/config/PartitionConfig.h"
#include "swift/filter/FieldFilter.h"
#include "swift/filter/MessageFilter.h"
#include "swift/log/BrokerLogClosure.h"
#include "swift/monitor/BrokerMetricsReporter.h"
#include "swift/monitor/MetricsCommon.h"
#include "swift/monitor/TimeTrigger.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/FBMessageWriter.h"
#include "swift/protocol/MessageCompressor.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/SwiftMessage_generated.h"
#include "swift/util/Block.h"
#include "swift/util/MessageUtil.h"
#include "swift/util/TimeoutChecker.h"

namespace swift {
namespace util {
class BlockPool;
class PermissionCenter;
} // namespace util
} // namespace swift

using namespace std;
using namespace autil;
using namespace kmonitor;
using namespace fslib::cache;
using namespace swift::protocol;
using namespace swift::monitor;
using namespace swift::config;
using namespace swift::util;
using namespace swift::common;
using namespace swift::filter;
using namespace swift::heartbeat;

namespace swift {
namespace service {
AUTIL_LOG_SETUP(swift, BrokerPartition);

BrokerPartition::BrokerPartition(const protocol::TaskInfo &taskInfo,
                                 monitor::BrokerMetricsReporter *metricsReporter,
                                 util::ZkDataAccessorPtr zkDataAccessor)
    : BrokerPartition(std::make_shared<ThreadSafeTaskStatus>(taskInfo), metricsReporter, zkDataAccessor) {}

BrokerPartition::BrokerPartition(const ThreadSafeTaskStatusPtr &taskStatus,
                                 BrokerMetricsReporter *metricsReporter,
                                 util::ZkDataAccessorPtr zkDataAccessor)
    : _messageBrain(nullptr)
    , _taskStatus(taskStatus)
    , _metricsReporter(metricsReporter)
    , _isCommitting(false)
    , _sessionId(TimeUtility::currentTimeInMicroSeconds())
    , _rangeUtil(nullptr)
    , _needFieldFilter(false)
    , _enableLongPolling(false)
    , _metricsTags(new MetricsTags())
    , _zkDataAccessor(zkDataAccessor) {
    if (_taskStatus) {
        const PartitionId &partId = _taskStatus->getPartitionId();
        _topicName = partId.topicname();
        _partitionId = partId.id();
        _taskStatus->setSessionId(_sessionId);
        _metricsTags->AddTag("topic", _topicName);
        _metricsTags->AddTag("partition", intToString(_partitionId));
        _metricsTags->AddTag("access_id", DEFAULT_METRIC_ACCESSID);
    }
}

BrokerPartition::~BrokerPartition() {
    if (_mirror) {
        _mirror->stop();
        _mirror.reset();
    }
    DELETE_AND_SET_NULL(_messageBrain);
    DELETE_AND_SET_NULL(_rangeUtil);
}

void BrokerPartition::stopSecurityModeThread() {
    if (_messageBrain) {
        _messageBrain->stopSecurityModeThread();
    }
}

bool BrokerPartition::init(const BrokerConfig &brokerConfig,
                           BlockPool *centerBlockPool,
                           BlockPool *fileCachePool,
                           PermissionCenter *permissionCenter,
                           const std::shared_ptr<network::SwiftRpcChannelManager> &channelManager) {
    assert(_messageBrain == NULL);
    PartitionConfig partitionConfig;
    preparePartitionConfig(brokerConfig, partitionConfig);
    _messageBrain = new storage::MessageBrain(_sessionId);
    const PartitionId &partId = getPartitionId();
    ErrorCode errorCode = _messageBrain->init(partitionConfig,
                                              centerBlockPool,
                                              fileCachePool,
                                              permissionCenter,
                                              _metricsReporter,
                                              _taskStatus,
                                              brokerConfig.getOneFileFdNum(),
                                              brokerConfig.getCacheFileReserveTime(),
                                              brokerConfig.enableFastRecover);
    if (errorCode != ERROR_NONE) {
        AUTIL_LOG(ERROR, "[%s] messageBrain init Failed", partId.ShortDebugString().c_str());
        _taskStatus->setError(errorCode, "init partition failed");
        return false;
    }
    _requestTimeout = brokerConfig.getRequestTimeout();
    _rangeUtil = new RangeUtil(partId.partitioncount(), partId.rangecount());
    _needFieldFilter = partitionConfig.needFieldFilter();
    _enableLongPolling = partitionConfig.enableLongPolling();
    if (_taskStatus->versionControl()) {
        _writerVersionController =
            std::make_shared<SingleTopicWriterController>(_zkDataAccessor, brokerConfig.zkRoot, _topicName);
        if (!_writerVersionController->init()) {
            AUTIL_LOG(ERROR, "[%s] writer version controller init failed", partId.ShortDebugString().c_str());
            return false;
        }
    }
    _taskStatus->setPartitionStatus(PARTITION_STATUS_RUNNING);
    return true;
}

std::string BrokerPartition::getPartitionDir(const std::string &rootDir, const protocol::PartitionId &partId) {
    if (rootDir.empty()) {
        return string();
    }
    ostringstream oss;
    oss << rootDir;
    if (*(rootDir.rbegin()) != '/') {
        oss << "/";
    }
    assert(partId.has_topicname() && partId.has_id());
    oss << partId.topicname() << "/" << partId.id() << "/";
    return oss.str();
}

void BrokerPartition::preparePartitionConfig(const config::BrokerConfig &brokerConfig,
                                             config::PartitionConfig &partitionConfig) {
    brokerConfig.getDefaultPartitionConfig(partitionConfig);
    const TaskInfo &taskInfo = _taskStatus->getTaskInfo();
    assert(taskInfo.has_partitionid());
    if (!partitionConfig.getConfigUnlimited()) {
        if (taskInfo.has_minbuffersize() && taskInfo.has_maxbuffersize()) {
            partitionConfig.setPartitionMinBufferSize(int64_t(taskInfo.minbuffersize()) << 20);
            partitionConfig.setPartitionMaxBufferSize(int64_t(taskInfo.maxbuffersize()) << 20);
        } else if (taskInfo.has_buffersize()) { // compatible old version
            uint32_t bufferSize = taskInfo.buffersize();
            partitionConfig.setPartitionMinBufferSize(int64_t((bufferSize + 1) / 2) << 20);
            partitionConfig.setPartitionMaxBufferSize(int64_t(bufferSize * 2) << 20);
        }
        int64_t allowedPartitionBufferSize = brokerConfig.getTotalBufferSize() / 2;
        if (partitionConfig.getPartitionMinBufferSize() > allowedPartitionBufferSize) {
            partitionConfig.setPartitionMinBufferSize(allowedPartitionBufferSize);
        }
        if (partitionConfig.getPartitionMaxBufferSize() > allowedPartitionBufferSize) {
            partitionConfig.setPartitionMaxBufferSize(allowedPartitionBufferSize);
        }
        if (partitionConfig.getPartitionMinBufferSize() > partitionConfig.getPartitionMaxBufferSize()) {
            partitionConfig.setPartitionMaxBufferSize(partitionConfig.getPartitionMinBufferSize());
        }
        if (partitionConfig.getBlockSize() < config::MIN_BLOCK_SIZE) {
            AUTIL_LOG(INFO,
                      "block size [%ld] too small, adjust to [%ld]",
                      partitionConfig.getBlockSize(),
                      config::MIN_BLOCK_SIZE);
            partitionConfig.setBlockSize(config::MIN_BLOCK_SIZE);
        }
        int64_t minPartitionBufferSize = partitionConfig.getBlockSize() * 2; // need 2 blocks at least
        if (partitionConfig.getPartitionMaxBufferSize() < minPartitionBufferSize) {
            AUTIL_LOG(WARN,
                      "max partition buffer size for [%s %d] too small, adjest to [%ld], "
                      "raw is [%ld]",
                      taskInfo.partitionid().topicname().c_str(),
                      taskInfo.partitionid().id(),
                      minPartitionBufferSize,
                      partitionConfig.getPartitionMaxBufferSize());
            partitionConfig.setPartitionMaxBufferSize(minPartitionBufferSize);
        }
    }
    if (taskInfo.has_topicmode()) {
        partitionConfig.setTopicMode(taskInfo.topicmode());
    }
    if (taskInfo.has_needfieldfilter()) {
        partitionConfig.setNeedFieldFilter(taskInfo.needfieldfilter());
    }
    if (taskInfo.has_obsoletefiletimeinterval() && taskInfo.obsoletefiletimeinterval() != -1) {
        int64_t timeInterval = taskInfo.obsoletefiletimeinterval() * 3600 * 1000 * 1000;
        partitionConfig.setObsoleteFileTimeInterval(timeInterval);
    }
    if (taskInfo.has_reservedfilecount() && taskInfo.reservedfilecount() != -1) {
        partitionConfig.setReservedFileCount(taskInfo.reservedfilecount());
    }
    if (taskInfo.has_maxwaittimeforsecuritycommit()) {
        partitionConfig.setMaxWaitTimeForSecurityCommit(taskInfo.maxwaittimeforsecuritycommit());
    }
    if (taskInfo.has_maxdatasizeforsecuritycommit()) {
        partitionConfig.setMaxDataSizeForSecurityCommit(taskInfo.maxdatasizeforsecuritycommit());
    }
    if (taskInfo.has_compressmsg()) {
        partitionConfig.setCompressMsg(taskInfo.compressmsg());
    }
    if (taskInfo.has_compressthres()) {
        partitionConfig.setCompressThres(taskInfo.compressthres());
    }
    if (taskInfo.has_readsizelimitsec() && 0 < taskInfo.readsizelimitsec()) {
        partitionConfig.setMaxReadSizeSec(taskInfo.readsizelimitsec());
    }
    if (taskInfo.has_enablelongpolling()) {
        partitionConfig.setEnableLongPolling(taskInfo.enablelongpolling());
    }
    if (taskInfo.has_readnotcommittedmsg()) {
        partitionConfig.setReadNotCommittedMsg(taskInfo.readnotcommittedmsg());
    }
    string dfsRoot = brokerConfig.dfsRoot;
    if (taskInfo.topicmode() != TOPIC_MODE_MEMORY_ONLY) {
        if (taskInfo.has_dfsroot() && taskInfo.dfsroot() != "") {
            dfsRoot = taskInfo.dfsroot();
        }
        string partitionDir = getPartitionDir(dfsRoot, taskInfo.partitionid());
        partitionConfig.setDataRoot(partitionDir);
    } else {
        partitionConfig.setDataRoot("");
    }
    if (taskInfo.extenddfsroot_size() > 0) {
        vector<string> extendRoots;
        for (int i = 0; i < taskInfo.extenddfsroot_size(); i++) {
            string extendDfsRoot = taskInfo.extenddfsroot(i);
            if (extendDfsRoot == dfsRoot) {
                continue;
            }
            string extendPartitionDir = getPartitionDir(extendDfsRoot, taskInfo.partitionid());
            extendRoots.push_back(extendPartitionDir);
        }
        partitionConfig.setExtendDataRoots(extendRoots);
    }
}

TimeoutChecker *BrokerPartition::createTimeoutChecker() {
    TimeoutChecker *timeoutChecker = new TimeoutChecker();
    timeoutChecker->setExpireTimeout(_requestTimeout);
    return timeoutChecker;
}

ErrorCode
BrokerPartition::getMessage(const ConsumptionRequest *request, MessageResponse *response, const string *srcIpPort) {
    string accessId;
    MetricsTagsPtr metricsTags = _metricsTags;
    if (request->has_authentication() && request->authentication().has_accessauthinfo()) {
        accessId = request->authentication().accessauthinfo().accessid();
        if (!accessId.empty()) {
            metricsTags = nullptr;
        }
    }
    ReadMetricsCollectorPtr collector(new ReadMetricsCollector(_topicName, _partitionId, accessId));
    TimeoutCheckerPtr timeoutCheckerPtr(createTimeoutChecker());
    TimeTrigger timeTrigger;
    timeTrigger.beginTrigger();
    collector->requiredMsgsCountPerReadRequest = request->count();
    adjustRequestFilter(request);
    ErrorCode ec = _messageBrain->getMessage(request, response, timeoutCheckerPtr.get(), srcIpPort, *collector);
    response->mutable_tracetimeinfo()->add_processedtimes(TimeUtility::currentTime());
    if (ec == ERROR_NONE && sealed() && response->maxtimestamp() < response->nexttimestamp() &&
        response->maxmsgid() < response->nextmsgid() && 0 == response->totalmsgcount()) {
        ec = ERROR_SEALED_TOPIC_READ_FINISH;
    } else if (ec == ERROR_BROKER_BUSY) {
        collector->readBusyError = true;
    } else if (ec == ERROR_BROKER_SOME_MESSAGE_LOST) {
        collector->messageLostError = true;
    } else if (ec == ERROR_BROKER_NO_DATA) {
        if (sealed()) {
            ec = ERROR_SEALED_TOPIC_READ_FINISH;
        }
        collector->brokerNodataError = true;
    } else if (ec != ERROR_NONE) {
        collector->otherReadError = true;
    }
    if (!request->candecompressmsg()) {
        decompressMessage(response, *collector);
    }
    if (!request->versioninfo().supportmergemsg()) {
        unpackMergedMessage(request, response, *collector);
    }
    if (request->needcompress()) {
        compressResponse(response, *collector);
    }
    if (_mirror) {
        response->set_selfversion(_mirror->selfVersion());
    }
    timeTrigger.endTrigger();
    collector->readRequestLatency = timeTrigger.getLatency();
    if (_metricsReporter) {
        _metricsReporter->reportReadMetricsBackupThread(collector, metricsTags);
    }
    return ec;
}

void BrokerPartition::addReplicationMessage(protocol::ProductionRequest *request, protocol::MessageResponse *response) {
    request->set_sessionid(getSessionId());
    request->set_replicationmode(true);
    auto closure = new ProductionLogClosure(request, response, nullptr, "addReplicationMessage");
    auto ec = sendMessage(closure);
    if (ec != ERROR_NONE) {
        AUTIL_LOG(WARN,
                  "%s:%d addReplicationMessage failed, error: %s",
                  _topicName.c_str(),
                  _partitionId,
                  ErrorCode_Name(ec).c_str());
    }
}

ErrorCode BrokerPartition::sendMessage(ProductionLogClosure *closure) {
    ErrorCode ec = ERROR_NONE;
    const ProductionRequest *request = closure->_request;
    MessageResponse *response = closure->_response;
    MetricsTagsPtr metricsTags = _metricsTags;
    string accessId;
    if (request->has_authentication() && request->authentication().has_accessauthinfo()) {
        accessId = request->authentication().accessauthinfo().accessid();
        if (!accessId.empty()) {
            metricsTags = nullptr;
        }
    }
    WriteMetricsCollectorPtr collector(new WriteMetricsCollector(_topicName, _partitionId, accessId));
    if (_taskStatus->versionControl()) {
        if (!request->has_writername() || request->writername().empty()) {
            string errMsg("request invalid, writer name empty error[ERROR_BROKER_WRITE_VERSION_INVALID]");
            AUTIL_LOG(WARN, "%s", errMsg.c_str());
            ec = ERROR_BROKER_WRITE_VERSION_INVALID;
            storage::setBrokerResponseError(response->mutable_errorinfo(), ec, errMsg);
            if (_metricsReporter) {
                collector->writeVersionError = true;
                _metricsReporter->reportWriteMetricsBackupThread(collector, metricsTags);
            }
            closure->Run();
            return ec;
        }
        if (!_writerVersionController->validateThenUpdate(
                request->writername(), request->majorversion(), request->minorversion())) {
            string errMsg =
                StringUtil::formatString("[%s %d %s]write version[%u-%u] error[ERROR_BROKER_WRITE_VERSION_INVALID], "
                                         "allow version[%s]",
                                         _topicName.c_str(),
                                         _partitionId,
                                         request->writername().c_str(),
                                         request->majorversion(),
                                         request->minorversion(),
                                         _writerVersionController->getDebugVersionInfo(request->writername()).c_str());
            AUTIL_LOG(WARN, "%s", errMsg.c_str());
            ec = ERROR_BROKER_WRITE_VERSION_INVALID;
            storage::setBrokerResponseError(response->mutable_errorinfo(), ec, errMsg);
            if (_metricsReporter) {
                collector->writeVersionError = true;
                _metricsReporter->reportWriteMetricsBackupThread(collector, metricsTags);
            }
            closure->Run();
            return ec;
        }
    }
    if (_mirror && !request->replicationmode() && !_mirror->canWrite()) {
        AUTIL_LOG(WARN, "[%s %d] only support replication message in mirror mode", _topicName.c_str(), _partitionId);
        ec = ERROR_BROKER_PERMISSION_DENIED;
        storage::setBrokerResponseError(response->mutable_errorinfo(), ec);
        closure->Run();
        return ec;
    }
    if (request->has_compressedmsgs()) {
        TimeTrigger decompressTimeTrigger;
        decompressTimeTrigger.beginTrigger();
        float ratio = 1.0;
        ec = MessageCompressor::decompressProductionRequest(const_cast<ProductionRequest *>(request), ratio);
        decompressTimeTrigger.endTrigger();
        collector->requestDecompressedRatio = ratio * 100;
        collector->requestDecompressedLatency = decompressTimeTrigger.getLatency();
        if (ec != ERROR_NONE) {
            if (_metricsReporter) {
                collector->otherWriteError = true;
                _metricsReporter->reportWriteMetricsBackupThread(collector, metricsTags);
            }
            AUTIL_LOG(WARN, "[%s] decompress request message failed", getPartitionId().ShortDebugString().c_str());
            storage::setBrokerResponseError(response->mutable_errorinfo(), ec);
            closure->Run();
            return ec;
        }
    }
    ec = _messageBrain->addMessage(closure, collector);
    return ec;
}

ErrorCode BrokerPartition::getMaxMessageId(const MessageIdRequest *request, MessageIdResponse *response) {
    return _messageBrain->getMaxMessageId(request, response);
}

ErrorCode BrokerPartition::getMinMessageIdByTime(const MessageIdRequest *request, MessageIdResponse *response) {
    assert(request->has_timestamp());
    TimeoutCheckerPtr timeoutCheckerPtr(createTimeoutChecker());
    TimeTrigger timeTrigger;
    timeTrigger.beginTrigger();
    ErrorCode ec = _messageBrain->getMinMessageIdByTime(request, response, timeoutCheckerPtr.get());
    timeTrigger.endTrigger();
    if (_metricsReporter) {
        _metricsReporter->incGetMinMessageIdByTimeQps(_metricsTags.get());
        _metricsReporter->reportGetMinMessageIdByTimeLatency(timeTrigger.getLatency(), _metricsTags.get());
    }
    return ec;
}

storage::RecycleInfo BrokerPartition::getRecycleInfo() {
    if (_taskStatus->getPartitionStatus() == PARTITION_STATUS_RUNNING) {
        return _messageBrain->getRecycleInfo();
    } else {
        return storage::RecycleInfo();
    }
}

void BrokerPartition::recycleBuffer() {
    if (_taskStatus->getPartitionStatus() == PARTITION_STATUS_RUNNING) {
        _messageBrain->recycleBuffer(0);
    }
}

void BrokerPartition::recycleFileCache(int64_t metaThreshold, int64_t dataThreshold) {
    if (_taskStatus->getPartitionStatus() == PARTITION_STATUS_RUNNING) {
        _messageBrain->recycleFileCache(metaThreshold, dataThreshold);
    }
}

void BrokerPartition::recycleFile() {
    if (_taskStatus->getPartitionStatus() == PARTITION_STATUS_RUNNING) {
        _messageBrain->recycleFile();
    }
}

void BrokerPartition::recycleObsoleteReader() {
    if (_taskStatus->getPartitionStatus() == PARTITION_STATUS_RUNNING) {
        _messageBrain->recycleObsoleteReader();
    }
}

void BrokerPartition::delExpiredFile() {
    if (_taskStatus->getPartitionStatus() == PARTITION_STATUS_RUNNING) {
        if (_taskStatus->enableTTlDel()) {
            _messageBrain->delExpiredFile();
        }
    }
}

void BrokerPartition::syncDfsUsedSize() {
    if (_taskStatus->getPartitionStatus() == PARTITION_STATUS_RUNNING) {
        _messageBrain->syncDfsUsedSize();
    }
}

bool BrokerPartition::needCommitMessage() {
    if (_mirror && !_mirror->canWrite()) {
        return false;
    }
    if (_taskStatus->getPartitionStatus() == PARTITION_STATUS_RUNNING &&
        (_messageBrain->getTopicMode() != TOPIC_MODE_SECURITY)) {
        return _messageBrain->needCommitMessage();
    }
    return false;
}

void BrokerPartition::commitMessage() {
    if (_taskStatus->getPartitionStatus() == PARTITION_STATUS_RUNNING &&
        (_messageBrain->getTopicMode() != TOPIC_MODE_SECURITY) && !_messageBrain->hasSealError()) {
        ErrorCode ec = _messageBrain->commitMessage();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN,
                      "commit message for partition[%s] failed,"
                      " errorCode[%d], errorMsg[%s]",
                      _taskStatus->getPartitionId().ShortDebugString().c_str(),
                      ec,
                      ErrorCode_Name(ec).c_str());
            _taskStatus->setError(ec, ErrorCode_Name(ec).c_str());
        }
    }
}

int64_t BrokerPartition::getCommittedMsgId() const { return _messageBrain->getCommittedMsgId(); }

void BrokerPartition::decompressMessage(MessageResponse *response, ReadMetricsCollector &collector) {
    TimeTrigger decompressTimeTrigger;
    decompressTimeTrigger.beginTrigger();
    float ratio;
    ErrorCode ec2 = MessageCompressor::decompressResponseMessage(response, ratio);
    decompressTimeTrigger.endTrigger();
    collector.readMsgDecompressedRatio = ratio * 100;
    collector.readMsgDecompressedLatency = decompressTimeTrigger.getLatency();
    if (ec2 != ERROR_NONE) {
        AUTIL_LOG(WARN, "decompress message error.");
    }
}

void BrokerPartition::compressResponse(MessageResponse *response, ReadMetricsCollector &collector) {
    TimeTrigger compressTimeTrigger;
    compressTimeTrigger.beginTrigger();
    float ratio = 0;
    MessageCompressor::compressMessageResponse(response, ratio);
    compressTimeTrigger.endTrigger();
    collector.requestCompressedRatio = ratio * 100;
    collector.msgCompressedLatency = compressTimeTrigger.getLatency();
}

void BrokerPartition::adjustRequestFilter(const ConsumptionRequest *request) {
    ConsumptionRequest *editableRequst = const_cast<ConsumptionRequest *>(request);
    Filter *filter = editableRequst->mutable_filter();
    uint16_t mergedTo = _rangeUtil->getMergeHashId(filter->to());
    filter->set_mergedto(mergedTo);
}

void BrokerPartition::unpackMergedMessage(const ConsumptionRequest *request,
                                          MessageResponse *response,
                                          ReadMetricsCollector &collector) {
    if (!response->hasmergedmsg()) {
        return;
    }
    MessageFilter *metaFilter = NULL;
    FieldFilter *fieldFilter = NULL;
    if ((request->filter().to() < request->filter().mergedto() && request->filter().to() < getPartitionId().to()) ||
        request->filter().from() > getPartitionId().from()) {
        metaFilter = new MessageFilter(request->filter());
    }
    if (_needFieldFilter) {
        if (request->requiredfieldnames_size() != 0 || request->has_fieldfilterdesc()) {
            fieldFilter = new FieldFilter();
            if (ERROR_NONE != fieldFilter->init(request)) {
                AUTIL_LOG(WARN, "[%s, %d] init field filter failed", _topicName.c_str(), _partitionId);
                DELETE_AND_SET_NULL(fieldFilter);
            }
            fieldFilter->setTopicName(_topicName);
            fieldFilter->setPartId(_partitionId);
        }
    }
    swift::common::ThreadBasedObjectPool<autil::ZlibCompressor> *compressorPool =
        autil::Singleton<swift::common::ThreadBasedObjectPool<autil::ZlibCompressor>,
                         ZlibCompressorInstantiation>::getInstance();
    autil::ZlibCompressor *compressor = compressorPool->getObject();
    if (response->messageformat() == MF_FB) {
        unpackMergedFbMessage(compressor, metaFilter, fieldFilter, response);
    } else if (response->messageformat() == MF_PB) {
        unpackMergedPbMessage(compressor, metaFilter, fieldFilter, response);
    }
    DELETE_AND_SET_NULL(metaFilter);
    DELETE_AND_SET_NULL(fieldFilter);
    collector.unpackMsgQPS = response->totalmsgcount();
    return;
}

void BrokerPartition::unpackMergedFbMessage(ZlibCompressor *compressor,
                                            const MessageFilter *metaFilter,
                                            FieldFilter *fieldfilter,
                                            MessageResponse *response) {
    ThreadBasedObjectPool<FBMessageWriter> *objectPool =
        Singleton<ThreadBasedObjectPool<FBMessageWriter>>::getInstance();
    protocol::FBMessageWriter *writer = objectPool->getObject();
    writer->reset();

    protocol::FBMessageReader reader;
    if (!reader.init(response->fbmsgs(), false)) {
        return;
    }
    size_t msgCount = reader.size();
    for (size_t i = 0; i < msgCount; i++) {
        const protocol::flat::Message *fbMsg = reader.read(i);
        if (!fbMsg->merged()) {
            writer->addMessage(fbMsg->data()->data(),
                               fbMsg->data()->size(),
                               fbMsg->msgId(),
                               fbMsg->timestamp(),
                               fbMsg->uint16Payload(),
                               fbMsg->uint8MaskPayload(),
                               fbMsg->compress(),
                               fbMsg->merged());
            continue;
        }
        unpackFbMessage(compressor, fbMsg, metaFilter, fieldfilter, writer);
    }
    writer->finish();
    response->set_fbmsgs(writer->data(), writer->size());
    response->set_hasmergedmsg(false);
    writer->reset();
    FBMessageReader readerCount;
    readerCount.init(response->fbmsgs(), false);
    response->set_totalmsgcount(readerCount.size());
}

void BrokerPartition::unpackMergedPbMessage(ZlibCompressor *compressor,
                                            const MessageFilter *metaFilter,
                                            FieldFilter *fieldfilter,
                                            MessageResponse *response) {
    MessageResponse tmpResponse;
    google::protobuf::RepeatedPtrField<protocol::Message> *tmpMsgs = tmpResponse.mutable_msgs();
    google::protobuf::RepeatedPtrField<protocol::Message> *msgs = response->mutable_msgs();
    tmpMsgs->Swap(msgs);
    int msgCount = tmpMsgs->size();
    for (int i = 0; i < msgCount; i++) {
        protocol::Message *msg = tmpMsgs->Mutable(i);
        if (!msg->merged()) {
            response->add_msgs()->Swap(msg);
            continue;
        }
        unpackPbMessage(compressor, msg, metaFilter, fieldfilter, response);
    }
    response->set_hasmergedmsg(false);
    response->set_totalmsgcount(response->msgs_size());
}

void BrokerPartition::unpackPbMessage(ZlibCompressor *compressor,
                                      Message *msg,
                                      const MessageFilter *metaFilter,
                                      FieldFilter *fieldfilter,
                                      MessageResponse *response) {
    std::vector<protocol::Message> unpackMsgVec;
    int32_t totalCount = 0;
    ZlibCompressor *tmpCompressor = msg->compress() ? compressor : NULL;
    bool ret = MessageUtil::unpackMessage(tmpCompressor,
                                          msg->data().c_str(),
                                          msg->data().size(),
                                          msg->msgid(),
                                          msg->timestamp(),
                                          metaFilter,
                                          fieldfilter,
                                          unpackMsgVec,
                                          totalCount);
    if (!ret) {
        AUTIL_LOG(WARN, "[%s %d] unpack message failed.", _topicName.c_str(), _partitionId);
        response->add_msgs()->Swap(msg);
        return;
    }
    AUTIL_LOG(DEBUG,
              "[%s %d] unpack message [%lu], total [%d].",
              _topicName.c_str(),
              _partitionId,
              unpackMsgVec.size(),
              totalCount);
    for (size_t i = 0; i < unpackMsgVec.size(); ++i) {
        response->add_msgs()->Swap(&unpackMsgVec[i]);
    }
}

void BrokerPartition::unpackFbMessage(ZlibCompressor *compressor,
                                      const protocol::flat::Message *fbMsg,
                                      const MessageFilter *metaFilter,
                                      FieldFilter *fieldFilter,
                                      protocol::FBMessageWriter *writer) {
    std::vector<protocol::Message> unpackMsgVec;
    int32_t totalCount = 0;
    ZlibCompressor *tmpCompressor = fbMsg->compress() ? compressor : NULL;
    bool ret = MessageUtil::unpackMessage(tmpCompressor,
                                          fbMsg->data()->data(),
                                          fbMsg->data()->size(),
                                          fbMsg->msgId(),
                                          fbMsg->timestamp(),
                                          metaFilter,
                                          fieldFilter,
                                          unpackMsgVec,
                                          totalCount);
    if (!ret) {
        AUTIL_LOG(WARN, "[%s %d] unpack message failed.", _topicName.c_str(), _partitionId);
        writer->addMessage(fbMsg->data()->data(),
                           fbMsg->data()->size(),
                           fbMsg->msgId(),
                           fbMsg->timestamp(),
                           fbMsg->uint16Payload(),
                           fbMsg->uint8MaskPayload(),
                           fbMsg->compress(),
                           fbMsg->merged());
        return;
    }
    AUTIL_LOG(DEBUG,
              "[%s %d] unpack message [%lu], total [%d].",
              _topicName.c_str(),
              _partitionId,
              unpackMsgVec.size(),
              totalCount);

    for (size_t i = 0; i < unpackMsgVec.size(); ++i) {
        protocol::Message &msg = unpackMsgVec[i];
        writer->addMessage(msg.data().c_str(),
                           msg.data().size(),
                           msg.msgid(),
                           msg.timestamp(),
                           msg.uint16payload(),
                           msg.uint8maskpayload(),
                           msg.compress(),
                           msg.merged());
    }
}

int64_t BrokerPartition::getFileCacheBlockCount() { return _messageBrain->getFileCacheBlockCount(); }

bool BrokerPartition::getFileBlockDis(vector<int64_t> &metaDisBlocks, vector<int64_t> &dataDisBlocks) {
    return _messageBrain->getFileBlockDis(metaDisBlocks, dataDisBlocks);
}

int64_t
BrokerPartition::addLongPolling(int64_t expireTime, const util::ConsumptionLogClosurePtr &closure, int64_t startId) {
    ScopedWriteLock lock(_pollingLock);
    _pollingMinStartId = min(_pollingMinStartId, startId);
    _pollingQueue.emplace_back(expireTime, closure);
    AUTIL_LOG(DEBUG,
              "session[%ld] part[%s] add long polling expireTime [%ld] queue size[%lu]",
              getSessionId(),
              getPartitionId().DebugString().c_str(),
              expireTime,
              _pollingQueue.size());
    return _pollingMaxMsgId;
}

size_t BrokerPartition::stealTimeoutPolling(bool timeBack,
                                            int64_t currentTime,
                                            int64_t maxHoldTime,
                                            ReadRequestQueue &timeoutReqs) {
    if (timeBack) {
        stealAllPolling(timeoutReqs);
        return 0;
    }

    ScopedWriteLock lock(_pollingLock);
    _pollingMinStartId = std::numeric_limits<int64_t>::max();
    // while (!_pollingQueue.empty()) {
    for (auto it = _pollingQueue.begin(); it != _pollingQueue.end();) {
        auto diff = it->first - currentTime;
        // timing back occured after request enqueue before addLongPolling, diff may exceed the maxHoldTime
        if (diff <= 0 || diff > maxHoldTime) {
            AUTIL_LOG(DEBUG,
                      "session [%ld] part [%s] steal timeout polling timeBack[%d] currentTime [%ld]  expireTime [%ld]",
                      getSessionId(),
                      getPartitionId().DebugString().c_str(),
                      timeBack,
                      currentTime,
                      it->first);

            timeoutReqs.push_back(*it);
            it = _pollingQueue.erase(it);
        } else {
            auto request = it->second->_request;
            _pollingMinStartId = min(_pollingMinStartId, request->startid());
            it++;
        }
    }
    return _pollingQueue.size();
}

void BrokerPartition::stealNeedActivePolling(ReadRequestQueue &pollingQueue,
                                             uint64_t compressPayload,
                                             int64_t maxMsgId) {
    autil::ScopedWriteLock lock(_pollingLock);
    _pollingMaxMsgId = max(_pollingMaxMsgId, maxMsgId);
    if (maxMsgId != -1 && maxMsgId < _pollingMinStartId) {
        return;
    }
    AUTIL_LOG(DEBUG,
              "session[%ld] part[%s] steal all polling, queue size[%lu] maxMsgId [%lu] _pollingMaxMsgId [%lu]",
              getSessionId(),
              getPartitionId().DebugString().c_str(),
              _pollingQueue.size(),
              maxMsgId,
              _pollingMaxMsgId);
    _pollingMinStartId = std::numeric_limits<int64_t>::max();
    for (auto it = _pollingQueue.begin(); it != _pollingQueue.end();) {
        const auto *request = it->second->_request;
        auto startId = request->startid();
        if (startId <= _pollingMaxMsgId) {
            if (!compressPayload || (compressPayload & request->compressfilterregion())) {
                pollingQueue.push_back(*it);
                it = _pollingQueue.erase(it);
                continue;
            }
        }
        _pollingMinStartId = min(_pollingMinStartId, startId);
        ++it;
    }
}

void BrokerPartition::stealAllPolling(ReadRequestQueue &pollingQueue) {
    autil::ScopedWriteLock lock(_pollingLock);
    AUTIL_LOG(DEBUG,
              "session[%ld] part[%s] steal all polling, queue size[%lu] _pollingMaxMsgId [%ld]",
              getSessionId(),
              getPartitionId().DebugString().c_str(),
              _pollingQueue.size(),
              _pollingMaxMsgId);
    _pollingMinStartId = std::numeric_limits<int64_t>::max();
    pollingQueue.swap(_pollingQueue);
}

void BrokerPartition::reportLongPollingQps() {
    if (_metricsReporter) {
        _metricsReporter->reportLongPollingQps(_metricsTags);
    }
}

void BrokerPartition::reportLongPollingMetrics(LongPollingMetricsCollector &collector) {
    if (_metricsReporter) {
        _metricsReporter->reportLongPollingMetrics(_metricsTags, collector);
    }
}

bool BrokerPartition::needRequestPolling(int64_t maxMsgId) {
    autil::ScopedReadLock lock(_pollingLock);
    return maxMsgId >= _pollingMinStartId;
}

void BrokerPartition::collectMetrics(monitor::PartitionMetricsCollector &collector) {
    _messageBrain->getPartitionMetrics(collector);
}

void BrokerPartition::collectInMetric(uint32_t interval, protocol::PartitionInMetric *metric) {
    _messageBrain->getPartitionInMetric(interval, metric);
}

protocol::TopicMode BrokerPartition::getTopicMode() { return _messageBrain->getTopicMode(); }

void BrokerPartition::setForceUnload(bool flag) { _messageBrain->setForceUnload(flag); }

protocol::PartitionId BrokerPartition::getPartitionId() const { return _taskStatus->getPartitionId(); }

protocol::PartitionStatus BrokerPartition::getPartitionStatus() const { return _taskStatus->getPartitionStatus(); }

void BrokerPartition::setPartitionStatus(protocol::PartitionStatus status) { _taskStatus->setPartitionStatus(status); }

ThreadSafeTaskStatusPtr BrokerPartition::getTaskStatus() const { return _taskStatus; }

bool BrokerPartition::startMirrorPartition(const string &mirrorZkRoot,
                                           const std::shared_ptr<network::SwiftRpcChannelManager> &channelManager) {
    auto taskInfo = _taskStatus->getTaskInfo();
    const auto &pid = taskInfo.partitionid();
    if (taskInfo.topicmode() == TOPIC_MODE_MEMORY_ONLY) {
        AUTIL_LOG(ERROR, "%s: can not start mirror topic in memory_only_mode", pid.ShortDebugString().c_str());
        return false;
    }

    _taskStatus->setPartitionStatus(PARTITION_STATUS_RECOVERING);

    auto mirror = createMirrorPartition(pid);
    if (!mirror->start(mirrorZkRoot, channelManager, this)) {
        AUTIL_LOG(ERROR, "%s: start mirror partition failed", pid.ShortDebugString().c_str());
        return false;
    }
    _mirror = std::move(mirror);
    return true;
}

std::unique_ptr<MirrorPartition> BrokerPartition::createMirrorPartition(const protocol::PartitionId &pid) const {
    return std::make_unique<MirrorPartition>(pid);
}

} // namespace service
} // namespace swift
