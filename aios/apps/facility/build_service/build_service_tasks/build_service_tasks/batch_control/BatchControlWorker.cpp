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
#include "build_service_tasks/batch_control/BatchControlWorker.h"

#include <beeper/beeper.h>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/PathDefine.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service_tasks/batch_control/BatchReporter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "worker_framework/ZkState.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

using namespace build_service::config;
using namespace build_service::common;
using namespace build_service::task_base;

using namespace indexlib::file_system;

using namespace swift::client;
using namespace swift::protocol;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, BatchControlWorker);

const string BatchControlWorker::LINE_SEP = "\n";
const string BatchControlWorker::KEY_VALUE_SEP = "=";
const string BatchControlWorker::KEY_OPERATION = "operation";
const string BatchControlWorker::KEY_OPERATION_BEGIN = "begin";
const string BatchControlWorker::KEY_OPERATION_END = "end";
const string BatchControlWorker::KEY_BATCH_ID = "batch";
const string BatchControlWorker::BS_CALL_GRAPH_FAILED_TIME = "bs_call_graph_fail_time";
const string BatchControlWorker::BS_SLOW_BATCH_STEP = "bs_slow_batch_merge_step";

#define ZFS_PREFIX "zfs://"
#define ZFS_PREFIX_LEN 6
#define DEFAULT_ZK_TIMEOUT 10

typedef worker_framework::WorkerState WorkerState;
typedef worker_framework::ZkState ZkState;

string BatchControlWorker::BatchInfo::toString() const
{
    string op = "unknown";
    string ts = "";
    if (operation == BatchOp::begin) {
        op = "begin";
        ts = StringUtil::toString(beginTime);
    } else if (operation == BatchOp::end) {
        op = "end";
        ts = StringUtil::toString(endTime);
    }
    return StringUtil::toString(batchId) + "-" + op + "-" + ts;
}

bool BatchControlWorker::BatchInfo::operator==(const BatchControlWorker::BatchInfo& other) const
{
    return beginTime == other.beginTime && endTime == other.endTime && batchId == other.batchId &&
           operation == other.operation;
}

void BatchControlWorker::BatchInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("begin_ts", beginTime, beginTime);
    json.Jsonize("end_ts", endTime, endTime);
    json.Jsonize("batch_id", batchId, batchId);
    json.Jsonize("operation", operation, operation);
}

BatchControlWorker::BatchControlWorker(bool async)
    : _asyncMode(async)
    , _startTimestamp(-1)
    , _batchCursor(0)
    , _locator(-1)
    , _globalId(0)
    , _finalStepBatchId(-1)
    , _successReportTs(TimeUtility::currentTimeInSeconds())
    , _callGraphFailedTime(DEFAULT_CALL_GRAPH_FAIL_TIME)
    , _slowBatchStep(-1)
    , _swiftReader(NULL)
    , _zkWrapper(NULL)
{
    _callGraphFailedTime = autil::EnvUtil::getEnv(BS_CALL_GRAPH_FAILED_TIME, _callGraphFailedTime);
    if (autil::EnvUtil::hasEnv(BS_SLOW_BATCH_STEP)) {
        _slowBatchStep = autil::EnvUtil::getEnv(BS_SLOW_BATCH_STEP, _slowBatchStep);
        BS_LOG(INFO, "init slow batch step from env [%ld]", _slowBatchStep);
    }
}

BatchControlWorker::~BatchControlWorker() { DELETE_AND_SET_NULL(_zkWrapper); }

void BatchControlWorker::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("batch_cursor", _batchCursor, _batchCursor);
    json.Jsonize("locator", _locator, _locator);
    json.Jsonize("batch_queue", _batchQueue, _batchQueue);
    json.Jsonize("global_id", _globalId, _globalId);
    json.Jsonize("final_step_batch", _finalStepBatchId, _finalStepBatchId);
    json.Jsonize("data_description", _dataDesc, _dataDesc);
}

bool BatchControlWorker::init(Task::TaskInitParam& initParam, beeper::EventTagsPtr tags)
{
    _beeperTags = tags;
    auto resourceReader = initParam.resourceReader;
    BuildServiceConfig buildServiceConfig;
    if (!resourceReader->getConfigWithJsonPath("build_app.json", "", buildServiceConfig)) {
        BS_LOG(WARN, "failed to get build_app.json");
        return false;
    }
    vector<string> clusterNames;
    if (!resourceReader->getAllClusters(initParam.buildId.dataTable, clusterNames)) {
        BS_LOG(ERROR, "get all clusters for %s failed", initParam.buildId.dataTable.c_str());
        return false;
    }

    proto::BuildId buildId;
    buildId.set_datatable(initParam.buildId.dataTable);
    buildId.set_appname(initParam.buildId.appName);
    buildId.set_generationid(initParam.buildId.generationId);

    if (!initZkState(buildServiceConfig.zkRoot, buildId)) {
        return false;
    }
    // load from checkpoint, locator, batchCursor, batchQueue
    string content;
    WorkerState::ErrorCode ec = _zkState->read(content);
    if (WorkerState::EC_FAIL == ec) {
        BS_LOG(ERROR, "read batch_info.json failed");
        return false;
    }
    if (ec != WorkerState::EC_NOT_EXIST) {
        FromJsonString(*this, content);
    }

    _reporter.reset(new BatchReporter);
    _reporter->init(buildId, clusterNames);
    return true;
}

bool BatchControlWorker::initZkState(const string& zkRoot, const proto::BuildId& buildId)
{
    string zkHost = getHostFromZkPath(zkRoot);
    if (zkHost.empty()) {
        return false;
    }
    string zkPath = PathDefine::getGenerationZkRoot(zkRoot, buildId);
    _zkWrapper = new cm_basic::ZkWrapper(zkHost, DEFAULT_ZK_TIMEOUT);
    if (!_zkWrapper->isConnected()) {
        if (!_zkWrapper->open()) {
            BS_LOG(ERROR, "connect to zk[%s] failed", zkHost.c_str());
            return false;
        }
    }
    string statusFile = FslibWrapper::JoinPath(zkPath, "batch_info.json");
    BS_LOG(INFO, "status file:%s", statusFile.c_str());
    _zkState.reset(new ZkState(_zkWrapper, statusFile));
    return true;
}

string BatchControlWorker::getHostFromZkPath(const string& zkPath)
{
    if (!autil::StringUtil::startsWith(zkPath, ZFS_PREFIX)) {
        BS_LOG(ERROR, "zkPath[%s] is not invalid.", zkPath.c_str());
        return "";
    }
    std::string tmpStr = zkPath.substr(ZFS_PREFIX_LEN);
    return tmpStr.substr(0, tmpStr.find("/"));
}

bool BatchControlWorker::resetHost(const std::string& host, int32_t port)
{
    if (_reporter) {
        return _reporter->resetHost(host, port);
    }
    BS_LOG(ERROR, "no reporter, reset failed");
    return false;
}
/**
 * should not override variable that initialized from checkpoint
 */
bool BatchControlWorker::start(SwiftReader* reader, int64_t startTs, const string& host, int32_t port)
{
    assert(_reporter);
    if (!_reporter->resetHost(host, port)) {
        return false;
    }
    _swiftReader = reader;
    _startTimestamp = startTs;
    // use latest locator
    int64_t seekTs = _locator != -1 ? _locator : startTs;
    BS_LOG(INFO, "seek to %ld", seekTs);
    swift::protocol::ErrorCode ec = _swiftReader->seekByTimestamp(seekTs, false);
    if (ec != swift::protocol::ERROR_NONE && ec != swift::protocol::ERROR_CLIENT_NO_MORE_MESSAGE) {
        BS_LOG(ERROR, "seek to %ld failed, ec[%d]", seekTs, ec);
        return false;
    }

    if (!_asyncMode) {
        BS_LOG(INFO, "start as non-async mode");
        return false;
    }
    _readThread = LoopThread::createLoopThread(bind(&BatchControlWorker::readDocStr, this), 1000 * 1000);
    if (!_readThread) {
        BS_LOG(ERROR, "create read thread failed.");
        return false;
    }
    _reportThread = LoopThread::createLoopThread(bind(&BatchControlWorker::reportBatch, this), 10 * 1000 * 1000);
    if (!_reportThread) {
        BS_LOG(ERROR, "create report thread failed.");
        return false;
    }
    _syncThread = LoopThread::createLoopThread(bind(&BatchControlWorker::syncCheckpoint, this), 30 * 1000 * 1000);
    if (!_syncThread) {
        BS_LOG(ERROR, "create sync thread failed.");
        return false;
    }
    BEEPER_FORMAT_REPORT(WORKER_STATUS_COLLECTOR_NAME, *_beeperTags, "start batchControlWorker, seek to %ld", seekTs);
    return true;
}

void BatchControlWorker::syncCheckpoint()
{
    if (!_zkWrapper->isConnected()) {
        if (!_zkWrapper->open()) {
            BS_LOG(ERROR, "connect to zk failed");
            return;
        }
    }

    // sync checkpoint every 30s
    ScopedLock lock(_lock);
    assert(_zkState);
    string content = ToJsonString(*this);
    if (WorkerState::EC_FAIL == _zkState->write(content)) {
        BS_LOG(WARN, "update status[%s] failed", content.c_str());
    }
}

void BatchControlWorker::reportBatch()
{
    int64_t lastGlobalId = -1;
    BatchInfo batchInfo;
    {
        ScopedLock lock(_lock);
        if (_batchQueue.size() == 0 || (size_t)_batchCursor >= _batchQueue.size()) {
            // no more operation
            return;
        }

        batchInfo = _batchQueue[_batchCursor];
        if (_batchCursor - 2 >= 0) {
            lastGlobalId = _globalId - 1;
        }
    }
    if (!innerReport(batchInfo, _globalId, lastGlobalId)) {
        if (TimeUtility::currentTimeInSeconds() - _successReportTs > _callGraphFailedTime) {
            string msg = "worker long time call admin failed, exit now";
            handleError(msg);
        }
        return;
    }
    {
        ScopedLock lock(_lock);
        _batchCursor++;
        if (batchInfo.operation == BatchOp::end) {
            _globalId++;
        }
        _successReportTs = TimeUtility::currentTimeInSeconds();
    }
}

bool BatchControlWorker::innerReport(const BatchInfo& batchInfo, int64_t globalId, int64_t lastGlobalId)
{
    if (_slowBatchStep <= 1) {
        return _reporter->report(batchInfo, globalId, lastGlobalId);
    }
    return _reporter->report(batchInfo, globalId, lastGlobalId, _slowBatchStep, _finalStepBatchId);
}

void BatchControlWorker::handleError(const string& msg)
{
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg.c_str(), *_beeperTags);
    BEEPER_CLOSE();
    BS_LOG(ERROR, "%s", msg.c_str());
    _exit(-1);
}

void BatchControlWorker::readDocStr()
{
    swift::protocol::Message message;
    int64_t timestamp = -1;
    swift::protocol::ErrorCode ec = _swiftReader->read(timestamp, message, DEFAULT_WAIT_READER_TIME);

    if (swift::protocol::ERROR_NONE == ec) {
        BatchInfo batchInfo;
        if (!parse(message, batchInfo)) {
            _locator = timestamp;
            return;
        }
        BS_LOG(INFO, "begin process batch[%s]", message.data().c_str());
        process(batchInfo);
        _locator = timestamp;
        return;
    }
    if (swift::protocol::ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT == ec) {
        BS_LOG(ERROR, "swift read exceed the limited timestamp, curLocator[%ld]", _locator);
        return;
    }
    if (swift::protocol::ERROR_CLIENT_NO_MORE_MESSAGE == ec) {
        BS_INTERVAL_LOG(100, INFO, "no more messages");
        return;
    }

    BS_LOG(ERROR, "read from swift fail [ec = %d]", ec);
    return;
}

bool BatchControlWorker::validate(const BatchInfo& batchInfo) const
{
    if (batchInfo.operation == BatchOp::unknown) {
        BS_LOG(ERROR, "omit invalid operation[%d]", batchInfo.operation);
        return false;
    }
    if (batchInfo.operation == BatchOp::begin && batchInfo.beginTime == -1) {
        BS_LOG(ERROR, "omit invalid operation[%d], beginTime[%ld]", batchInfo.operation, batchInfo.beginTime);
        return false;
    }
    if (batchInfo.operation == BatchOp::end && batchInfo.endTime == -1) {
        BS_LOG(ERROR, "omit invalid operation[%d], endTime[%ld]", batchInfo.operation, batchInfo.endTime);
        return false;
    }
    return true;
}

void BatchControlWorker::addBatch(BatchInfo batchInfo)
{
    if (_batchQueue.size() != 0) {
        BatchInfo lastBatch = _batchQueue[_batchQueue.size() - 1];
        if (batchInfo.operation == BatchOp::end && batchInfo.batchId == lastBatch.batchId) {
            if (batchInfo.endTime < lastBatch.beginTime) {
                BS_LOG(ERROR, "add batch[%s] end time[%ld] smaller than begin time [%ld], set it to begin time",
                       batchInfo.toString().c_str(), batchInfo.endTime, lastBatch.beginTime);
                batchInfo.endTime = lastBatch.beginTime;
            }
        }
    }
    string batchStr = batchInfo.toString();
    BS_LOG(INFO, "add batch[%s]", batchStr.c_str());
    BEEPER_FORMAT_REPORT(WORKER_STATUS_COLLECTOR_NAME, *_beeperTags, "add batch[%s]", batchStr.c_str());
    _batchQueue.push_back(batchInfo);
}

void BatchControlWorker::process(const BatchInfo& batchInfo)
{
    if (!validate(batchInfo)) {
        return;
    }

    ScopedLock lock(_lock);
    if (_batchQueue.size() == 0) {
        if (batchInfo.operation == BatchOp::end) {
            // fill up begin op
            addBatch(BatchInfo(BatchOp::begin, _startTimestamp, -1, batchInfo.batchId));
        }
        addBatch(batchInfo);
        return;
    }
    auto it = find(_batchQueue.begin(), _batchQueue.end(), batchInfo);
    if (it != _batchQueue.end()) {
        BS_LOG(WARN, "batch[%s] already exist in batchQueue, skip it", batchInfo.toString().c_str());
        return;
    }
    BatchInfo lastBatch = _batchQueue[_batchQueue.size() - 1];
    if (lastBatch.batchId == batchInfo.batchId) {
        if (lastBatch.operation == batchInfo.operation) {
            BS_LOG(WARN, "duplicated batchId[%d], oldTs[%ld]-newTs[%ld], omit new one", batchInfo.batchId,
                   lastBatch.beginTime, batchInfo.beginTime);
            return;
        }
        if (lastBatch.operation == BatchOp::end && batchInfo.operation == BatchOp::begin) {
            BS_LOG(WARN, "invalid begin operation[%d] after end operation, omit it", batchInfo.batchId);
            return;
        }
        assert(lastBatch.operation == BatchOp::begin);
        assert(batchInfo.operation == BatchOp::end);
        addBatch(batchInfo);
        return;
    }

    // for different batch
    if (batchInfo.operation == BatchOp::begin) {
        if (lastBatch.operation == BatchOp::begin) {
            // fill up a missing end op
            addBatch(BatchInfo(BatchOp::end, -1, batchInfo.beginTime, lastBatch.batchId));
        }
        addBatch(batchInfo);
        return;
    }

    assert(batchInfo.operation == BatchOp::end);
    if (lastBatch.operation == BatchOp::begin) {
        // fill up end op[old batch], fill up begin op[new batch]
        addBatch(BatchInfo(BatchOp::end, -1, batchInfo.endTime, lastBatch.batchId));
        addBatch(BatchInfo(BatchOp::begin, lastBatch.beginTime, -1, batchInfo.batchId));
        addBatch(batchInfo);
        return;
    }

    assert(lastBatch.operation == BatchOp::end);
    // fill up a missing begin op
    addBatch(BatchInfo(BatchOp::begin, lastBatch.endTime, -1, batchInfo.batchId));
    addBatch(batchInfo);
}

bool BatchControlWorker::parse(const Message& msg, BatchInfo& batchInfo) const
{
    string cmd = msg.data();
    vector<string> params = StringUtil::split(cmd, BatchControlWorker::LINE_SEP);
    for (size_t i = 0; i < params.size(); i++) {
        StringUtil::trim(params[i]);
        if (params[i].empty()) {
            continue;
        }
        vector<string> kv = StringUtil::split(params[i], BatchControlWorker::KEY_VALUE_SEP);
        if (kv.size() != 2) {
            BS_LOG(ERROR, "invalid cmd[%s]", cmd.c_str());
            return false;
        }
        StringUtil::trim(kv[0]);
        StringUtil::trim(kv[1]);
        if (kv[0] == BatchControlWorker::KEY_OPERATION) {
            if (kv[1] == BatchControlWorker::KEY_OPERATION_BEGIN) {
                batchInfo.operation = BatchOp::begin;
                batchInfo.beginTime = msg.timestamp();
            } else if (kv[1] == BatchControlWorker::KEY_OPERATION_END) {
                batchInfo.operation = BatchOp::end;
                batchInfo.endTime = msg.timestamp();
            } else {
                BS_LOG(ERROR, "invalid operation, cmd[%s]", cmd.c_str());
                return false;
            }
        } else if (kv[0] == BatchControlWorker::KEY_BATCH_ID) {
            int64_t batchId = -1;
            if (!StringUtil::fromString(kv[1], batchId)) {
                BS_LOG(ERROR, "invalid batchId, cmd[%s]", cmd.c_str());
                return false;
            }
            batchInfo.batchId = batchId;
        }
    }
    if (batchInfo.operation == BatchOp::unknown) {
        BS_LOG(ERROR, "lack of operation, cmd[%s]", cmd.c_str());
        return false;
    }
    if (batchInfo.batchId == -1) {
        BS_LOG(ERROR, "lack of batchId, cmd[%s]", cmd.c_str());
        return false;
    }
    return true;
}

void BatchControlWorker::setDataDescription(const proto::DataDescription& dataDesc)
{
    _dataDesc = dataDesc;
    if (_reporter) {
        _reporter->setDataDescription(_dataDesc);
    }
}

void BatchControlWorker::setUseV2Graph(bool useV2)
{
    if (_reporter) {
        _reporter->setUseV2Graph(useV2);
    }
}

}} // namespace build_service::task_base
