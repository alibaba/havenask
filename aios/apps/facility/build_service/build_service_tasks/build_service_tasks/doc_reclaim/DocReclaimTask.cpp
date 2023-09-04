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
#include "build_service_tasks/doc_reclaim/DocReclaimTask.h"

#include <beeper/beeper.h>

#include "autil/HashFuncFactory.h"
#include "autil/StringUtil.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/Monitor.h"
#include "build_service/util/SwiftClientCreator.h"
#include "build_service_tasks/doc_reclaim/IndexReclaimConfigMaker.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"

using namespace autil;
using namespace build_service::util;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::common;
using namespace std;

using namespace indexlib::file_system;
using namespace swift::client;
using namespace swift::protocol;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, DocReclaimTask);

const string DocReclaimTask::TASK_NAME = BS_TASK_DOC_RECLAIM;

DocReclaimTask::DocReclaimTask() {}

DocReclaimTask::~DocReclaimTask() {}

bool DocReclaimTask::init(TaskInitParam& initParam)
{
    _taskInitParam = initParam;
    auto resourceReader = _taskInitParam.resourceReader;
    BuildServiceConfig buildServiceConfig;
    if (!resourceReader->getConfigWithJsonPath("build_app.json", "", buildServiceConfig)) {
        BS_LOG(WARN, "failed to get build_app.json");
        return false;
    }

    _reclaimConfigDir = IndexPathConstructor::constructDocReclaimDirPath(
        buildServiceConfig.getIndexRoot(), _taskInitParam.clusterName, _taskInitParam.buildId.generationId);
    bool dirExist = false;
    if (!fslib::util::FileUtil::isDir(_reclaimConfigDir, dirExist)) {
        return false;
    }

    if (!dirExist && !fslib::util::FileUtil::mkDir(_reclaimConfigDir, true)) {
        BS_LOG(WARN, "make dir [%s] fail!", _reclaimConfigDir.c_str());
        return false;
    }
    _swiftClientCreator.reset(createSwiftClientCreator());
    prepareMetrics();
    return true;
}

bool DocReclaimTask::handleTarget(const config::TaskTarget& target)
{
    int64_t stopTimeInSec = -1;
    if (!target.getTargetDescription("stopTime", stopTimeInSec)) {
        BS_LOG(ERROR, "fail to get stopTime from target [%s]", ToJsonString(target).c_str());
        return false;
    }

    if (isDone(target)) {
        return true;
    }

    string reclaimSrcStr;
    if (!target.getTargetDescription("reclaimSource", reclaimSrcStr)) {
        BS_LOG(ERROR, "fail to get reclaimSource from target [%s]", ToJsonString(target).c_str());
        return false;
    }

    string reclaimConfigFile = fslib::util::FileUtil::joinFilePath(_reclaimConfigDir, BS_DOC_RECLAIM_CONF_FILE);
    string targetFileWithTs = reclaimConfigFile + "." + StringUtil::toString(stopTimeInSec);
    string content;
    auto ec = FslibWrapper::AtomicLoad(targetFileWithTs, content).Code();
    if (ec == FSEC_OK) {
        BS_LOG(INFO, "file [%s] already exist, no need redo swift msg", targetFileWithTs.c_str());
        ec = FslibWrapper::AtomicStore(reclaimConfigFile, content, true).Code();
        THROW_IF_FS_ERROR(ec, "path[%s]", reclaimConfigFile.c_str());
    } else if (unlikely(ec != FSEC_NOENT)) {
        THROW_IF_FS_ERROR(ec, "path[%s]", targetFileWithTs.c_str());
    } else if (generateReclaimConfigFromSwift(reclaimSrcStr, stopTimeInSec, content)) {
        ec = FslibWrapper::AtomicStore(targetFileWithTs, content).Code();
        THROW_IF_FS_ERROR(ec, "path[%s]", targetFileWithTs.c_str());
        ec = FslibWrapper::AtomicStore(reclaimConfigFile, content, true).Code();
        THROW_IF_FS_ERROR(ec, "path[%s]", reclaimConfigFile.c_str());
    } else {
        BS_LOG(ERROR, "fail to generate reclaim config from swift msg");
        return false;
    }

    removeObsoleteReclaimConfig(_reclaimConfigDir, 10);
    {
        ScopedLock lock(_lock);
        _currentFinishTarget = target;
    }
    return true;
}

bool DocReclaimTask::isDone(const config::TaskTarget& targetDescription)
{
    ScopedLock lock(_lock);
    if (targetDescription == _currentFinishTarget) {
        return true;
    }
    return false;
}

indexlib::util::CounterMapPtr DocReclaimTask::getCounterMap() { return indexlib::util::CounterMapPtr(); }

std::string DocReclaimTask::getTaskStatus() { return "reclaim doc task is running"; }

void DocReclaimTask::removeObsoleteReclaimConfig(const string& reclaimConfRoot, uint32_t reservedVersionNum)
{
    vector<string> fileList;
    if (!fslib::util::FileUtil::listDir(reclaimConfRoot, fileList)) {
        BS_LOG(INFO, "list reclaimConfRoot[%s] failed", reclaimConfRoot.c_str());
        return;
    }

    auto it = remove_if(fileList.begin(), fileList.end(),
                        [](const string& f) { return f.find(BS_DOC_RECLAIM_CONF_FILE + ".") != 0; });

    fileList.erase(it, fileList.end());
    if (fileList.size() <= reservedVersionNum) {
        return;
    }

    auto COMP = [](const string& f1, const string& f2) -> bool {
        string id1Str = f1.substr(BS_DOC_RECLAIM_CONF_FILE.size() + 1);
        string id2Str = f2.substr(BS_DOC_RECLAIM_CONF_FILE.size() + 1);
        return StringUtil::fromString<int64_t>(id1Str) < StringUtil::fromString<int64_t>(id2Str);
    };

    sort(fileList.begin(), fileList.end(), COMP);
    for (size_t i = 0; i < fileList.size() - reservedVersionNum; i++) {
        if (!fslib::util::FileUtil::remove(fslib::util::FileUtil::joinFilePath(reclaimConfRoot, fileList[i]))) {
            BS_LOG(WARN, "remove [%s] failed", fileList[i].c_str());
        } else {
            BS_LOG(INFO, "remove [%s] ok", fileList[i].c_str());
        }
    }
}

bool DocReclaimTask::generateReclaimConfigFromSwift(const string& reclaimSrcStr, int64_t stopTsInSecond,
                                                    string& content)
{
    DocReclaimSource reclaimSource;
    try {
        FromJsonString(reclaimSource, reclaimSrcStr);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "parse DocReclaimSource from [%s] failed, exception[%s]", reclaimSrcStr.c_str(), e.what());
        return false;
    }
    SwiftReader* swiftReader = createSwiftReader(reclaimSource, stopTsInSecond);
    if (!swiftReader) {
        string errorMsg = "doc reclaim task for cluster [" + _taskInitParam.clusterName + "] create swift reader fail.";
        BEEPER_REPORT(WORKER_ERROR_COLLECTOR_NAME, errorMsg);
        return false;
    }

    IndexReclaimConfigMaker configMaker(_taskInitParam.clusterName);
    int64_t startTs = -1;
    int64_t lastTs = -1;
    int64_t timestamp = -1;
    int64_t cnt = 0;
    swift::protocol::ErrorCode ec = ERROR_NONE;
    while (true) {
        REPORT_METRIC(_reclaimMsgCntMetric, cnt);
        Message message;
        ec = swiftReader->read(timestamp, message);
        if (ec == ERROR_NONE) {
            ++cnt;
            lastTs = timestamp;
            if (startTs == -1) {
                startTs = timestamp;
                BS_LOG(INFO, "read swift from msgId[%ld] uint16Payload[%u] uint8Payload[%u]", message.msgid(),
                       message.uint16payload(), message.uint8maskpayload());
            }
            configMaker.parseOneMessage(message.data());
            REPORT_METRIC(_reclaimMsgFreshnessMetric, autil::TimeUtility::currentTime() - timestamp);
            continue;
        }
        if (ec == ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT) {
            REPORT_METRIC(_reclaimMsgCntMetric, cnt);
            BS_LOG(INFO,
                   "swift read exceed the limited timestamp, "
                   "timeRange[%ld:%ld] finish now",
                   startTs, lastTs);
            break;
        }

        if (ec != swift::protocol::ERROR_CLIENT_NO_MORE_MESSAGE) {
            INCREASE_QPS(_reclaimMsgReadErrQpsMetric);
        }

        BS_INTERVAL_LOG2(30, WARN, "read from swift error, errorCode [%s], time range [%ld : %ld]",
                         swift::protocol::ErrorCode_Name(ec).c_str(), startTs, lastTs);
        usleep(100 * 1000);
    }
    content = configMaker.makeReclaimParam();
    DELETE_AND_SET_NULL(swiftReader);
    return true;
}

SwiftClientCreator* DocReclaimTask::createSwiftClientCreator() { return new SwiftClientCreator(); }

SwiftReader* DocReclaimTask::createSwiftReader(const DocReclaimSource& reclaimSource, int64_t stopTsInSecond)
{
    string readerConfig = generationSwiftReaderConfigStr(reclaimSource);
    SwiftReader* swiftReader =
        doCreateSwiftReader(reclaimSource.swiftRoot, reclaimSource.clientConfigStr, readerConfig);
    if (!swiftReader) {
        return NULL;
    }

    if (reclaimSource.msgTTLInSeconds > 0) {
        int64_t startTsInSecond = stopTsInSecond - reclaimSource.msgTTLInSeconds;
        if (startTsInSecond >= 0) {
            int64_t ts = startTsInSecond * 1000 * 1000;
            BS_LOG(INFO, "seek to startTimestamp [%ld]", ts);
            swift::protocol::ErrorCode ec = swiftReader->seekByTimestamp(ts);
            if (ec != ERROR_NONE) {
                BS_LOG(WARN, "seek to startTimestamp [%ld] fail, errorCode [%s]!", ts,
                       swift::protocol::ErrorCode_Name(ec).c_str());
                DELETE_AND_SET_NULL(swiftReader);
                return NULL;
            }
        } else {
            BS_LOG(WARN, "invalid ttl [%ld], stopTsInSecond [%ld]", reclaimSource.msgTTLInSeconds, stopTsInSecond);
            DELETE_AND_SET_NULL(swiftReader);
            return NULL;
        }
    }
    int64_t stopTs = stopTsInSecond * 1000 * 1000;
    BS_LOG(INFO, "stopTimestamp: %ld", stopTs);
    swiftReader->setTimestampLimit(stopTs, stopTs);
    BS_LOG(INFO, "actual stopTimestamp: %ld", stopTs);

    string msg = "doc reclaim task for cluster [" + _taskInitParam.clusterName + "], stopTimestamp [" +
                 StringUtil::toString(stopTs) + "]";
    if (reclaimSource.msgTTLInSeconds > 0) {
        msg += ", ttlInSeconds [" + StringUtil::toString(reclaimSource.msgTTLInSeconds) + "]";
    }
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    return swiftReader;
}

SwiftReader* DocReclaimTask::doCreateSwiftReader(const string& swiftRoot, const string& clientConfig,
                                                 const string& readerConfig)
{
    assert(_swiftClientCreator);
    SwiftClient* swiftClient = _swiftClientCreator->createSwiftClient(swiftRoot, clientConfig);
    if (!swiftClient) {
        BS_LOG(ERROR, "create swift client fail, swiftRoot [%s], clientConfig [%s]", swiftRoot.c_str(),
               clientConfig.c_str());
        return NULL;
    }

    BS_LOG(INFO, "create swift reader with config %s", readerConfig.c_str());
    swift::protocol::ErrorInfo errorInfo;
    SwiftReader* swiftReader = swiftClient->createReader(readerConfig, &errorInfo);
    if (!swiftReader || errorInfo.has_errcode()) {
        BS_LOG(ERROR, "create reader failed, config[%s], error msg[%s]", readerConfig.c_str(),
               errorInfo.errmsg().c_str());
        DELETE_AND_SET_NULL(swiftReader);
        return NULL;
    }
    return swiftReader;
}

string DocReclaimTask::generationSwiftReaderConfigStr(const DocReclaimSource& reclaimSource)
{
    string readerConfig = "topicName=" + reclaimSource.topicName;
    vector<vector<string>> readerParams;
    StringUtil::fromString(reclaimSource.readerConfigStr, readerParams, "=", ";");
    bool hashByClusterName = false;
    string hashFuncName = "HASH";
    map<string, string> kvPairs;
    for (size_t i = 0; i < readerParams.size(); i++) {
        if (readerParams[i].size() != 2) {
            BS_LOG(ERROR, "invalid swift reader config param [%s]", reclaimSource.readerConfigStr.c_str());
            return readerConfig;
        }
        if (readerParams[i][0] == "hashByClusterName" && readerParams[i][1] == "true") {
            hashByClusterName = true;
            continue;
        }
        if (readerParams[i][0] == "clusterHashFunc") {
            hashFuncName = readerParams[i][1];
            continue;
        }
        kvPairs.insert(make_pair(readerParams[i][0], readerParams[i][1]));
    }
    if (hashByClusterName) {
        HashFunctionBasePtr hashFunc = HashFuncFactory::createHashFunc(hashFuncName);
        if (!hashFunc) {
            BS_LOG(ERROR, "create HashFunc [%s] fail!", hashFuncName.c_str());
            return readerConfig;
        }
        uint32_t hashId = hashFunc->getHashId(_taskInitParam.clusterName);
        kvPairs["from"] = StringUtil::toString(hashId);
        kvPairs["to"] = StringUtil::toString(hashId);
        BS_LOG(INFO, "rewrite swift reader range to [%u:%u]", hashId, hashId);
    }
    for (auto kv : kvPairs) {
        string tmp = kv.first + "=" + kv.second;
        readerConfig = readerConfig + ";" + tmp;
    }
    return readerConfig;
}

void DocReclaimTask::prepareMetrics()
{
    _metricProvider = _taskInitParam.metricProvider;
    _reclaimMsgFreshnessMetric = DECLARE_METRIC(_metricProvider, "reclaimMsgFreshness", kmonitor::GAUGE, "us");
    _reclaimMsgCntMetric = DECLARE_METRIC(_metricProvider, "reclaimMsgCount", kmonitor::STATUS, "count");
    _reclaimMsgReadErrQpsMetric = DECLARE_METRIC(_metricProvider, "reclaimMsgReadErrorQps", kmonitor::QPS, "count");
}

}} // namespace build_service::task_base
