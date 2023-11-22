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
#include "build_service/common/IndexReclaimParamPreparer.h"

#include <ext/alloc_traits.h>
#include <iosfwd>
#include <memory>
#include <unistd.h>
#include <utility>
#include <vector>

#include "autil/HashFuncFactory.h"
#include "autil/HashFunctionBase.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/common/IndexReclaimConfigMaker.h"
#include "build_service/util/Monitor.h"
#include "build_service/util/SwiftClientCreator.h"
#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/framework/index_task/BasicDefs.h"
#include "indexlib/table/normal_table/index_task/PrepareIndexReclaimParamOperation.h"
#include "kmonitor/client/MetricType.h"
#include "swift/client/SwiftClient.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

using namespace std;

namespace build_service { namespace common {
AUTIL_LOG_SETUP(common, IndexReclaimParamPreparer);

IndexReclaimParamPreparer::IndexReclaimParamPreparer(const std::string& clusterName) : _clusterName(clusterName) {}

indexlib::Status IndexReclaimParamPreparer::Run(const indexlibv2::framework::IndexTaskContext* context,
                                                const std::map<std::string, std::string>& params)
{
    AUTIL_LOG(INFO, "index reclaim param preparer begin run");

    auto metricProvider = context->GetMetricProvider();
    _reclaimMsgFreshnessMetric = DECLARE_METRIC(metricProvider, "reclaimMsgFreshness", kmonitor::GAUGE, "us");
    _reclaimMsgCntMetric = DECLARE_METRIC(metricProvider, "reclaimMsgCount", kmonitor::STATUS, "count");
    _reclaimMsgReadErrQpsMetric = DECLARE_METRIC(metricProvider, "reclaimMsgReadErrorQps", kmonitor::QPS, "count");

    auto taskConfigs = context->GetTabletOptions()->GetIndexTaskConfigs("reclaim");
    if (taskConfigs.empty()) {
        return indexlib::Status::NotFound("get reclaim task config failed");
    }
    indexlibv2::config::IndexTaskConfig taskConfig = taskConfigs[0];
    auto [status, docReclaimSource] = taskConfig.GetSetting<config::DocReclaimSource>("doc_reclaim_source");
    RETURN_IF_STATUS_ERROR(status, "doc reclaim config is invalid");

    int64_t stopTsInSeconds = 0;
    indexlibv2::framework::IndexOperationId opId = 0;
    bool useOpFenceDir = false;
    indexlibv2::table::PrepareIndexReclaimParamOperation::GetOpInfo(params, opId, useOpFenceDir);
    auto iter = params.find(indexlibv2::table::PrepareIndexReclaimParamOperation::TASK_STOP_TS_IN_SECONDS);
    if (iter == params.end()) {
        RETURN_STATUS_ERROR(InternalError, "failed get stopTsInSeconds");
    }
    if (!autil::StringUtil::fromString(iter->second, stopTsInSeconds)) {
        RETURN_STATUS_ERROR(InternalError, "stopTsInSeconds [%s] is invalid", iter->second.c_str());
    }
    auto [status1, fenceDir] = context->GetOpFenceRoot(opId, useOpFenceDir);
    RETURN_STATUS_DIRECTLY_IF_ERROR(status1);
    RETURN_STATUS_DIRECTLY_IF_ERROR(
        fenceDir
            ->RemoveDirectory(indexlibv2::table::PrepareIndexReclaimParamOperation::DOC_RECLAIM_DATA_DIR,
                              indexlib::file_system::RemoveOption::MayNonExist())
            .Status());
    auto res = fenceDir->MakeDirectory(indexlibv2::table::PrepareIndexReclaimParamOperation::DOC_RECLAIM_DATA_DIR,
                                       indexlib::file_system::DirectoryOption());
    status = res.Status();
    RETURN_IF_STATUS_ERROR(status, "make directory failed, dir[%s]",
                           indexlibv2::table::PrepareIndexReclaimParamOperation::DOC_RECLAIM_DATA_DIR);
    auto workDir = res.result;
    std::string content;
    RETURN_STATUS_DIRECTLY_IF_ERROR(generateReclaimConfigFromSwift(docReclaimSource, stopTsInSeconds, content));

    RETURN_STATUS_DIRECTLY_IF_ERROR(
        workDir
            ->Store(/*fileName=*/indexlibv2::table::PrepareIndexReclaimParamOperation::DOC_RECLAIM_CONF_FILE,
                    /*fileContent=*/content, indexlib::file_system::WriterOption::AtomicDump())
            .Status());
    AUTIL_LOG(INFO, "index reclaim param preparer end run");
    return indexlib::Status::OK();
}

indexlib::Status
IndexReclaimParamPreparer::generateReclaimConfigFromSwift(const config::DocReclaimSource& reclaimSource,
                                                          int64_t stopTsInSecond, std::string& content)
{
    std::unique_ptr<swift::client::SwiftReader> swiftReader(createSwiftReader(reclaimSource, stopTsInSecond));
    if (!swiftReader) {
        std::string errorMsg = "doc reclaim task for cluster [" + _clusterName + "] create swift reader fail.";
        RETURN_STATUS_ERROR(InternalError, errorMsg.c_str());
    }

    int64_t startTs = -1;
    int64_t lastTs = -1;
    int64_t timestamp = -1;
    int64_t cnt = 0;
    swift::protocol::ErrorCode ec = swift::protocol::ERROR_NONE;
    IndexReclaimConfigMaker configMaker(_clusterName);

    while (true) {
        REPORT_METRIC(_reclaimMsgCntMetric, cnt);
        swift::protocol::Message message;
        ec = swiftReader->read(timestamp, message);
        if (ec == swift::protocol::ERROR_NONE) {
            ++cnt;
            lastTs = timestamp;
            if (startTs == -1) {
                startTs = timestamp;
                AUTIL_LOG(INFO, "read swift from msgId[%ld] uint16Payload[%u] uint8Payload[%u]", message.msgid(),
                          message.uint16payload(), message.uint8maskpayload());
            }
            configMaker.parseOneMessage(message.data());
            REPORT_METRIC(_reclaimMsgFreshnessMetric, autil::TimeUtility::currentTime() - timestamp);
            continue;
        }
        if (ec == swift::protocol::ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT) {
            REPORT_METRIC(_reclaimMsgCntMetric, cnt);
            AUTIL_LOG(INFO,
                      "swift read exceed the limited timestamp, "
                      "timeRange[%ld:%ld] finish now",
                      startTs, lastTs);
            break;
        }

        if (ec != swift::protocol::ERROR_CLIENT_NO_MORE_MESSAGE) {
            INCREASE_QPS(_reclaimMsgReadErrQpsMetric);
        }
        AUTIL_INTERVAL_LOG2(30, WARN, "read from swift error, errorCode [%s], time range [%ld : %ld]",
                            swift::protocol::ErrorCode_Name(ec).c_str(), startTs, lastTs);
        usleep(100 * 1000);
    }
    try {
        content = configMaker.makeReclaimParam();
    } catch (autil::legacy::ExceptionBase& e) {
        RETURN_STATUS_ERROR(InternalError, "make reclaim param failed, catch exception [%s]", e.what());
    }
    return indexlib::Status::OK();
}

swift::client::SwiftReader* IndexReclaimParamPreparer::createSwiftReader(const config::DocReclaimSource& reclaimSource,
                                                                         int64_t stopTsInSecond)
{
    std::string readerConfig = generationSwiftReaderConfigStr(reclaimSource);
    std::unique_ptr<swift::client::SwiftReader> swiftReader(
        doCreateSwiftReader(reclaimSource.swiftRoot, reclaimSource.clientConfigStr, readerConfig));
    if (!swiftReader) {
        return nullptr;
    }

    if (reclaimSource.msgTTLInSeconds > 0) {
        int64_t startTsInSecond = stopTsInSecond - reclaimSource.msgTTLInSeconds;
        if (startTsInSecond >= 0) {
            int64_t ts = startTsInSecond * 1000 * 1000;
            AUTIL_LOG(INFO, "seek to startTimestamp [%ld]", ts);
            swift::protocol::ErrorCode ec = swiftReader->seekByTimestamp(ts);
            if (ec != swift::protocol::ERROR_NONE) {
                AUTIL_LOG(WARN, "seek to startTimestamp [%ld] fail, errorCode [%s]!", ts,
                          swift::protocol::ErrorCode_Name(ec).c_str());
                return nullptr;
            }
        } else {
            AUTIL_LOG(WARN, "invalid ttl [%ld], stopTsInSecond [%ld]", reclaimSource.msgTTLInSeconds, stopTsInSecond);
            return nullptr;
        }
    }
    int64_t stopTs = stopTsInSecond * 1000 * 1000;
    AUTIL_LOG(INFO, "stopTimestamp: %ld", stopTs);
    swiftReader->setTimestampLimit(stopTs, stopTs);
    AUTIL_LOG(INFO, "actual stopTimestamp: %ld", stopTs);

    std::string msg = "doc reclaim task for cluster [" + _clusterName + "], stopTimestamp [" +
                      autil::StringUtil::toString(stopTs) + "]";
    if (reclaimSource.msgTTLInSeconds > 0) {
        msg += ", ttlInSeconds [" + autil::StringUtil::toString(reclaimSource.msgTTLInSeconds) + "]";
    }
    AUTIL_LOG(INFO, "%s", msg.c_str());
    return swiftReader.release();
}

std::string IndexReclaimParamPreparer::generationSwiftReaderConfigStr(const config::DocReclaimSource& reclaimSource)
{
    std::string readerConfig = "topicName=" + reclaimSource.topicName;
    std::vector<std::vector<std::string>> readerParams;
    autil::StringUtil::fromString(reclaimSource.readerConfigStr, readerParams, "=", ";");
    bool hashByClusterName = false;
    std::string hashFuncName = "HASH";
    std::map<std::string, std::string> kvPairs;
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
        autil::HashFunctionBasePtr hashFunc = autil::HashFuncFactory::createHashFunc(hashFuncName);
        if (!hashFunc) {
            BS_LOG(ERROR, "create HashFunc [%s] fail!", hashFuncName.c_str());
            return readerConfig;
        }
        uint32_t hashId = hashFunc->getHashId(_clusterName);
        kvPairs["from"] = autil::StringUtil::toString(hashId);
        kvPairs["to"] = autil::StringUtil::toString(hashId);
        BS_LOG(INFO, "rewrite swift reader range to [%u:%u]", hashId, hashId);
    }
    for (auto kv : kvPairs) {
        std::string tmp = kv.first + "=" + kv.second;
        readerConfig = readerConfig + ";" + tmp;
    }
    return readerConfig;
}
swift::client::SwiftReader* IndexReclaimParamPreparer::doCreateSwiftReader(const std::string& swiftRoot,
                                                                           const std::string& clientConfig,
                                                                           const std::string& readerConfig)
{
    auto swiftClientCreator = std::make_shared<util::SwiftClientCreator>();
    swift::client::SwiftClient* swiftClient = swiftClientCreator->createSwiftClient(swiftRoot, clientConfig);
    if (!swiftClient) {
        AUTIL_LOG(ERROR, "create swift client fail, swiftRoot [%s], clientConfig [%s]", swiftRoot.c_str(),
                  clientConfig.c_str());
        return nullptr;
    }

    AUTIL_LOG(INFO, "create swift reader with config %s", readerConfig.c_str());
    swift::protocol::ErrorInfo errorInfo;
    std::unique_ptr<swift::client::SwiftReader> swiftReader(swiftClient->createReader(readerConfig, &errorInfo));
    if (!swiftReader || errorInfo.has_errcode()) {
        AUTIL_LOG(ERROR, "create reader failed, config[%s], error msg[%s]", readerConfig.c_str(),
                  errorInfo.errmsg().c_str());
        return nullptr;
    }
    return swiftReader.release();
}

IndexReclaimParamPreparer::~IndexReclaimParamPreparer() {}

}} // namespace build_service::common
