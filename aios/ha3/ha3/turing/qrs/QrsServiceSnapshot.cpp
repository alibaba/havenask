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
#include "ha3/turing/qrs/QrsServiceSnapshot.h"

#include <assert.h>
#include <algorithm>
#include <iosfwd>
#include <map>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/HashAlgorithm.h"
#include "autil/Log.h"
#include "autil/RangeUtil.h"
#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "kmonitor/client/KMonitorFactory.h"
#include "ha3/common/QueryInfo.h"
#include "ha3/common/VersionCalculator.h"
#include "ha3/config/ConfigAdapter.h"
#include "ha3/config/SqlConfig.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/sql/resource/IquanResource.h"
#include "ha3/sql/resource/SqlConfig.h"
#include "ha3/turing/common/QrsModelBiz.h"
#include "ha3/turing/qrs/QrsBiz.h"
#include "ha3/turing/qrs/QrsSqlBiz.h"
#include "ha3/turing/qrs/QrsTuringBiz.h"
#include "ha3/util/TypeDefine.h"
#include "ha3/version.h"
#include "iquan/common/Status.h"
#include "iquan/jni/Iquan.h"
#include "iquan/jni/IquanEnv.h"
#include "aios/network/gig/multi_call/common/ControllerParam.h"
#include "aios/network/gig/multi_call/common/TopoInfoBuilder.h"
#include "suez/sdk/KMonitorMetaInfo.h"
#include "suez/sdk/ServiceInfo.h"
#include "suez/turing/common/BizInfo.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/VersionConfig.h"
#include "suez/turing/common/WorkerParam.h"
#include "suez/turing/search/Biz.h"
#include "suez/turing/search/ServiceSnapshot.h"

namespace suez {
namespace turing {
class ServiceSnapshotBase;
struct SnapshotBaseOptions;
}  // namespace turing
}  // namespace suez

using namespace suez;
using namespace std;
using namespace autil;
using namespace suez::turing;

using namespace isearch::proto;
using namespace isearch::util;
using namespace isearch::common;
namespace isearch {
namespace turing {

const std::string QrsServiceSnapshot::EXTRA_PUBLISH_BIZ_NAME = "extraPublishBizName";

AUTIL_LOG_SETUP(ha3, QrsServiceSnapshot);

QrsServiceSnapshot::QrsServiceSnapshot(bool enableSql)
    : _enableSql(enableSql)
{
}

QrsServiceSnapshot::~QrsServiceSnapshot() {
}

suez::BizMetas QrsServiceSnapshot::addExtraBizMetas(
        const suez::BizMetas &currentBizMetas) const
{
    if (currentBizMetas.size() <= 0) {
        return currentBizMetas;
    }
    BizMetas copyMetas = currentBizMetas;
    AUTIL_LOG(INFO, "biz size [%zu].", currentBizMetas.size());
    if (enableSql()) {
        auto it = copyMetas.find(DEFAULT_BIZ_NAME);
        if (it != copyMetas.end()) {
            copyMetas[DEFAULT_SQL_BIZ_NAME] = it->second;
            auto customBizInfo = copyMetas[DEFAULT_SQL_BIZ_NAME].getCustomBizInfo() ;
            customBizInfo[BIZ_TYPE] = autil::legacy::Any(string(SQL_BIZ_TYPE));
            copyMetas[DEFAULT_SQL_BIZ_NAME].setCustomBizInfo(customBizInfo);
            AUTIL_LOG(INFO, "add default sql biz.");
        } else {
            AUTIL_LOG(WARN,
                      "can not found biz[%s], skip create sql biz[%s]",
                      DEFAULT_BIZ_NAME,
                      DEFAULT_SQL_BIZ_NAME);
        }
    }
    bool onlySql = autil::EnvUtil::getEnv<bool>("onlySql", false);
    if (onlySql) {
        BizMetas sqlMetas;
        auto it = copyMetas.find(DEFAULT_SQL_BIZ_NAME);
        if (it != copyMetas.end()) {
            sqlMetas[DEFAULT_SQL_BIZ_NAME] = it->second;
            AUTIL_LOG(INFO, "remove other bizs, only use defalut sql biz.");
        }
        copyMetas = sqlMetas;
    }
    return copyMetas;
}

void QrsServiceSnapshot::calculateBizMetas(const suez::BizMetas &currentBizMetas,
        ServiceSnapshotBase *oldSnapshot, suez::BizMetas &toUpdate, suez::BizMetas &toKeep)
{
    const suez::BizMetas &metas = addExtraBizMetas(currentBizMetas);
    ServiceSnapshot::calculateBizMetas(metas, oldSnapshot, toUpdate, toKeep);
}

BizBasePtr QrsServiceSnapshot::createBizBase(const std::string &bizName,
                                       const suez::BizMeta &bizMeta)
{
    const auto &customBizInfo = bizMeta.getCustomBizInfo();
    auto iter = customBizInfo.find(BIZ_TYPE);
    if (iter != customBizInfo.end()) {
        if (autil::legacy::json::IsJsonString(iter->second)) {
            std::string bizType = *autil::legacy::AnyCast<std::string>(&iter->second);
            if (bizType == MODEL_BIZ_TYPE) {
                return BizPtr(new QrsModelBiz());
            } else if (bizType == SQL_BIZ_TYPE) {
                return BizPtr(new QrsSqlBiz(this, &_ha3BizMeta));
            }
        }
    }
    if (_basicTuringBizNames.count(bizName) == 1) {
        return BizPtr(new QrsTuringBiz());
    }
    return BizPtr(new QrsBiz());
}

void QrsServiceSnapshot::calTopoInfo(multi_call::TopoInfoBuilder &infoBuilder) {
    int32_t partCount = _serviceInfo.getPartCount();
    int32_t partId = _serviceInfo.getPartId();
    for (auto it : _bizMap) {
        auto qrsBiz = dynamic_cast<QrsBiz *>(it.second.get());
        if (qrsBiz) {
            uint32_t dataVersion = qrsBiz->getVersion();
            uint32_t protocolVersion = qrsBiz->getProtocolVersion();
            const vector<string> &topoBizNames = getTopoBizNames(qrsBiz);
            for (const auto &bizName : topoBizNames) {
                infoBuilder.addBiz(bizName,
                        partCount,
                        partId,
                        dataVersion,
                        multi_call::MAX_WEIGHT,
                        protocolVersion,
                        _grpcPort);
            }
        } else {
            auto qrsSqlBiz = dynamic_cast<QrsSqlBiz *>(it.second.get());
            if (qrsSqlBiz) {
                uint32_t dataVersion = qrsSqlBiz->getVersion();
                uint32_t protocolVersion = qrsSqlBiz->getProtocolVersion();
                auto bizNames = getExternalPublishBizNames();
                auto qrsBizName = qrsSqlBiz->getBizName();
                for (auto &bizName : bizNames) {
                    // add prefix
                    bizName = qrsBizName + "." + bizName;
                }
                bizNames.emplace_back(qrsBizName);
                for (const auto &bizName : bizNames) {
                    infoBuilder.addBiz("qrs." + bizName,
                                       1,
                                       0,
                                       dataVersion,
                                       multi_call::MAX_WEIGHT,
                                       protocolVersion,
                                       _grpcPort);
                    AUTIL_LOG(INFO,
                              "fill topo info in qrs sql biz: biz [%s], part count [%d],"
                              " partid [%d], version[%u], protocol version[%u]",
                              bizName.c_str(),
                              1,
                              0,
                              dataVersion,
                              protocolVersion);
                }
            }
        }
    }
}

// todo add default.sql
vector<string> QrsServiceSnapshot::getTopoBizNames(QrsBiz *qrsBiz) const {
    std::string domainName = "ha3";
    vector<string> searcherBizNames;
    if (qrsBiz->getConfigAdapter()) {
        qrsBiz->getConfigAdapter()->getClusterNamesWithExtendBizs(searcherBizNames);
    }
    vector<string> topoBizNames;
    for (const auto &bizName : searcherBizNames) {
        topoBizNames.push_back(domainName + "." + bizName);
    }
    return topoBizNames;
}

std::vector<std::string> QrsServiceSnapshot::getExternalPublishBizNames() {
    auto externalBizName = EnvUtil::getEnv(EXTRA_PUBLISH_BIZ_NAME, "");
    if (externalBizName.empty()) {
        return {};
    }
    return {externalBizName};
}

std::string QrsServiceSnapshot::getBizName(
        const std::string &bizName,
        const suez::BizMeta &bizMeta) const
{
    const auto &customBizInfo = bizMeta.getCustomBizInfo();
    auto iter = customBizInfo.find(BIZ_TYPE);
    if (iter != customBizInfo.end()) {
        if (autil::legacy::json::IsJsonString(iter->second)) {
            std::string bizType = *autil::legacy::AnyCast<std::string>(&iter->second);
            if (bizType == MODEL_BIZ_TYPE) {
                return bizName;
            }
        }
    }
    if (_basicTuringBizNames.count(bizName) == 1) {
        return bizName;
    }
    return getZoneBizName(bizName);
}

bool QrsServiceSnapshot::initPid() {
    PartitionID partitionId;
    partitionId.set_clustername(getZoneName());
    autil::PartitionRange range;
    if (!getRange(_serviceInfo.getPartCount(), _serviceInfo.getPartId(), range)) {
        AUTIL_LOG(ERROR, "range invalid [%s]", ToJsonString(_serviceInfo).c_str());
        return false;
    }
    Range protoRange;
    protoRange.set_from(range.first);
    protoRange.set_to(range.second);
    *(partitionId.mutable_range()) = protoRange;
    partitionId.set_role(ROLE_QRS);
    _pid = partitionId;
    return true;
}

bool QrsServiceSnapshot::doInit(const SnapshotBaseOptions &options, ServiceSnapshotBase *oldSnapshot) {
    initAddress();
    if(!initPid()) {
        AUTIL_LOG(ERROR, "init Pid failed.");
        return false;
    }
    return true;
}

void QrsServiceSnapshot::initAddress() {
    _address = autil::EnvUtil::getEnv(WorkerParam::IP, "") +
               ":" + autil::EnvUtil::getEnv(WorkerParam::PORT);
}

bool QrsServiceSnapshot::getRange(uint32_t partCount, uint32_t partId, autil::PartitionRange &range) {
    const RangeVec &vec = RangeUtil::splitRange(0, RangeUtil::MAX_PARTITION_RANGE, partCount);
    if (partId >= vec.size()) {
        return false;
    }
    range = vec[partId];
    return true;
}

bool QrsServiceSnapshot::doWarmup() {
    if (_bizMap.empty()) {
        AUTIL_LOG(INFO, "biz is empty, skip start sql client");
        return true;
    }

    if (enableSql()) {
        if(!hasQrsSqlBiz()) {
            AUTIL_LOG(ERROR, "no qrs sql biz was found");
            return false;
        }

        if (!initSqlClient(_ha3BizMeta)) {
            AUTIL_LOG(ERROR, "init sql client failed");
            return false;
        }

        for (auto biz : _bizMap) {
            auto qrsSqlBiz = dynamic_cast<QrsSqlBiz *>(biz.second.get());
            if (qrsSqlBiz
                && !qrsSqlBiz->updateNavi(_sqlClientPtr, _ha3BizMeta.getModelConfigMap())) {
                AUTIL_LOG(ERROR, "update navi failed");
                return false;
            }
        }

        bool onlySql = autil::EnvUtil::getEnv<bool>("onlySql", false);
        if (onlySql) {
            auto searchService = getSearchService();
            if (searchService) {
                searchService->disableSnapshotLog();
                AUTIL_LOG(INFO, "only sql enabled, disable search service snapshot log for turing");
            }
        }
    }
    return true;
}

bool QrsServiceSnapshot::initSqlClient(const Ha3BizMeta &ha3BizMeta) {
    _sqlClientPtr.reset(new iquan::Iquan());
    const auto &sqlConfigPtr = ha3BizMeta.getSqlConfig();
    if (!sqlConfigPtr) {
        AUTIL_LOG(ERROR, "get sql config failed");
        return false;
    }
    iquan::Status status = _sqlClientPtr->init(
            sqlConfigPtr->jniConfig, sqlConfigPtr->clientConfig);
    if (!status.ok()) {
        AUTIL_LOG(
            ERROR,
            "init sql client failed, error msg [%s], view the detailed errors in iquan.error.log",
            status.errorMessage().c_str());
        return false;
    }

    auto enableLocalCatalog = autil::EnvUtil::getEnv<bool>("enableLocalCatalog", false);
    auto enableUpdateCatalog = autil::EnvUtil::getEnv<bool>("enableUpdateCatalog", false);
    if (enableUpdateCatalog || enableLocalCatalog) {
        AUTIL_LOG(INFO, "skip iquan update, invoke by catalog.");
        return true;
    }

    // 1. update table
    iquan::TableModels tableModels = ha3BizMeta.getSqlTableModels();

    sql::IquanResource::fillSummaryTables(tableModels, sqlConfigPtr->sqlConfig);

    bool isKhronosTable = false;
    if (!sql::IquanResource::fillKhronosTable(tableModels, isKhronosTable)) {
        AUTIL_LOG(WARN, "fill khronos table failed.");
        return false;
    }

    const auto &logicTables = sqlConfigPtr->sqlConfig.logicTables;
    if (!logicTables.empty()) {
        tableModels.merge(logicTables);
        SQL_LOG(INFO, "merge [%lu] logic tables.", logicTables.tables.size());
    }

    iquan::LayerTableModels layerTableModels;
    const auto &layerTables = sqlConfigPtr->sqlConfig.layerTables;
    if (!layerTables.empty()) {
        layerTableModels.merge(layerTables);
        SQL_LOG(INFO, "merge [%lu] layer tables.", layerTables.tables.size());
    }

    const auto &remoteTables = sqlConfigPtr->sqlConfig.remoteTables;
    if (!remoteTables.empty()) {
        tableModels.merge(remoteTables);
        SQL_LOG(INFO," merge [%lu] remote tables.", remoteTables.tables.size());
    }

    status = _sqlClientPtr->updateTables(tableModels);
    if (!status.ok()) {
        AUTIL_LOG(
            ERROR,
            "update table info failed, error msg [%s], view the detailed errors in iquan.error.log",
            status.errorMessage().c_str());
        return false;
    }
    status = _sqlClientPtr->updateLayerTables(layerTableModels);
    if (!status.ok()) {
        AUTIL_LOG(
            ERROR,
            "update layer table info failed, error msg [%s], view the detailed errors in iquan.error.log",
            status.errorMessage().c_str());
        return false;
    }

    // 2. update function
    auto functionModels = ha3BizMeta.getFunctionModels();
    status = _sqlClientPtr->updateFunctions(functionModels);
    if (!status.ok()) {
        std::string errorMsg = "update function info failed, error msg: " +
                               status.errorMessage();
        AUTIL_LOG(ERROR, "[%s], view the detailed errors in iquan.error.log", errorMsg.c_str());
        return false;
    }
    status = _sqlClientPtr->updateFunctions(ha3BizMeta.getTvfModels());
    if (!status.ok()) {
        std::string errorMsg = "update function info failed, error msg: " +
                               status.errorMessage();
        AUTIL_LOG(ERROR, "[%s], view the detailed errors in iquan.error.log", errorMsg.c_str());
        return false;
    }

    // 3. dump catalog
    std::string result;
    status = _sqlClientPtr->dumpCatalog(result);
    if (!status.ok()) {
        std::string errorMsg = "register sql client catalog failed: " + status.errorMessage();
        AUTIL_LOG(ERROR, "[%s], view the detailed errors in iquan.error.log", errorMsg.c_str());
        return false;
    }
    AUTIL_LOG(INFO, "sql client catalog: [%s]", result.c_str());

    // 4. warmup sql client
    const string &disableSqlWarmup = autil::EnvUtil::getEnv("disableSqlWarmup", "");
    if (disableSqlWarmup.empty() || !autil::StringUtil::fromString<bool>(disableSqlWarmup)) {
        AUTIL_LOG(INFO, "begin warmup iquan.");
        status = _sqlClientPtr->warmup(sqlConfigPtr->warmupConfig);
        if (!status.ok()) {
            AUTIL_LOG(
                ERROR,
                "failed to warmup iquan,error [%s], view the detailed errors in iquan.error.log",
                status.errorMessage().c_str());
        }
        AUTIL_LOG(INFO, "end warmup iquan.");
    }

    // 5. try to start kmon
    AUTIL_LOG(INFO, "begin iquan kmon.");
    iquan::KMonConfig kmonConfig = sqlConfigPtr->kmonConfig;
    do {
        if (kmonConfig.isValid()) {
            AUTIL_LOG(INFO, "use user kmon config");
            break;
        }
        if (!kmonitor::KMonitorFactory::IsStarted()) {
            AUTIL_LOG(WARN, "failed to get cpp kmon config, cpp kmon is not started");
            break;
        }
        kmonitor::MetricsConfig* cppKmonConfig = kmonitor::KMonitorFactory::GetConfig();
        if (cppKmonConfig == nullptr) {
            AUTIL_LOG(WARN, "failed to get cpp kmon config, cpp kmon config is null");
            break;
        }
        kmonConfig.serviceName = cppKmonConfig->service_name();
        kmonConfig.tenantName = cppKmonConfig->tenant_name();
        kmonConfig.flumeAddress = cppKmonConfig->sink_address();
        const kmonitor::MetricsTags* metricsTags = cppKmonConfig->global_tags();
        if (metricsTags != nullptr) {
            const std::map<std::string, std::string>& tags = metricsTags->GetTagsMap();
            kmonConfig.globalTags = autil::StringUtil::toString(tags, ":", ";");
        }
        AUTIL_LOG(INFO, "use cpp kmon config");
    } while(false);

    if (kmonConfig.isValid()) {
        status = iquan::IquanEnv::startKmon(kmonConfig);
        if (!status.ok()) {
            AUTIL_LOG(WARN, "failed to start kmon iquan, %s", status.errorMessage().c_str());
        }
    } else {
        AUTIL_LOG(ERROR, "failed to start kmon iquan, kmon config is not valid");
    }
    AUTIL_LOG(INFO, "end iquan kmon.");

    return true;
}

bool QrsServiceSnapshot::enableSql() const {
    return _enableSql && _basicTuringBizNames.count(DEFAULT_BIZ_NAME) == 0;
}

bool QrsServiceSnapshot::hasQrsSqlBiz() {
    for (auto biz : _bizMap) {
        auto qrsSqlBiz = dynamic_cast<QrsSqlBiz*>(biz.second.get());
        if (qrsSqlBiz) {
            return true;
        }
    }
    return false;
}

QrsBizPtr QrsServiceSnapshot::getFirstQrsBiz() {
    QrsBizPtr qrsBiz;
    for (auto &iter : _bizMap) {
        qrsBiz = dynamic_pointer_cast<QrsBiz>(iter.second);
        if (qrsBiz != nullptr) {
            break;
        }
    }
    return qrsBiz;
}

QrsSqlBizPtr QrsServiceSnapshot::getFirstQrsSqlBiz() {
    QrsSqlBizPtr qrsSqlBiz;
    for (auto &iter : _bizMap) {
        qrsSqlBiz = dynamic_pointer_cast<QrsSqlBiz>(iter.second);
        if (qrsSqlBiz != nullptr) {
            break;
        }
    }
    return qrsSqlBiz;
}

} // namespace turing
} // namespace isearch
