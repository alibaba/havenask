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
#include "sql/ops/remoteScan/TableServiceConnector.h"

#include <cstdint>
#include <ext/alloc_traits.h>
#include <google/protobuf/message.h>
#include <iosfwd>
#include <unistd.h>
#include <utility>

#include "autil/HashFuncFactory.h"
#include "autil/HashFunctionBase.h"
#include "autil/RangeUtil.h"
#include "autil/TimeUtility.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "multi_call/common/ErrorCode.h"
#include "multi_call/common/common.h"
#include "multi_call/interface/QuerySession.h"
#include "multi_call/interface/RequestGenerator.h"
#include "multi_call/interface/Response.h"
#include "sql/common/Log.h" // IWYU pragma: keep
#include "sql/common/TableDistribution.h"
#include "sql/ops/scan/ScanUtil.h"
#include "suez/service/Service.pb.h"
#include "table/Row.h"
#include "table/TableUtil.h"

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

using namespace std;
using namespace table;
using namespace kmonitor;

namespace sql {

AUTIL_LOG_SETUP(sql, TableServiceConnector);

class TableServiceConnectorMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_QPS_MUTABLE_METRIC(_qps, "TableServiceConnector.qps");
        REGISTER_LATENCY_MUTABLE_METRIC(_lookupTime, "TableServiceConnector.lookupTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_convertTime, "TableServiceConnector.convertTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_batchScanTime, "TableServiceConnector.batchScanTime");
        REGISTER_GAUGE_MUTABLE_METRIC(_lookupCount, "TableServiceConnector.lookupCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_lookupPartCount, "TableServiceConnector.lookupPartCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_notFoundCount, "TableServiceConnector.notFoundCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_failedCount, "TableServiceConnector.failedCount");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags,
                TableServiceConnector::MetricCollector *metrics) {
        REPORT_MUTABLE_QPS(_qps);
        REPORT_MUTABLE_METRIC(_lookupTime, metrics->lookupTime / 1000.0f);
        REPORT_MUTABLE_METRIC(_convertTime, metrics->convertTime / 1000.0f);
        REPORT_MUTABLE_METRIC(_batchScanTime, metrics->batchScanTime / 1000.0f);
        REPORT_MUTABLE_METRIC(_lookupCount, metrics->lookupCount);
        REPORT_MUTABLE_METRIC(_lookupPartCount, metrics->lookupPartCount);
        REPORT_MUTABLE_METRIC(_notFoundCount, metrics->notFoundCount);
        REPORT_MUTABLE_METRIC(_failedCount, metrics->failedCount);
    }

private:
    MutableMetric *_qps = nullptr;
    MutableMetric *_lookupTime = nullptr;
    MutableMetric *_convertTime = nullptr;
    MutableMetric *_batchScanTime = nullptr;
    MutableMetric *_lookupCount = nullptr;
    MutableMetric *_lookupPartCount = nullptr;
    MutableMetric *_notFoundCount = nullptr;
    MutableMetric *_failedCount = nullptr;
};

TableServiceConnector::TableServiceConnector() {}

TableServiceConnector::~TableServiceConnector() {}

bool TableServiceConnector::init(const std::shared_ptr<multi_call::QuerySession> &querySession) {
    if (querySession == nullptr) {
        SQL_LOG(ERROR, "table service connector need subscribe remote zone");
        return false;
    } else {
        _querySession = querySession;
    }
    return true;
}

bool TableServiceConnector::batchScan(const Connector::ScanOptions &options, TablePtr &table) {
    auto beginScanTime = autil::TimeUtility::monotonicTimeUs();
    const auto &pks = options.pks;
    std::unordered_map<multi_call::PartIdTy, suez::KVBatchGetGenerator::KeyVec> partId2KeysMap;

    if (pks.empty()) {
        table = ScanUtil::createEmptyTable(options.attrs, options.attrsType, options.pool);
        return true;
    } else {
        auto pkMap = groupByPartition(
            pks, options.tableDist.hashMode._hashFunction, options.tableDist.partCnt);
        _metricsCollector.lookupPartCount = pkMap.size();
        // TODO use formattor
        _metricsCollector.lookupCount = 0;
        for (const auto &item : pkMap) {
            _metricsCollector.lookupCount += item.second.size();
            partId2KeysMap[item.first] = {};
            partId2KeysMap[item.first].resize(options.tableDist.partCnt);
            partId2KeysMap[item.first][item.first] = item.second;
        }
    }

    auto kvOptions = prepareKVOptions(options);
    auto generator = make_shared<suez::KVBatchGetGenerator>(kvOptions);

    generator->setPartIdToKeyMap(partId2KeysMap);
    multi_call::CallParam callParam;
    callParam.generatorVec.push_back(generator);
    multi_call::ReplyPtr reply;
    auto beginLookupTime = autil::TimeUtility::monotonicTimeUs();
    _querySession->call(callParam, reply);
    auto endLookupTime = autil::TimeUtility::monotonicTimeUs();
    _metricsCollector.lookupTime = endLookupTime - beginLookupTime;

    if (!fillTableResult(reply, table, options.pool)) {
        SQL_LOG(ERROR, "fill table [%s] result failed", options.tableName.c_str());
        return false;
    }

    auto endConvertTime = autil::TimeUtility::monotonicTimeUs();
    _metricsCollector.convertTime = endConvertTime - endLookupTime;
    _metricsCollector.batchScanTime = endConvertTime - beginScanTime;

    return true;
}

suez::KVBatchGetGenerator::KVBatchGetOptions
TableServiceConnector::prepareKVOptions(const Connector::ScanOptions &options) {
    suez::KVBatchGetGenerator::KVBatchGetOptions kvOptions;
    kvOptions.serviceName = options.serviceName;
    kvOptions.tableName = options.tableName;
    kvOptions.indexName = options.pkIndexName;
    kvOptions.attrs = options.attrs;
    kvOptions.timeout = options.leftTime;
    kvOptions.resultType = suez::KVBatchGetResultType::TABLE;
    kvOptions.useHashKey = false;
    return kvOptions;
}

bool TableServiceConnector::tryReportMetrics(kmonitor::MetricsReporter &opMetricsReporter) {
    opMetricsReporter.report<TableServiceConnectorMetrics>(nullptr, &_metricsCollector);
    SQL_LOG(TRACE1,
            "lookup time [%ld], convert time [%ld]",
            _metricsCollector.lookupTime,
            _metricsCollector.convertTime);
    return true;
}

int32_t TableServiceConnector::getPartId(uint32_t hashid, uint32_t partCount) {
    const autil::RangeVec &vec
        = autil::RangeUtil::splitRange(0, autil::RangeUtil::MAX_PARTITION_RANGE, partCount);
    for (int32_t id = 0; id < vec.size(); ++id) {
        if (hashid >= vec[id].first && hashid <= vec[id].second) {
            return id;
        }
    }
    return -1;
}

unordered_map<int32_t, set<string>> TableServiceConnector::groupByPartition(
    const vector<string> &pks, const string &hashType, uint32_t totalPartCount) {
    unordered_map<int32_t, set<string>> pkSet;
    autil::HashFunctionBasePtr func = autil::HashFuncFactory::createHashFunc(hashType);
    for (const auto &pk : pks) {
        uint32_t hashid = func->getHashId(pk);
        auto partId = getPartId(hashid, totalPartCount);
        pkSet[partId].insert(pk);
    }
    return pkSet;
}

bool TableServiceConnector::fillTableResult(const multi_call::ReplyPtr &reply,
                                            table::TablePtr &table,
                                            const autil::mem_pool::PoolPtr &pool) {
    size_t lackCount = 0;
    auto responseVec = reply->getResponses(lackCount);

    if (lackCount > 0) {
        SQL_LOG(ERROR, "lack count : %zu", lackCount);
        return false;
    }
    _metricsCollector.notFoundCount = 0;
    _metricsCollector.failedCount = 0;
    for (const auto &gigResponse : responseVec) {
        auto partId = gigResponse->getPartId();
        if (gigResponse->isFailed()) {
            SQL_LOG(ERROR,
                    "batch get failed, partId [%d], ec [%s]",
                    partId,
                    multi_call::translateErrorCode(gigResponse->getErrorCode()));
            return false;
        }
        auto batchGetResponse = dynamic_cast<suez::GigKVBatchGetResponse *>(gigResponse.get());
        if (!batchGetResponse) {
            SQL_LOG(ERROR, "batch get failed, invalid response type, partId [%d]", partId);
            return false;
        }
        auto pbResponse = dynamic_cast<suez::KVBatchGetResponse *>(batchGetResponse->getMessage());
        if (!pbResponse) {
            SQL_LOG(ERROR, "batch get failed, null pb response, partId [%d]", partId);
            return false;
        }

        if (pbResponse->errorinfo().errorcode() != suez::TBS_ERROR_NONE) {
            SQL_LOG(ERROR,
                    "batch get failed, error msg: %s",
                    pbResponse->errorinfo().errormsg().c_str());
            return false;
        }

        if (!pbResponse->has_binvalues()) {
            SQL_LOG(ERROR, "table service response no value");
            return false;
        }
        if (!mergeTableResult(table, pbResponse->binvalues(), pool)) {
            return false;
        }
        _metricsCollector.notFoundCount += pbResponse->notfoundcount();
        _metricsCollector.failedCount += pbResponse->failedcount();
        SQL_LOG(
            TRACE3, "after merge %d table is %s", partId, TableUtil::toString(table, 10).c_str());
    }

    SQL_LOG(TRACE1,
            "lookup count: %d, not found count: %d, failed count: %d",
            _metricsCollector.lookupCount,
            _metricsCollector.notFoundCount,
            _metricsCollector.failedCount);
    return true;
}

bool TableServiceConnector::mergeTableResult(table::TablePtr &table,
                                             const std::string &result,
                                             const autil::mem_pool::PoolPtr &pool) {
    TablePtr partTable = std::make_shared<table::Table>(pool);
    partTable->deserializeFromString(result, pool.get());
    SQL_LOG(TRACE3, "part table is %s", TableUtil::toString(partTable, 10).c_str());

    if (table == nullptr) {
        table = partTable;
    } else {
        if (!table->merge(partTable)) {
            SQL_LOG(ERROR,
                    "merge table failed, old table: %s, new table: %s",
                    TableUtil::toString(table, 10).c_str(),
                    TableUtil::toString(partTable, 10).c_str());
            return false;
        }
    }
    return true;
}

} // namespace sql
