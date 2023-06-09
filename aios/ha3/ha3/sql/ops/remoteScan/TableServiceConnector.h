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
#pragma once

#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "ha3/sql/ops/remoteScan/Connector.h"
#include "ha3/sql/ops/remoteScan/TableServiceConnectorConfig.h"
#include "suez/service/KVBatchGetGenerator.h"

namespace isearch {
namespace sql {

class TableServiceConnector : public Connector {
public:
    TableServiceConnector();
    ~TableServiceConnector();

public:
    struct MetricCollector {
        int64_t lookupTime = 0;
        int64_t convertTime = 0;
        int64_t groupByTime = 0;
        int64_t batchScanTime = 0;
        int32_t lookupCount = 0;
        int32_t lookupPartCount = 0;
        int32_t notFoundCount = 0;
        int32_t failedCount = 0;
    };

public:
    bool init(const std::shared_ptr<multi_call::QuerySession> &querySession) override;
    bool batchScan(const Connector::ScanOptions &options, table::TablePtr &table) override;
    bool tryReportMetrics(kmonitor::MetricsReporter &opMetricsReporter) override;

private:
    int32_t getPartId(uint32_t hashid, uint32_t partCount);
    std::unordered_map<int32_t, std::set<std::string>> groupByPartition(const std::vector<std::string> &pks, const std::string &hashType, uint32_t totalPartCount);
    bool fillTableResult(const multi_call::ReplyPtr &reply, table::TablePtr &table, const autil::mem_pool::PoolPtr &pool);
    bool mergeTableResult(table::TablePtr &table, const std::string &result, const autil::mem_pool::PoolPtr &pool);
    suez::KVBatchGetGenerator::KVBatchGetOptions prepareKVOptions(const Connector::ScanOptions &options);
private:
    std::vector<std::string> _attrs;
    std::shared_ptr<multi_call::QuerySession> _querySession;
    MetricCollector _metricsCollector;
private:
    AUTIL_LOG_DECLARE();
    
};

}
}
