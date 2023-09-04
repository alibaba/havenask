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

#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/mem_pool/Pool.h"
#include "table/Table.h"

namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor

namespace multi_call {
class QuerySession;
}

namespace sql {
class TableDistribution;

class Connector {
public:
    Connector();
    virtual ~Connector();

public:
    struct ScanOptions {
        const std::vector<std::string> &pks;
        const std::vector<std::string> &attrs;
        const std::vector<std::string> &attrsType;
        const std::string &tableName;
        const std::string &pkIndexName;
        const std::string &serviceName;
        const TableDistribution &tableDist;
        autil::mem_pool::PoolPtr pool;
        int64_t leftTime;

        ScanOptions(const std::vector<std::string> &pks_,
                    const std::vector<std::string> &attrs_,
                    const std::vector<std::string> &attrsType_,
                    const std::string &tableName_,
                    const std::string &pkIndexName_,
                    const std::string &serviceName_,
                    const TableDistribution &tableDist_,
                    const autil::mem_pool::PoolPtr &pool_,
                    int64_t leftTime_);

        std::string toString();
    };

public:
    virtual bool init(const std::shared_ptr<multi_call::QuerySession> &querySession) = 0;
    virtual bool batchScan(const ScanOptions &options, table::TablePtr &table) = 0;
    virtual bool tryReportMetrics(kmonitor::MetricsReporter &opMetricsReporter) = 0;

public:
    static std::unique_ptr<Connector> create(const std::string &sourceType);
};

} // namespace sql
