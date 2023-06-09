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
#include "ha3/sql/ops/remoteScan/RemoteScan.h"

#include "alog/Logger.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "table/Table.h"
#include "table/TableUtil.h"
#include "ha3/sql/ops/calc/CalcTable.h"
#include "ha3/sql/ops/condition/ExprUtil.h"
#include "ha3/sql/ops/remoteScan/RemoteScanProperties.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "ha3/util/TypeDefine.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "navi/resource/GigQuerySessionR.h"

using namespace std;
using namespace table;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, RemoteScan);

constexpr char TABLE_SERVICE_TOPO_NAME[] = "table_service";

RemoteScan::RemoteScan(const ScanInitParam &param, const navi::GigQuerySessionR *gigQuerySessionR)
    : ScanBase(param)
{
    _enableTableInfo = false;
    if (gigQuerySessionR) {
        _querySession = gigQuerySessionR->getQuerySession();
    }
}

RemoteScan::~RemoteScan() {}

bool RemoteScan::doInit() {
    const auto &mapIter = _param.hintsMap.find(SQL_SCAN_HINT);
    if (mapIter == _param.hintsMap.end()) {
        SQL_LOG(ERROR, "remote scan need scan hint");
        return false;
    }
    const auto &scanHints = mapIter->second;
    string sourceType;
    fromHint(scanHints, "remoteSourceType", sourceType);
    _connector = Connector::create(sourceType);
    if (_connector == nullptr) {
        return false;
    }

    if (_param.scanResource.metricsReporter != nullptr) {
        _metricsReporter = _param.scanResource.metricsReporter;
    }

    return _connector->init(_querySession);
}

bool RemoteScan::doBatchScan(TablePtr &table, bool &eof) {
    const std::string &indexName = _param.tableMeta.getPkIndexName();
    if (indexName.empty()) {
        SQL_LOG(ERROR, "pk of table [%s] not find", _param.tableName.c_str());
        return false;
    }
    auto zoneName = _param.location.tableGroupName + ZONE_BIZ_NAME_SPLITTER + TABLE_SERVICE_TOPO_NAME;
    auto serviceName = zoneName + ZONE_BIZ_NAME_SPLITTER + _param.tableName;
    Connector::ScanOptions options(_pks,
                                   _param.outputFields,
                                   _param.outputFieldsType,
                                   _param.tableName,
                                   indexName,
                                   serviceName,
                                   _param.tableDist,
                                   _param.scanResource.memoryPoolResource->getPool(),
                                   _timeoutTerminator ? _timeoutTerminator->getLeftTime() : numeric_limits<int64_t>::max());
    auto ret = _connector->batchScan(options, table);
    if (!ret) {
        SQL_LOG(ERROR, "remote scan failed, options: %s", options.toString().c_str());
        return false;
    }


    SQL_LOG(TRACE3, "pks: %s, remote scan batch table: %s", autil::StringUtil::toString(_pks).c_str(), TableUtil::toString(table, 20).c_str());

    if (_metricsReporter) {
        auto reporter = _metricsReporter->getSubReporter(
            "", {{{"table_name", getTableNameForMetrics()}}});
        if (!_connector->tryReportMetrics(*reporter)) {
            SQL_LOG(TRACE3, "ignore report metrics");
        }
    }

    eof = true;
    return true;
}

bool RemoteScan::doUpdateScanQuery(const StreamQueryPtr &inputQuery) {
    if (!inputQuery) {
        _pks.clear();
        SQL_LOG(TRACE3, "raw pk is empty for null input query");
    } else {
        _pks = inputQuery->primaryKeys;
    }
    return true;
}

}
}
