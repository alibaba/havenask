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
#include "sql/ops/remoteScan/RemoteScanR.h"

#include <algorithm>
#include <iosfwd>

#include "autil/StringUtil.h"
#include "autil/TimeoutTerminator.h"
#include "ha3/util/TypeDefine.h"
#include "indexlib/partition/partition_group_resource.h"
#include "iquan/common/catalog/LocationDef.h"
#include "kmonitor/client/MetricsReporter.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/proto/KernelDef.pb.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "navi/resource/QuerySessionR.h"
#include "sql/common/Log.h" // IWYU pragma: keep
#include "sql/common/TableMeta.h"
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/remoteScan/Connector.h"
#include "sql/resource/QueryMetricReporterR.h"
#include "sql/resource/TimeoutTerminatorR.h"
#include "table/Table.h"
#include "table/TableUtil.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

using namespace std;
using namespace table;

namespace sql {

const std::string RemoteScanR::RESOURCE_ID = "remote_scan_r";

constexpr char TABLE_SERVICE_TOPO_NAME[] = "table_service";

RemoteScanR::RemoteScanR() {}

RemoteScanR::~RemoteScanR() {}

void RemoteScanR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL);
}

bool RemoteScanR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode RemoteScanR::init(navi::ResourceInitContext &ctx) {
    if (!ScanBase::doInit()) {
        return navi::EC_ABORT;
    }
    auto scanHintMap = _scanInitParamR->getScanHintMap();
    if (!scanHintMap) {
        SQL_LOG(ERROR, "remote scan need scan hint");
        return navi::EC_ABORT;
    }
    const auto &scanHints = *scanHintMap;
    string sourceType;
    ScanInitParamR::fromHint(scanHints, "remoteSourceType", sourceType);
    _connector = Connector::create(sourceType);
    if (_connector == nullptr) {
        return navi::EC_ABORT;
    }
    if (!_connector->init(_querySessionR->getQuerySession())) {
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

bool RemoteScanR::doBatchScan(TablePtr &table, bool &eof) {
    const std::string &indexName = _scanInitParamR->tableMeta.getPkIndexName();
    if (indexName.empty()) {
        SQL_LOG(ERROR, "pk of table [%s] not find", _scanInitParamR->tableName.c_str());
        return false;
    }
    auto zoneName = _scanInitParamR->location.tableGroupName + isearch::ZONE_BIZ_NAME_SPLITTER
                    + TABLE_SERVICE_TOPO_NAME;
    auto serviceName = zoneName + isearch::ZONE_BIZ_NAME_SPLITTER + _scanInitParamR->tableName;
    auto timeoutTerminator = _timeoutTerminatorR->getTimeoutTerminator();
    Connector::ScanOptions options(_pks,
                                   _scanInitParamR->calcInitParamR->outputFields,
                                   _scanInitParamR->calcInitParamR->outputFieldsType,
                                   _scanInitParamR->tableName,
                                   indexName,
                                   serviceName,
                                   _scanInitParamR->tableDist,
                                   _scanR->graphMemoryPoolR->getPool(),
                                   timeoutTerminator->getLeftTime());
    auto ret = _connector->batchScan(options, table);
    if (!ret) {
        SQL_LOG(ERROR, "remote scan failed, options: %s", options.toString().c_str());
        return false;
    }
    SQL_LOG(TRACE3,
            "pks: %s, remote scan batch table: %s",
            autil::StringUtil::toString(_pks).c_str(),
            TableUtil::toString(table, 20).c_str());
    auto reporter = _queryMetricReporterR->getReporter()->getSubReporter(
        "", {{{"table_name", getTableNameForMetrics()}}});
    if (!_connector->tryReportMetrics(*reporter)) {
        SQL_LOG(TRACE3, "ignore report metrics");
    }
    eof = true;
    return true;
}

bool RemoteScanR::doUpdateScanQuery(const StreamQueryPtr &inputQuery) {
    if (!inputQuery) {
        _pks.clear();
        SQL_LOG(TRACE3, "raw pk is empty for null input query");
    } else {
        _pks = inputQuery->primaryKeys;
    }
    return true;
}

REGISTER_RESOURCE(RemoteScanR);

} // namespace sql
