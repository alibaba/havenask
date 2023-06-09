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
#include "alog/Logger.h"
#include "autil/Log.h"
#include "indexlib/index/orc/RowGroup.h"
#include "ha3/sql/common/IndexInfo.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/data/TableData.h"
#include "ha3/sql/data/TableType.h"
#include "ha3/sql/ops/scan/ScanBase.h"
#include "ha3/sql/ops/scan/ScanKernelUtil.h"
#include "ha3/sql/ops/scan/ScanUtil.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#if defined(__clang__) && (__cplusplus >= 202002L)
#include "ha3/sql/ops/orcScan/AsyncOrcScan.h"
#endif
#include "ha3/sql/ops/orcScan/ScanResult.h"
#include "ha3/sql/resource/SqlBizResource.h"
#include "ha3/sql/resource/SqlQueryResource.h"
#include "ha3/sql/resource/MetaInfoResource.h"
#include "ha3/sql/ops/condition/Condition.h"
#include "ha3/sql/ops/condition/ConditionParser.h"
#include "ha3/sql/ops/scan/PrimaryKeyScanConditionVisitor.h"
#include "ha3/sql/ops/scan/ScanUtil.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader_adaptor.h"
#include "indexlib/table/normal_table/NormalTabletSessionReader.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/Resource.h"
#include "navi/resource/GraphMemoryPoolResource.h" // IWYU pragma: keep

using namespace std;

namespace isearch {
namespace sql {

AUTIL_DECLARE_AND_SETUP_LOGGER(sql, TabletOrcScanKernel);

struct TabletOrcScanMetricsCollector {
    int64_t seekTime = 0;
    int64_t scanTime = 0;
    int64_t converTime = 0;
    int64_t calcTime = 0;
};

class TabletOrcScanMetric : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_QPS_MUTABLE_METRIC(_qps, "TabletOrcScan.qps");
        REGISTER_LATENCY_MUTABLE_METRIC(_seekTime, "TabletOrcScan.seekTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_scanTime, "TabletOrcScan.scanTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_convertTime, "TabletOrcScan.convertTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_calcTime, "TabletOrcScan.calcTime");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, TabletOrcScanMetricsCollector *collector) {
        REPORT_MUTABLE_QPS(_qps);
        REPORT_MUTABLE_METRIC(_seekTime, collector->seekTime / 1000.0f);
        REPORT_MUTABLE_METRIC(_scanTime, collector->scanTime / 1000.0f);
        REPORT_MUTABLE_METRIC(_convertTime, collector->converTime / 1000.0f);
        REPORT_MUTABLE_METRIC(_calcTime, collector->calcTime / 1000.0f);
    }

private:
    kmonitor::MutableMetric *_qps = nullptr;
    kmonitor::MutableMetric *_seekTime = nullptr;
    kmonitor::MutableMetric *_scanTime = nullptr;
    kmonitor::MutableMetric *_convertTime = nullptr;
    kmonitor::MutableMetric *_calcTime = nullptr;
};

class TabletOrcScanKernel : public navi::Kernel {
public:
    void def(navi::KernelDefBuilder &builder) const override {
        builder.name("TabletOrcScanKernel")
            .output("output0", TableType::TYPE_ID)
            .resource(SqlBizResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_bizResource))
            .resource(
                SqlQueryResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_queryResource))
            .resource(
                navi::GRAPH_MEMORY_POOL_RESOURCE_ID, true, BIND_RESOURCE_TO(_memoryPoolResource))
            .resource(
                navi::META_INFO_RESOURCE_ID, false, BIND_RESOURCE_TO(_metaInfoResource));
    }

    bool config(navi::KernelConfigContext &ctx) override {
        if (!_initParam.initFromJson(ctx)) {
            return false;
        }
        KernelUtil::stripName(_initParam.outputFields);
        KernelUtil::stripName(_initParam.indexInfos);
        KernelUtil::stripName(_initParam.usedFields);
        KernelUtil::stripName(_initParam.hashFields);
        return true;
    }

    navi::ErrorCode init(navi::KernelInitContext &context) override {
        _initParam.opName = getKernelName();
        _initParam.nodeName = getNodeName();
        if (!ScanKernelUtil::convertResource(_bizResource,
                                             _queryResource,
                                             _memoryPoolResource,
                                             nullptr,
                                             nullptr,
                                             _metaInfoResource,
                                             _initParam.scanResource)) {
            SQL_LOG(ERROR, "convert resource failed.");
            return navi::EC_ABORT;
        }

#if defined(__clang__) && (__cplusplus >= 202002L)
        string pk;
        int32_t docId;
        if (extractPkFromCondition(_initParam, pk)) {
            if (!queryDocIdByPk(_initParam, pk, &docId)) {
                _emptyOutput = true;
                return navi::EC_NONE;
            }
            preparePkLookUpAttr(docId);
        }
#endif

        _asyncPipe = context.createAsyncPipe();
        if (_asyncPipe == nullptr) {
            SQL_LOG(ERROR, "create async pipe failed");
            return navi::EC_ABORT;
        }
        _queryMetricsReporter = _queryResource->getQueryMetricsReporter();

#if defined(__clang__) && (__cplusplus >= 202002L)
        _scan = std::make_unique<AsyncOrcScan>(_initParam, _asyncPipe);
        if (!_scan->init()) {
            return navi::EC_ABORT;
        }
#else
        SQL_LOG(ERROR, "orc scan only support clang now");
        return navi::EC_ABORT;
#endif
        return navi::EC_NONE;
    }

    navi::ErrorCode compute(navi::KernelComputeContext &context) override {
#if defined(__clang__) && (__cplusplus >= 202002L)
        if (_emptyOutput) {
            auto table = ScanUtil::createEmptyTable(_initParam.outputFields, _initParam.outputFieldsType, _initParam.scanResource.memoryPoolResource->getPool());
            if (!table) {
                return navi::EC_ABORT;
            }
            TableDataPtr tableData(new TableData(table));
            context.setOutput(navi::PortIndex(0, navi::INVALID_INDEX), tableData, true);
            return navi::EC_NONE;
        }

        navi::DataPtr odata;
        bool eof;
        _asyncPipe->getData(odata, eof);

        std::shared_ptr<ScanResult> data = std::static_pointer_cast<ScanResult>(odata);
        if (!data && !eof) {
            SQL_LOG(ERROR, "get data empty and not eof");
            return navi::EC_ABORT;
        }
        if (eof) {
            return navi::EC_NONE;
        }

        if(!_scan->checkData(data)) {
            return navi::EC_ABORT;
        }
        auto state = data->getScanState();

        switch(state) {
        case ScanState::SS_INIT: {
            _collector.seekTime = _scan->getSeekTime();
            _scan->scan();
            break;
        }
        case ScanState::SS_SCAN: {
            table::TablePtr table;
            if (_retTable) {
                table = std::move(_retTable);
            }
            uint64_t beginConverTime = autil::TimeUtility::currentTime();

            if (!_scan->convertResult(table, data)) {
                return navi::EC_ABORT;
            }
            _collector.scanTime = _scan->getScanTime();
            _collector.converTime = autil::TimeUtility::currentTime() - beginConverTime;

            if (reachLimit(table)) {
                _retTable = std::move(table);
                _scan->setFinish();
            } else {
                if (table->getRowCount() >= _scan->getBatchSize()) {
                    // stream output
                    _scanCount += table->getRowCount();
                    if (!_scan->calc(table)) {
                        return navi::EC_ABORT;
                    }
                    TableDataPtr tableData(new TableData(table));
                    context.setOutput(navi::PortIndex(0, navi::INVALID_INDEX), tableData, false);
                } else {
                    _retTable = std::move(table);
                }
                reportMetric();
                _scan->scan();
            }
            break;
        }
        case ScanState::SS_FINISH: {
            if (_retTable == nullptr) {
                // out of range
                uint64_t beginConverTime = autil::TimeUtility::currentTime();
                if (!_scan->convertResult(_retTable, data)) {
                    return navi::EC_ABORT;
                }
                _collector.converTime = autil::TimeUtility::currentTime() - beginConverTime;
            }
            _asyncPipe->setEof();
            tryClearBackRows();
            uint64_t beginCalcTime = autil::TimeUtility::currentTime();
            if (!_scan->calc(_retTable)) {
                return navi::EC_ABORT;
            }
            _collector.calcTime = autil::TimeUtility::currentTime() - beginCalcTime;
            _scan->reportRetMetrics(_retTable);
            reportMetric();
            TableDataPtr tableData(new TableData(_retTable));
            context.setOutput(navi::PortIndex(0, navi::INVALID_INDEX), tableData, true);
            break;
        }
        default:
            return navi::EC_ABORT;
        }

        return navi::EC_NONE;
#else
        SQL_LOG(ERROR, "orc scan only support clang now");
        return navi::EC_ABORT;
#endif
        return navi::EC_NONE;
    }

private:
#if defined(__clang__) && (__cplusplus >= 202002L)
    bool extractPkFromCondition(const ScanInitParam &param, string &pk) {
        const auto &tableInfo = param.scanResource.getTableInfo(param.tableName);
        if (!tableInfo) {
            SQL_LOG(WARN, "do not have table info, using normal scan");
            return false;
        }
        auto indexInfo = tableInfo->getPrimaryKeyIndexInfo();
        if (!indexInfo) {
            SQL_LOG(WARN, "do not have index info, using normal scan");
            return false;
        }
        PrimaryKeyScanConditionVisitor visitor(indexInfo->fieldName, indexInfo->indexName);
        ConditionParser parser;
        ConditionPtr condition;
        if (!parser.parseCondition(param.conditionJson, condition)) {
            SQL_LOG(WARN, "parse sql condition failed, using normal scan");
            return false;
        }
        if (condition) {
            condition->accept(&visitor);
            auto pks = visitor.stealRawKeyVec();
            if (pks.size() == 1) {
                pk = pks[0];
                return true;
            } else {
                SQL_LOG(INFO, "get [%zu] pks, but OrcPkScan support one pk now, using normal scan", pks.size());
            }
        }
        return false;
    }

    bool queryDocIdByPk(const ScanInitParam &param, const string &pk, int32_t *docId) {
        auto tabletReader = (param.scanResource.getTabletReader(param.tableName));
        if (!tabletReader) {
            SQL_LOG(ERROR, "table [%s] not exist", param.tableName.c_str());
            return false;
        }
        auto normalTabletReader = dynamic_pointer_cast<indexlibv2::table::NormalTabletSessionReader>(tabletReader);
        if (!normalTabletReader) {
            SQL_LOG(ERROR, "get normal tablet reader of table [%s] failed", param.tableName.c_str());
            return false;
        }
        auto pkReader = normalTabletReader->GetPrimaryKeyReader();
        auto deletionMapReader = normalTabletReader->GetDeletionMapReader();

        if (!pkReader) {
            SQL_LOG(ERROR, "pkReader is nullptr");
            return false;
        }
        *docId = pkReader->Lookup(pk);
        if (INVALID_DOCID == *docId) {
            SQL_LOG(ERROR, "can't find docid with pk [%s]", pk.c_str());
            return false;
        }
        if (deletionMapReader && deletionMapReader->IsDeleted(*docId)) {
            SQL_LOG(ERROR, "find deleted docid [%d] with pk[%s]", *docId, pk.c_str());
            return false;
        }
        return true;
    }

    void preparePkLookUpAttr(int32_t docId) {
        _initParam.hintsMap[SQL_SCAN_HINT]["start_row"] = autil::StringUtil::toString(docId);
        _initParam.hintsMap[SQL_SCAN_HINT]["localLimit"] = "1";
    }

    bool reachLimit(const table::TablePtr &table) {
        return _scanCount + table->getRowCount() >= _scan->getLimit();
    }
    void tryClearBackRows() {
        assert(_retTable != nullptr);
        if (_scanCount + _retTable->getRowCount() > _scan->getLimit()) {
            auto deleteRowCount = _scanCount + _retTable->getRowCount() - _scan->getLimit();
            _retTable->clearBackRows(deleteRowCount);
            _scan->subTotalScanCount(deleteRowCount);
        }
    }
    void reportMetric() {
        if (_queryMetricsReporter != nullptr) {
            std::string pathName = "sql.user.ops." + getKernelName();
            auto opMetricsReporter = _queryMetricsReporter->getSubReporter(
                    pathName, {{{"table_name", _scan->getTableNameForMetrics()}}});
            opMetricsReporter->report<TabletOrcScanMetric, TabletOrcScanMetricsCollector>(nullptr, &_collector);
        }
    }
#endif
private:
    ScanInitParam _initParam;
    SqlBizResource *_bizResource = nullptr;
    SqlQueryResource *_queryResource = nullptr;
    navi::GraphMemoryPoolResource *_memoryPoolResource = nullptr;
    MetaInfoResource *_metaInfoResource = nullptr;
    kmonitor::MetricsReporter *_queryMetricsReporter = nullptr;
    bool _emptyOutput = false;

#if defined(__clang__) && (__cplusplus >= 202002L)
    std::unique_ptr<AsyncOrcScan> _scan = nullptr;
    table::TablePtr _retTable = nullptr;
#endif
    navi::AsyncPipePtr _asyncPipe;
    uint32_t _scanCount;
    TabletOrcScanMetricsCollector _collector;
};

REGISTER_KERNEL(TabletOrcScanKernel);


} // sql

} // isearch
