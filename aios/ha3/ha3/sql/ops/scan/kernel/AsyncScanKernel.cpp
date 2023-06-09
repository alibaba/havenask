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
#include "ha3/sql/ops/scan/AsyncScanKernel.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <string>

#include "alog/Logger.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/sql/common/IndexInfo.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/data/TableData.h"
#include "ha3/sql/ops/calc/CalcWrapper.h"
#include "ha3/sql/ops/externalTable/ha3sql/Ha3SqlRemoteScan.h"
#include "ha3/sql/ops/scan/AggregationScan.h"
#include "ha3/sql/ops/scan/KKVScan.h"
#include "ha3/sql/ops/scan/KVScan.h"
#include "ha3/sql/ops/scan/LogicalScan.h"
#include "ha3/sql/ops/scan/NormalScan.h"
#include "ha3/sql/ops/scan/ScanBase.h"
#include "ha3/sql/ops/scan/ScanKernelUtil.h"
#include "ha3/sql/ops/scan/SummaryScan.h"
#include "ha3/sql/ops/tvf/TvfWrapper.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "ha3/sql/resource/MetaInfoResource.h"
#include "ha3/sql/resource/ObjectPoolResource.h"
#include "ha3/sql/resource/SqlBizResource.h"
#include "ha3/sql/resource/SqlConfigResource.h"
#include "ha3/sql/resource/SqlQueryResource.h"
#include "ha3/sql/resource/TabletManagerR.h"
#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/proto/KernelDef.pb.h"
#include "navi/resource/GigQuerySessionR.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "rapidjson/rapidjson.h"
#include "suez/turing/expression/common.h"
#include "table/Table.h"

using namespace std;
using namespace table;
using namespace suez::turing;
using namespace isearch::search;
namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, AsyncScanKernel);
AsyncScanKernel::AsyncScanKernel() {}

AsyncScanKernel::~AsyncScanKernel() {
}

// kernel def, see: ha3/sql/executor/proto/KernelDef.proto
void AsyncScanKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("AsyncScanKernel")
        .output("output0", TableType::TYPE_ID)
        .resource(SqlBizResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_bizResource))
        .resource(ObjectPoolResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_objectPoolResource))
        .resource(SqlQueryResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_queryResource))
        .resource(navi::GRAPH_MEMORY_POOL_RESOURCE_ID, true, BIND_RESOURCE_TO(_memoryPoolResource))
        .resource(navi::GigQuerySessionR::RESOURCE_ID, false, BIND_RESOURCE_TO(_naviQuerySessionR))
        .resource(navi::META_INFO_RESOURCE_ID, false, BIND_RESOURCE_TO(_metaInfoResource))
        .resource(SqlConfigResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_sqlConfigResource))
        .resource(TABLET_MANAGER_RESOURCE_ID, true, BIND_RESOURCE_TO(_tabletManagerR));
}

bool AsyncScanKernel::config(navi::KernelConfigContext &ctx) {
    return ScanKernelBase::config(ctx);
}

navi::ErrorCode AsyncScanKernel::init(navi::KernelInitContext &context) {
    if (!ScanKernelBase::init(context, getKernelName(), getNodeName())) {
        return navi::EC_ABORT;
    }
    auto asyncPipe = context.createAsyncPipe();
    if (!asyncPipe) {
        SQL_LOG(ERROR, "create async pipe failed");
        return navi::EC_ABORT;
    }
    _asyncPipe = asyncPipe;
    if (_initParam.tableType == SCAN_SUMMARY_TABLE_TYPE) {
        _scanBase.reset(new SummaryScan(_initParam, asyncPipe));
    } else if (_initParam.tableType == SCAN_KV_TABLE_TYPE) {
        _scanBase.reset(new KVScan(_initParam, asyncPipe));
    } else if (_initParam.tableType == SCAN_EXTERNAL_TABLE_TYPE) {
        _scanBase.reset(new Ha3SqlRemoteScan(_initParam, asyncPipe));
    } else if (_initParam.tableType == SCAN_AGGREGATION_TABLE_TYPE) {
        if (_initParam.aggIndexName.empty()) {
            _scanBase.reset(new KVScan(_initParam, asyncPipe));
        } else {
            _scanBase.reset(new AggregationScan(_initParam, asyncPipe));
        }
    } else {
        SQL_LOG(ERROR, "not support table type [%s].", _initParam.tableType.c_str());
    }
    if (_scanBase == nullptr || !_scanBase->init()) {
        SQL_LOG(ERROR, "init async scan kernel failed, table [%s].",
                _initParam.tableName.c_str());
        return navi::EC_ABORT;
    }

    return initPushDownOps();
}

navi::ErrorCode AsyncScanKernel::computeEnd(navi::KernelComputeContext &context) {
    table::TablePtr table;
    bool eof = true;

    if (!_pushDownOpInitParams.empty()) {
        _scanBase->setPushDownMode(true);
    }

    do {
        if (!_scanBase->batchScan(table, eof)) {
            SQL_LOG(ERROR, "batch scan failed");
            return navi::EC_ABORT;
        }
        for (size_t i = 0; i < _pushDownOps.size(); ++i) {
            if (!_pushDownOps[i]->compute(table, eof)) {
                SQL_LOG(ERROR,
                        "the no.%ld push down op: %s compute failed",
                        i,
                        _pushDownOps[i]->getName().c_str());
                return navi::EC_ABORT;
            }
            if (!table) {
                if (eof) {
                    SQL_LOG(ERROR,
                            "the no.%ld push down op: %s output table is null",
                            i,
                            _pushDownOps[i]->getName().c_str());
                    return navi::EC_ABORT;
                }
                break;
            }
        }
    } while (!eof && !table);

    navi::PortIndex index(0, navi::INVALID_INDEX);
    if (table != nullptr) {
        TableDataPtr tableData(new TableData(table));
        context.setOutput(index, tableData, eof);
    } else {
        context.setOutput(index, nullptr, eof);
    }
    return navi::EC_NONE;
}

REGISTER_KERNEL(AsyncScanKernel);

} // namespace sql
} // namespace isearch
