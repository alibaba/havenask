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
#include "sql/ops/scan/AsyncScanKernel.h"

#include <engine/NaviConfigContext.h>
#include <engine/ResourceInitContext.h>
#include <iosfwd>
#include <memory>
#include <string>

#include "ha3/search/PartitionInfoWrapper.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/Resource.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/data/TableData.h"
#include "sql/data/TableType.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/scan/KVScanR.h"
#include "sql/ops/scan/ScanBase.h"
#include "sql/ops/scan/ScanPushDownR.h"
#include "sql/ops/scan/SummaryScanR.h"
#include "suez/turing/expression/common.h"
#include "table/Row.h"


namespace navi {
class KernelInitContext;
} // namespace navi

using namespace std;
using namespace table;
using namespace suez::turing;
using namespace isearch::search;
namespace sql {

AsyncScanKernel::AsyncScanKernel() {}

AsyncScanKernel::~AsyncScanKernel() {}

// kernel def, see: sql/executor/proto/KernelDef.proto
void AsyncScanKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("sql.AsyncScanKernel")
        .output("output0", TableType::TYPE_ID)
        .selectR(
            {SummaryScanR::RESOURCE_ID, KVScanR::RESOURCE_ID},
            [](navi::KernelConfigContext &ctx) {
                std::string tableType;
                NAVI_JSONIZE(ctx, "table_type", tableType);
                if (tableType == SCAN_SUMMARY_TABLE_TYPE) {
                    return 0;
                } else if (tableType == SCAN_KV_TABLE_TYPE) {
                    return 1;
                } else {
                    SQL_LOG(ERROR, "not support table type [%s].", tableType.c_str());
                    return navi::INVALID_SELECT_INDEX;
                }
            },
            true,
            BIND_RESOURCE_TO(_scanBase))
        .selectR(
            {ScanPushDownR::RESOURCE_ID},
            [](navi::KernelConfigContext &ctx) {
                if (ctx.hasKey("push_down_ops")) {
                    return 0;
                } else {
                    return navi::IGNORE_SELECT_INDEX;
                }
            },
            true,
            BIND_RESOURCE_TO(_scanPushDownR));
}

bool AsyncScanKernel::config(navi::KernelConfigContext &ctx) {
    return true;
}

navi::ErrorCode AsyncScanKernel::init(navi::KernelInitContext &ctx) {
    _asyncPipe = _scanBase->getAsyncPipe();
    if (_scanPushDownR) {
        _scanBase->setPushDownMode(_scanPushDownR);
    }
    return navi::EC_NONE;
}

navi::ErrorCode AsyncScanKernel::computeEnd(navi::KernelComputeContext &context) {
    table::TablePtr table;
    bool eof = true;
    do {
        if (!_scanBase->batchScan(table, eof)) {
            SQL_LOG(ERROR, "batch scan failed");
            return navi::EC_ABORT;
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
