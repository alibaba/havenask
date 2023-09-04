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
#include "sql/ops/scan/ScanKernel.h"

#include <engine/NaviConfigContext.h>
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
#include "sql/ops/scan/KKVScanR.h"
#include "sql/ops/scan/NormalScanR.h"
#include "sql/ops/scan/ScanBase.h"
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

ScanKernel::ScanKernel() {}

ScanKernel::~ScanKernel() {}

void ScanKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("sql.ScanKernel")
        .output("output0", TableType::TYPE_ID)
        .selectR(
            {NormalScanR::RESOURCE_ID, KKVScanR::RESOURCE_ID},
            [](navi::KernelConfigContext &ctx) {
                std::string tableType;
                NAVI_JSONIZE(ctx, "table_type", tableType);
                if (tableType == SCAN_NORMAL_TABLE_TYPE) {
                    return 0;
                } else if (tableType == SCAN_KKV_TABLE_TYPE) {
                    return 1;
                } else {
                    SQL_LOG(ERROR, "not support table type [%s].", tableType.c_str());
                    return navi::INVALID_SELECT_INDEX;
                }
            },
            true,
            BIND_RESOURCE_TO(_scanBase));
}

bool ScanKernel::config(navi::KernelConfigContext &ctx) {
    return true;
}

navi::ErrorCode ScanKernel::init(navi::KernelInitContext &context) {
    return navi::EC_NONE;
}

navi::ErrorCode ScanKernel::compute(navi::KernelComputeContext &context) {
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

REGISTER_KERNEL(ScanKernel);

} // namespace sql
