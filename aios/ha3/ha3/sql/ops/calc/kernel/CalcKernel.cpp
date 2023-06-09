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
#include "ha3/sql/ops/calc/CalcKernel.h"

#include <iosfwd>
#include <memory>

#include "alog/Logger.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/data/TableData.h"
#include "ha3/sql/data/TableType.h"
#include "ha3/sql/ops/calc/CalcTable.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "ha3/sql/resource/SqlBizResource.h"
#include "ha3/sql/resource/SqlQueryResource.h"
#include "ha3/sql/resource/MetaInfoResource.h"
#include "matchdoc/flatbuffer/MatchDoc_generated.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/Resource.h"
#include "navi/resource/GraphMemoryPoolResource.h" // IWYU pragma: keep
#include "suez/turing/expression/common.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"

namespace navi {
class KernelInitContext;
} // namespace navi

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

using namespace std;
using namespace matchdoc;
using namespace navi;
using namespace autil;
using namespace table;
using namespace suez::turing;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, CalcKernel);

CalcKernel::CalcKernel() {}

CalcKernel::~CalcKernel() {}

void CalcKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("CalcKernel")
        .input("input0", TableType::TYPE_ID)
        .output("output0", TableType::TYPE_ID)
        .resource(SqlBizResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_bizResource))
        .resource(SqlQueryResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_queryResource))
        .resource(navi::GRAPH_MEMORY_POOL_RESOURCE_ID, true, BIND_RESOURCE_TO(_memoryPoolResource))
        .resource(navi::META_INFO_RESOURCE_ID, false, BIND_RESOURCE_TO(_metaInfoResource));
}

bool CalcKernel::config(navi::KernelConfigContext &ctx) {
    if (!_initParam.initFromJson(ctx)) {
        return false;
    }
    _initParam.opName = getKernelName();
    _initParam.nodeName = getNodeName();
    return true;
}

navi::ErrorCode CalcKernel::init(navi::KernelInitContext &context) {
    _calcWrapperPtr.reset(
            new CalcWrapper(_initParam, _bizResource, _queryResource,
                            _metaInfoResource, _memoryPoolResource));
    if (!_calcWrapperPtr->init()) {
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

navi::ErrorCode CalcKernel::compute(KernelComputeContext &ctx) {
    navi::PortIndex inputIndex(0, navi::INVALID_INDEX);
    bool isEof = false;
    navi::DataPtr data;
    ctx.getInput(inputIndex, data, isEof);

    TablePtr table;
    if (data) {
        table = KernelUtil::getTable(data, _memoryPoolResource, !_initParam.reuseInputs.empty());
        if (!table) {
            SQL_LOG(ERROR, "invalid input table");
            return EC_ABORT;
        }
        if (!_calcWrapperPtr->compute(table, isEof)) {
            SQL_LOG(ERROR, "filter table error.");
            return EC_ABORT;
        }
    }

    navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
    TableDataPtr tableData(new TableData(table));
    ctx.setOutput(outputIndex, tableData, isEof);
    return EC_NONE;
}

REGISTER_KERNEL(CalcKernel);

} // namespace sql
} // namespace isearch
