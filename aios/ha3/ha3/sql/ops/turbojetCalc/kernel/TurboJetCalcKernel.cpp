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
#include "ha3/sql/ops/calc/TurboJetCalcKernel.h"

#include "ha3/sql/data/TableData.h"
#include "ha3/sql/data/TableType.h"
#include "ha3/sql/ops/turbojetCalc/IquanPlanImporter.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "ha3/sql/resource/MetaInfoResource.h"
#include "ha3/sql/resource/SqlBizResource.h"
#include "ha3/sql/resource/SqlQueryResource.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "table/TableUtil.h"

namespace navi {
class KernelInitContext;
} // namespace navi

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

using namespace std;
using namespace matchdoc;
// using namespace navi;
// using namespace autil;
using namespace table;
using namespace suez::turing;

namespace isearch::sql::turbojet {

AUTIL_LOG_SETUP(sql, TurboJetCalcKernel);

TurboJetCalcKernel::TurboJetCalcKernel() {}

TurboJetCalcKernel::~TurboJetCalcKernel() {}

void TurboJetCalcKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("TurboJetCalcKernel")
        .input("input0", TableType::TYPE_ID)
        .output("output0", TableType::TYPE_ID)
        .resource(SqlBizResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_bizResource))
        .resource(SqlQueryResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_queryResource))
        .resource(navi::GRAPH_MEMORY_POOL_RESOURCE_ID, true, BIND_RESOURCE_TO(_memoryPoolResource))
        .resource(navi::META_INFO_RESOURCE_ID, false, BIND_RESOURCE_TO(_metaInfoResource));
}

bool TurboJetCalcKernel::config(navi::KernelConfigContext &ctx) {
    if (!_initParam.initFromJson(ctx)) {
        return false;
    }
    _initParam.opName = getKernelName();
    _initParam.nodeName = getNodeName();
    return true;
}

navi::ErrorCode TurboJetCalcKernel::init(navi::KernelInitContext &context) {
    // init metric reporter
    auto queryMetricsReporter = _queryResource->getQueryMetricsReporter();
    if (queryMetricsReporter) {
        string pathName = "sql.user.ops." + _initParam.opName;
        _opMetricReporter = queryMetricsReporter->getSubReporter(pathName, {}).get();
    }
    _calc = make_unique<TurboJetCalc>(
        _initParam, _bizResource, _queryResource, _metaInfoResource, _memoryPoolResource);
    auto r = _calc->doInit();
    if (r.is_err()) {
        SQL_LOG(ERROR, "%s", r.get_error().get_stack_trace().c_str());
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

navi::ErrorCode TurboJetCalcKernel::compute(navi::KernelComputeContext &ctx) {
    bool isEof = false;
    navi::DataPtr data;
    ctx.getInput(0, data, isEof);

    TablePtr table;
    if (data) {
        table = KernelUtil::getTable(data, _memoryPoolResource, false);
        if (!table) {
            SQL_LOG(ERROR, "invalid input table");
            return navi::EC_ABORT;
        }
        FunctionProvider functionProvider(table->getMatchDocAllocator(),
                                          _queryResource->getPool(),
                                          _queryResource->getSuezCavaAllocator(),
                                          _queryResource->getTracer(),
                                          nullptr,
                                          nullptr,
                                          _opMetricReporter);
        auto r = _calc->doCompute(&functionProvider, table, isEof);
        if (r.is_err()) {
            SQL_LOG(ERROR, "%s", r.get_error().get_stack_trace().c_str());
            return navi::EC_ABORT;
        }
    }

    TableDataPtr tableData(new TableData(table));
    ctx.setOutput(0, tableData, isEof);
    return navi::EC_NONE;
}

REGISTER_KERNEL(TurboJetCalcKernel);

} // namespace isearch::sql::turbojet
