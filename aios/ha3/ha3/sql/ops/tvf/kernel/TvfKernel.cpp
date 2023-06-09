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
#include "ha3/sql/ops/tvf/TvfKernel.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <set>

#include "alog/Logger.h"
#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/data/TableData.h"
#include "ha3/sql/data/TableType.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "ha3/sql/resource/SqlBizResource.h"
#include "ha3/sql/resource/SqlQueryResource.h"
#include "ha3/sql/resource/MetaInfoResource.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/Resource.h"
#include "navi/resource/GraphMemoryPoolResource.h" // IWYU pragma: keep
#include "table/Table.h"

namespace navi {
class KernelInitContext;
} // namespace navi

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace navi;
using namespace suez::turing;
using namespace table;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, TvfKernel);

TvfKernel::TvfKernel() {}

TvfKernel::~TvfKernel() {}

void TvfKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("TvfKernel")
        .input("input0", TableType::TYPE_ID)
        .output("output0", TableType::TYPE_ID)
        .resource(SqlBizResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_bizResource))
        .resource(SqlQueryResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_queryResource))
        .resource(navi::GRAPH_MEMORY_POOL_RESOURCE_ID, true, BIND_RESOURCE_TO(_memoryPoolResource))
        .resource(navi::META_INFO_RESOURCE_ID, false, BIND_RESOURCE_TO(_metaInfoResource));
}

bool TvfKernel::config(navi::KernelConfigContext &ctx) {
    if (!_initParam.initFromJson(ctx)) {
        return false;
    }

    _initParam.opName = getKernelName();
    _initParam.nodeName = getNodeName();
    return true;
}

navi::ErrorCode TvfKernel::init(navi::KernelInitContext &context) {
    _tvfWrapper.reset(new TvfWrapper(_initParam, _bizResource, _queryResource,
                    _metaInfoResource, _memoryPoolResource));
    if (!_tvfWrapper->init()) {
        SQL_LOG(ERROR, "tvf wrapper init failed.");
        return navi::EC_ABORT;
    }

    return navi::EC_NONE;
}

navi::ErrorCode TvfKernel::compute(KernelComputeContext &ctx) {
    navi::PortIndex inputIndex(0, navi::INVALID_INDEX);
    bool isEof = false;
    navi::DataPtr data;
    ctx.getInput(inputIndex, data, isEof);
    auto inputTable = KernelUtil::getTable(data, _memoryPoolResource, !_initParam.reuseInputs.empty());
    TablePtr outputTable;
    if (!_tvfWrapper->compute(inputTable, isEof, outputTable)) {
        SQL_LOG(ERROR, "tvf [%s] compute failed.", _initParam.invocation.tvfName.c_str());
        return EC_ABORT;
    }

    navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
    if (outputTable || isEof) {
        if (outputTable && inputTable) {
            outputTable->mergeDependentPools(inputTable);
        }
        TableDataPtr tableData(new TableData(outputTable));
        ctx.setOutput(outputIndex, tableData, isEof);
    }

    return EC_NONE;
}

REGISTER_KERNEL(TvfKernel);

} // namespace sql
} // namespace isearch
