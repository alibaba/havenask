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
#include "ha3/sql/ops/identity/IdentityKernel.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <stdint.h>

#include "alog/Logger.h"
#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/data/TableData.h"
#include "ha3/sql/data/TableType.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "ha3/sql/proto/SqlSearchInfoCollector.h"
#include "ha3/sql/resource/MetaInfoResource.h"
#include "ha3/sql/resource/SqlQueryResource.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/Resource.h"
#include "navi/resource/GraphMemoryPoolResource.h"
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
using namespace table;
using namespace matchdoc;
using namespace kmonitor;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, IdentityKernel);

class IdentityOpMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_GAUGE_MUTABLE_METRIC(_totalOutputCount, "TotalOutputCount");
        REGISTER_LATENCY_MUTABLE_METRIC(_durationTime, "DurationTime");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, IdentityInfo *identityInfo) {
        REPORT_MUTABLE_METRIC(_totalOutputCount, identityInfo->totaloutputcount());
        REPORT_MUTABLE_METRIC(_durationTime, identityInfo->durationtime() / 1000);
    }

private:
    MutableMetric *_totalOutputCount = nullptr;
    MutableMetric *_durationTime = nullptr;
};

IdentityKernel::IdentityKernel()
    : _scope("default")
{}
IdentityKernel::~IdentityKernel()  {
    reportMetrics();
}

// kernel def, see: navi/sql/executor/proto/KernelDef.proto
void IdentityKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("IdentityKernel")
        .input("input0", TableType::TYPE_ID)
        .output("output0", TableType::TYPE_ID)
        .resource(SqlQueryResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_queryResource))
        .resource(navi::META_INFO_RESOURCE_ID, false, BIND_RESOURCE_TO(_metaInfoResource));
}

bool IdentityKernel::config(navi::KernelConfigContext &ctx) {
    _durationTimer = make_unique<autil::ScopedTime2>();
    ctx.Jsonize(IQUAN_OP_ID, _opId, _opId);
    ctx.Jsonize("op_scope", _scope, _scope);
    return true;
}

navi::ErrorCode IdentityKernel::init(navi::KernelInitContext &context) {
    _identityInfo.set_hashkey(_opId);
    if (_metaInfoResource) {
        _sqlSearchInfoCollector = _metaInfoResource->getOverwriteInfoCollector();
    }
    _queryMetricsReporter = _queryResource->getQueryMetricsReporter();
    return navi::EC_NONE;
}

navi::ErrorCode IdentityKernel::compute(navi::KernelComputeContext &runContext) {
    bool eof = false;
    navi::DataPtr data;
    navi::PortIndex inputIndex(0, navi::INVALID_INDEX);
    runContext.getInput(inputIndex, data, eof);

    TablePtr table = KernelUtil::getTable(data);
    if (table != nullptr) {
        _identityInfo.set_totaloutputcount(_identityInfo.totaloutputcount() + table->getRowCount());
    }

    if (eof == true) {
        _identityInfo.set_durationtime(_durationTimer ? _durationTimer->done_us() : 0);
    }
    navi::PortIndex index(0, navi::INVALID_INDEX);
    runContext.setOutput(index, data, eof);
    if (_sqlSearchInfoCollector) {
        _sqlSearchInfoCollector->overwriteIdentityInfo(_identityInfo);
    }
    return navi::EC_NONE;
}

void IdentityKernel::reportMetrics() {
    if (_queryMetricsReporter != nullptr) {
        string pathName = "sql.user.ops." + getKernelName();
        auto opMetricsReporter = _queryMetricsReporter->getSubReporter(pathName, {
                    {{"op_scope", _scope}}});
        opMetricsReporter->report<IdentityOpMetrics, IdentityInfo>(nullptr, &_identityInfo);
    }
}

REGISTER_KERNEL(IdentityKernel);

} // namespace sql
} // namespace isearch
