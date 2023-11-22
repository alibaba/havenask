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
#include "sql/ops/runSqlGraph/RunSqlGraphKernel.h"

#include <algorithm>
#include <engine/NaviConfigContext.h>
#include <iosfwd>
#include <string>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/builder/GraphDesc.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/proto/GraphDef.pb.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/data/SqlGraphData.h"
#include "sql/data/SqlGraphType.h"
#include "sql/data/SqlQueryConfigData.h"
#include "sql/data/TableType.h"
#include "sql/ops/metaCollect/MetaCollectKernel.h"
#include "sql/ops/metaCollect/MetaMergeKernel.h"
#include "sql/ops/metaCollect/SqlMetaData.h"
#include "sql/ops/runSqlGraph/PartAccessAssigner.h"
#include "sql/proto/SqlSearchInfoCollector.h"

namespace navi {
class KernelInitContext;
} // namespace navi

using namespace std;
using namespace navi;
using namespace autil;

namespace sql {

static const float TIMEOUT_DECAY_FACTOR = 0.9f;
static const int64_t TIMEOUT_DECAY_MS = 5;

RunSqlGraphKernel::RunSqlGraphKernel() {}

void RunSqlGraphKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("sql.RunSqlGraphKernel")
        .input("input0", SqlGraphType::TYPE_ID)
        .input("input1", SqlQueryConfigType::TYPE_ID)
        .output("output0", TableType::TYPE_ID)
        .output("output1", SqlMetaDataType::TYPE_ID);
}

bool RunSqlGraphKernel::config(navi::KernelConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "one_batch", _oneBatch, _oneBatch);
    return true;
}

navi::ErrorCode RunSqlGraphKernel::init(navi::KernelInitContext &context) {
    return navi::EC_NONE;
}

navi::ErrorCode RunSqlGraphKernel::compute(navi::KernelComputeContext &ctx) {
    navi::PortIndex inputIndex0(0, navi::INVALID_INDEX);
    bool isEof = false;
    navi::DataPtr data;
    ctx.getInput(inputIndex0, data, isEof);
    if (data == nullptr) {
        SQL_LOG(ERROR, "data is nullptr");
        return navi::EC_ABORT;
    }
    SqlGraphData *sqlGraphData = dynamic_cast<SqlGraphData *>(data.get());
    if (sqlGraphData == nullptr) {
        SQL_LOG(ERROR, "sql graph data is nullptr");
        return navi::EC_ABORT;
    }
    navi::PortIndex inputIndex1(1, navi::INVALID_INDEX);
    navi::DataPtr sqlQueryConfigData;
    ctx.getInput(inputIndex1, sqlQueryConfigData, isEof);
    if (dynamic_cast<SqlQueryConfigData *>(sqlQueryConfigData.get()) == nullptr) {
        SQL_LOG(ERROR, "sql query config data[%p] cast failed", sqlQueryConfigData.get());
        return navi::EC_ABORT;
    }

    if (sqlGraphData->getOutputPorts().size() != 1) {
        SQL_LOG(ERROR,
                "output ports not equal 1 %s",
                StringUtil::toString(sqlGraphData->getOutputPorts()).c_str());
        return navi::EC_ABORT;
    }
    if (sqlGraphData->getOutputNodes().size() != 1) {
        SQL_LOG(ERROR,
                "output nodes not equal 1 %s",
                StringUtil::toString(sqlGraphData->getOutputNodes()).c_str());
        return navi::EC_ABORT;
    }
    unique_ptr<GraphDef> &graphDef = sqlGraphData->getGraphDef();
    if (graphDef == nullptr) {
        SQL_LOG(ERROR, "sql graphdef is nullptr");
        return navi::EC_ABORT;
    }
    try {
        PartAccessAssigner partAccessAssigner(_gigClientR, _hashFunctionCacheR);
        if (!partAccessAssigner.process(graphDef.get())) {
            SQL_LOG(ERROR, "part access assigner process failed");
            return navi::EC_ABORT;
        }
        GraphBuilder builder(graphDef.get());
        string nodeName = sqlGraphData->getOutputNodes()[0];
        string portName = sqlGraphData->getOutputPorts()[0];
        SQL_LOG(TRACE3, "sub graph def: %s", graphDef->ShortDebugString().c_str());
        builder.subGraph(0)
            .node(nodeName)
            .out(portName)
            .toForkNodeOutput("output0")
            .require(true)
            .merge("sql.TableMergeKernel")
            .integerAttr("one_batch", _oneBatch);
        addMetaCollectorOutput(builder);
    } catch (const autil::legacy::ExceptionBase &e) {
        SQL_LOG(ERROR, "build graph failed, msg: %s", e.GetMessage().c_str());
        return navi::EC_ABORT;
    }
    _sqlSearchInfoCollectorR->getCollector()->setSqlRunForkGraphBeginTime(
        TimeUtility::currentTime());
    ForkGraphParam param;

    NamedData namedData;
    namedData.name = SQL_QUERY_CONFIG_NAME;
    namedData.data = sqlQueryConfigData;
    for (size_t i = 0, subCount = graphDef->sub_graphs_size(); i < subCount; ++i) {
        auto graphId = graphDef->sub_graphs(i).graph_id();
        if (graphId >= 0) {
            namedData.graphId = graphId;
            param.namedDataVec.emplace_back(namedData);
        }
    }

    auto remainTimeMs = ctx.getRemainTimeMs();
    param.timeoutMs
        = std::max(int64_t(remainTimeMs * TIMEOUT_DECAY_FACTOR), remainTimeMs - TIMEOUT_DECAY_MS);
    return ctx.fork(graphDef.release(), param);
}

void RunSqlGraphKernel::addMetaCollectorOutput(GraphBuilder &builder) const {
    auto subGraphIds = builder.getAllGraphId();
    builder.newSubGraph("");
    auto mergeNode = builder.node("meta_merge").kernel(MetaMergeKernel::KERNEL_ID);
    auto inPort = mergeNode.in(MetaMergeKernel::INPUT_PORT);
    auto outPort = mergeNode.out(MetaMergeKernel::OUTPUT_PORT);
    outPort.toForkNodeOutput("output1");
    for (auto graphId : subGraphIds) {
        auto scopes = builder.getAllScope(graphId);
        for (const auto &scope : scopes) {
            auto terminator = builder.getScopeTerminator(scope);
            terminator.kernel(MetaCollectKernel::KERNEL_ID)
                .out(MetaCollectKernel::OUTPUT_PORT)
                .to(inPort.autoNext());
        }
    }
}

REGISTER_KERNEL(RunSqlGraphKernel);

} // namespace sql
