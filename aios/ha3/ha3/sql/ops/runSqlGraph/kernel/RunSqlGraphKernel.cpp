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
#include "ha3/sql/ops/runSqlGraph/RunSqlGraphKernel.h"

#include "ha3/sql/ops/runSqlGraph/PartAccessAssigner.h"
#include "ha3/sql/resource/HashFunctionCacheR.h"
#include "autil/StringUtil.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/data/SqlGraphData.h"
#include "ha3/sql/data/SqlGraphType.h"
#include "ha3/sql/data/TableData.h"
#include "ha3/sql/data/TableType.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/resource/GigClientResource.h"
#include "ha3/sql/resource/MetaInfoResource.h"

using namespace std;
using namespace navi;
using namespace autil;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(kernel, RunSqlGraphKernel);

RunSqlGraphKernel::RunSqlGraphKernel() {}

void RunSqlGraphKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("RunSqlGraphKernel")
        .input("input0", SqlGraphType::TYPE_ID)
        .output("output0", TableType::TYPE_ID)
        .resource(navi::GIG_CLIENT_RESOURCE_ID, false, BIND_RESOURCE_TO(_gigClientResource))
        .resource(HashFunctionCacheR::RESOURCE_ID, false, BIND_RESOURCE_TO(_hashFunctionCacheR))
        .resource(navi::META_INFO_RESOURCE_ID, false, BIND_RESOURCE_TO(_metaInfoResource));
}

bool RunSqlGraphKernel::config(navi::KernelConfigContext &ctx) {
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
        PartAccessAssigner partAccessAssigner(_gigClientResource, _hashFunctionCacheR);
        if (!partAccessAssigner.process(graphDef.get())) {
            SQL_LOG(ERROR, "part access assigner process failed");
            return navi::EC_ABORT;
        }
        GraphBuilder builder(graphDef.get());
        string nodeName = sqlGraphData->getOutputNodes()[0];
        string portName = sqlGraphData->getOutputPorts()[0];
        SQL_LOG(DEBUG, "sub graph def: %s", graphDef->ShortDebugString().c_str());
        builder.subGraph(0).node(nodeName).out(portName).toForkNodeOutput("output0").require(true);
    } catch (const autil::legacy::ExceptionBase &e) {
        SQL_LOG(ERROR, "build graph failed, msg: %s", e.GetMessage().c_str());
        return navi::EC_ABORT;
    }
    if (_metaInfoResource && _metaInfoResource->getOverwriteInfoCollector()) {
        _metaInfoResource->getOverwriteInfoCollector()->setSqlRunForkGraphBeginTime(TimeUtility::currentTime());
    }
    return ctx.fork(graphDef.release());
}
REGISTER_KERNEL(RunSqlGraphKernel);

} // namespace sql
} // end namespace isearch
